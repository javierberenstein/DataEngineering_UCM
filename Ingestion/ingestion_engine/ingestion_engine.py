# -*- coding: utf-8 -*-
import logging
from os.path import join as path_join

from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.streaming import StreamingQuery
from utils.autoloader import create_cloud_files_df, create_kafka_streaming_df
from utils.image import process_images
from utils.metadata import add_metadata_columns
from yaml import YAMLError, safe_load


class DataIngestion:
    def __init__(self, spark: SparkSession, config_path: str):
        self.logger = self.setup_logger()
        self.spark = spark
        self.config = self.load_config(config_path)
        self.schema_registry_client = None
        self.schema_cache = {}

    def setup_logger(self):
        logger = logging.getLogger("DataIngestion")
        if not logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
            logger.propagate = False
        return logger

    def load_config(self, path: str):
        try:
            with open(path, "r") as file:
                config = safe_load(file)
            self.logger.info(f"Successfully loaded configuration from {path}")
            self.validate_config(config)
            return config
        except FileNotFoundError as e:
            self.logger.error(f"Configuration file not found: {e}")
            raise
        except YAMLError as e:
            self.logger.error(f"Error parsing configuration file: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error loading configuration: {e}")
            raise

    def validate_config(self, config):
        required_keys = ["datasets"]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required configuration key: {key}")

    def setup_schema_registry_client(self, schema_registry_config: dict):
        try:
            schema_registry_conf = {"url": schema_registry_config["url"]}
            if (
                "username" in schema_registry_config
                and "password" in schema_registry_config
            ):
                schema_registry_conf[
                    "basic.auth.user.info"
                ] = f"{schema_registry_config['username']}:{schema_registry_config['password']}"
            return SchemaRegistryClient(schema_registry_conf)
        except KeyError as e:
            self.logger.error(f"Missing key in schema registry configuration: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error setting up schema registry client: {e}")
            raise

    def get_schema(self, subject: str) -> str:
        if subject in self.schema_cache:
            return self.schema_cache[subject]
        try:
            schema = self.schema_registry_client.get_latest_version(
                subject
            ).schema.schema_str
            self.schema_cache[subject] = schema
            return schema
        except Exception as e:
            self.logger.error(f"Error fetching schema for subject {subject}: {e}")
            raise

    def join_paths(self, *paths):
        return path_join(*paths).replace("\\", "/")

    def sanitize_column_names(self, df: DataFrame) -> DataFrame:
        sanitized_columns = [
            col(column).alias(
                column.replace(" ", "_")
                .replace(",", "")
                .replace(";", "")
                .replace("{", "")
                .replace("}", "")
                .replace("(", "")
                .replace(")", "")
                .replace("\n", "")
                .replace("\t", "")
                .replace("=", "")
            )
            for column in df.columns
        ]
        return df.select(*sanitized_columns)

    def ingest_data(self):
        datasets = self.config.get("datasets", {})

        for dataset in datasets.get("batch", []) + datasets.get("streaming", []):
            try:
                self.logger.info(f"Starting ingestion for dataset: {dataset['name']}")
                options = dataset.get("options", {})
                bronze_path = self.join_paths(
                    dataset["bronze_path"], dataset["datasource"], dataset["dataset"]
                )

                if dataset.get("incremental", False):
                    current_date = self.spark.sql(
                        "SELECT current_date() as date"
                    ).collect()[0]["date"]
                    year, month, day = (
                        current_date.year,
                        current_date.month,
                        current_date.day,
                    )
                    bronze_path = self.join_paths(
                        bronze_path, str(year), str(month), str(day)
                    )

                options["cloudFiles.schemaLocation"] = self.join_paths(
                    bronze_path, "_schema"
                )

                if dataset["format"] == "image":
                    df = process_images(self.spark, dataset["landing_path"])
                elif "kafka" in dataset:
                    df = self.create_kafka_streaming_df(dataset)
                else:
                    df = create_cloud_files_df(
                        self.spark, dataset["landing_path"], options
                    )

                df = add_metadata_columns(df)
                df = self.sanitize_column_names(df)
                self.write_stream(df, dataset, bronze_path)
                self.logger.info(f"Completed ingestion for dataset: {dataset['name']}")
            except KeyError as e:
                self.logger.error(f"Missing key in dataset configuration: {e}")
                raise
            except ValueError as e:
                self.logger.error(f"Configuration error: {e}")
                raise
            except Exception as e:
                self.logger.error(f"Error ingesting dataset {dataset['name']}: {e}")
                raise

    def create_kafka_streaming_df(self, dataset):
        kafka_options = dataset.get("kafka", {})
        message_format = dataset.get("format", "json")
        value_schema = None
        if message_format == "avro":
            schema_registry_config = dataset.get("schema_registry", {})
            if not self.schema_registry_client:
                self.schema_registry_client = self.setup_schema_registry_client(
                    schema_registry_config
                )
            value_subject = f"{dataset['topic_pattern']}-value"
            value_schema = self.get_schema(value_subject)
        return create_kafka_streaming_df(
            self.spark,
            dataset["topic_pattern"],
            dataset["key_subject"],
            dataset["value_subject"],
            kafka_options,
            message_format,
            value_schema,
        )

    def write_stream(
        self, df: DataFrame, ingestion_config: dict, bronze_path: str
    ) -> StreamingQuery:
        try:
            datasource = ingestion_config.get("datasource")
            dataset = ingestion_config.get("dataset")
            sink = ingestion_config.get("sink", {})
            layer = sink.get("layer", "bronze")
            if layer != "bronze":
                raise Exception(f"Layer {layer} not configured!")

            format = ingestion_config.get("output_format", "delta")
            opts = {
                "checkpointLocation": self.join_paths(
                    ingestion_config["bronze_path"], datasource, dataset, "_checkpoint"
                )
            }
            if sink.get("options"):
                opts.update(sink.get("options"))

            query_name = f"{datasource} {dataset}"
            writer = (
                df.writeStream.format(format)
                .options(**opts)
                .option("path", bronze_path)
                .queryName(query_name)
            )

            mode = ingestion_config.get("mode", "planned")
            trigger_interval = ingestion_config.get("trigger_interval", "1 minute")
            if mode == "continuous":
                writer = writer.trigger(processingTime=trigger_interval)
            else:
                writer = writer.trigger(availableNow=True)

            if ingestion_config.get("merge_schema", False):
                writer = writer.option("mergeSchema", "true")

            query = writer.start()
            self.logger.info(f"Streaming data successfully written to {bronze_path}")
            return query
        except KeyError as e:
            self.logger.error(f"Missing key in streaming dataset configuration: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error writing streaming data: {e}")
            raise

    def run(self):
        try:
            self.ingest_data()
        except Exception as e:
            self.logger.error(f"Error running ingestion process: {e}")
            raise
