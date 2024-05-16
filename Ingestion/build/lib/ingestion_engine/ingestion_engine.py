# -*- coding: utf-8 -*-
import logging

import yaml
from confluent_kafka.schema_registry import CachedSchemaRegistryClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, date_format
from pyspark.sql.streaming import StreamingQuery
from utils.autoloader import create_cloud_files_df, create_kafka_streaming_df
from utils.image import process_images
from utils.metadata import add_metadata_columns


class DataIngestion:
    def __init__(self, spark: SparkSession, config_path: str):
        self.logger = self.setup_logger()
        self.spark = spark
        self.config = self.load_config(config_path)
        self.schema_registry_client = None
        self.schema_cache = {}

    def setup_logger(self):
        logger = logging.getLogger("DataIngestion")
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    def load_config(self, path: str):
        if not self.logger:
            self.logger = self.setup_logger()
        try:
            with open(path, "r") as file:
                config = yaml.safe_load(file)
            self.logger.info(f"Successfully loaded configuration from {path}")
            self.validate_config(config)
            return config
        except FileNotFoundError as e:
            self.logger.error(f"Configuration file not found: {e}")
            raise
        except yaml.YAMLError as e:
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

            return CachedSchemaRegistryClient(schema_registry_conf)
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

    def ingest_batch(self):
        for dataset in self.config.get("datasets", {}).get("batch", []):
            try:
                self.logger.info(
                    f"Starting batch ingestion for dataset: {dataset['name']}"
                )
                options = dataset.get("options", {})
                bronze_path = f"{dataset['bronze_path']}/{dataset['datasource']}/{dataset['dataset']}/"
                if dataset["format"] == "image":
                    df = process_images(self.spark, dataset["landing_path"])
                else:
                    is_stream = (
                        dataset.get("options", {}).get("cloudFiles.format", None)
                        is not None
                    )
                    df = create_cloud_files_df(
                        self.spark,
                        dataset["landing_path"],
                        options,
                        is_stream=is_stream,
                    )
                df = add_metadata_columns(df)
                self.write_data(
                    df,
                    bronze_path,
                    dataset["partition_columns"],
                    dataset["incremental"],
                    dataset.get("output_format", "delta"),
                    dataset.get("merge_schema", False),
                )
                self.logger.info(
                    f"Completed batch ingestion for dataset: {dataset['name']}"
                )
            except KeyError as e:
                self.logger.error(f"Missing key in batch dataset configuration: {e}")
                raise
            except Exception as e:
                self.logger.error(
                    f"Error ingesting batch dataset {dataset['name']}: {e}"
                )
                raise

    def ingest_streaming(self):
        for dataset in self.config.get("datasets", {}).get("streaming", []):
            try:
                self.logger.info(
                    f"Starting streaming ingestion for dataset: {dataset['name']}"
                )
                options = dataset.get("options", {})
                bronze_path = f"{dataset['bronze_path']}/{dataset['datasource']}/{dataset['dataset']}/"
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
                df = create_kafka_streaming_df(
                    self.spark,
                    dataset["topic_pattern"],
                    dataset["key_subject"],
                    dataset["value_subject"],
                    kafka_options,
                    message_format,
                    value_schema,
                )
                df = add_metadata_columns(df)
                self.write_stream(df, dataset, bronze_path=bronze_path)
                self.logger.info(
                    f"Started streaming ingestion for dataset: {dataset['name']}"
                )
            except KeyError as e:
                self.logger.error(
                    f"Missing key in streaming dataset configuration: {e}"
                )
                raise
            except ValueError as e:
                self.logger.error(f"Configuration error: {e}")
                raise
            except Exception as e:
                self.logger.error(
                    f"Error ingesting streaming dataset {dataset['name']}: {e}"
                )
                raise

    def write_data(
        self,
        df,
        bronze_path: str,
        partition_columns: list,
        incremental: bool,
        output_format: str,
        merge_schema: bool,
    ):
        try:
            if incremental:
                df = (
                    df.withColumn("year", date_format(current_timestamp(), "yyyy"))
                    .withColumn("month", date_format(current_timestamp(), "MM"))
                    .withColumn("day", date_format(current_timestamp(), "dd"))
                )
                partition_columns += ["year", "month", "day"]

            writer = (
                df.write.mode("append" if incremental else "overwrite")
                .format(output_format)
                .partitionBy(*partition_columns)
            )
            if merge_schema:
                writer = writer.option("mergeSchema", "true")
            writer.save(bronze_path)
            self.logger.info(f"Data successfully written to {bronze_path}")
        except KeyError as e:
            self.logger.error(f"Missing key in dataset configuration: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error writing data to {bronze_path}: {e}")
            raise

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
                "checkpointLocation": f"{ingestion_config['bronze_path']}/{datasource}/{dataset}/_checkpoint/"
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
            self.ingest_batch()
            self.ingest_streaming()
        except Exception as e:
            self.logger.error(f"Error running ingestion process: {e}")
            raise


if __name__ == "__main__":
    spark = SparkSession.builder.appName("DataIngestion").getOrCreate()
    ingestion = DataIngestion(spark, "configs/ingestion_config.yaml")
    ingestion.run()
