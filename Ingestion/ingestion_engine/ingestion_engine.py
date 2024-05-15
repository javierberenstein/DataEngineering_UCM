# -*- coding: utf-8 -*-
import yaml
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import current_timestamp, date_format
from utils.metadata import add_metadata_columns
from utils.autoloader import (
    create_cloud_files_df,
    create_cloud_files_streaming_df,
    create_kafka_streaming_df,
)
from utils.image import process_images


class DataIngestion:
    def __init__(self, spark: SparkSession, config_path: str):
        """
        Initialize the DataIngestion class with a Spark session and configuration path.

        :param spark: Spark session
        :param config_path: Path to the YAML configuration file
        """
        self.spark = spark
        self.config = self.load_config(config_path)
        self.logger = self.setup_logger()

    def setup_logger(self):
        """Sets up the logger for the ingestion process."""
        logger = logging.getLogger("DataIngestion")
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger

    def load_config(self, path: str):
        """
        Loads the YAML configuration file.

        :param path: Path to the YAML configuration file
        :return: Parsed configuration dictionary
        """
        try:
            with open(path, "r") as file:
                config = yaml.safe_load(file)
            self.logger.info(f"Successfully loaded configuration from {path}")
            return config
        except Exception as e:
            self.logger.error(f"Error loading configuration: {e}")
            raise

    def ingest_batch(self):
        """Processes batch datasets based on the provided configuration."""
        for dataset in self.config["datasets"]["batch"]:
            try:
                self.logger.info(
                    f"Starting batch ingestion for dataset: {dataset['name']}"
                )
                options = dataset.get("options", {})
                bronze_path = f"{dataset['bronze_path']}/{dataset['datasource']}/{dataset['dataset']}/"
                if dataset["format"] == "image":
                    df = process_images(self.spark, dataset["landing_path"])
                else:
                    df = create_cloud_files_df(
                        self.spark, dataset["landing_path"], options
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
            except Exception as e:
                self.logger.error(
                    f"Error ingesting batch dataset {dataset['name']}: {e}"
                )

    def ingest_streaming(self):
        """Processes streaming datasets based on the provided configuration."""
        for dataset in self.config["datasets"]["streaming"]:
            try:
                self.logger.info(
                    f"Starting streaming ingestion for dataset: {dataset['name']}"
                )
                options = dataset.get("options", {})
                bronze_path = f"{dataset['bronze_path']}/{dataset['datasource']}/{dataset['dataset']}/"
                if dataset["source_type"] == "kafka":
                    kafka_options = dataset.get("kafka", {})
                    df = create_kafka_streaming_df(
                        self.spark,
                        dataset["topic_pattern"],
                        dataset["key_subject"],
                        dataset["value_subject"],
                        kafka_options,
                    )
                elif dataset["source_type"] == "file":
                    df = create_cloud_files_streaming_df(
                        self.spark, dataset["landing_path"], options
                    )
                else:
                    raise ValueError(
                        f"Unsupported source type: {dataset['source_type']}"
                    )

                df = add_metadata_columns(df)
                self.write_stream(df, dataset, bronze_path=bronze_path)
                self.logger.info(
                    f"Started streaming ingestion for dataset: {dataset['name']}"
                )
            except Exception as e:
                self.logger.error(
                    f"Error ingesting streaming dataset {dataset['name']}: {e}"
                )

    def write_data(
        self,
        df,
        bronze_path: str,
        partition_columns: list,
        incremental: bool,
        output_format: str,
        merge_schema: bool,
    ):
        """
        Writes batch DataFrame to the specified bronze path with partitioning.
        Handles incremental and one-shot loads.

        :param df: DataFrame to write
        :param bronze_path: Destination path in bronze layer
        :param partition_columns: Columns to partition by
        :param incremental: Boolean flag for incremental or one-shot load
        :param output_format: Format to write the data (e.g., 'delta', 'parquet')
        :param merge_schema: Boolean flag to merge schema or not
        """
        try:
            if incremental:
                # Add extraction date columns for partitioning
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
        except Exception as e:
            self.logger.error(f"Error writing data to {bronze_path}: {e}")
            raise

    def write_stream(self, df: DataFrame, ingestion_config: dict) -> StreamingQuery:
        """
        Writes streaming DataFrame to the specified path based on the ingestion configuration.

        :param df: DataFrame to write
        :param ingestion_config: Ingestion configuration dictionary
        :return: StreamingQuery object
        """
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

            bronze_path = f"{ingestion_config['bronze_path']}/{datasource}/{dataset}/"
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
        except Exception as e:
            self.logger.error(f"Error writing streaming data: {e}")
            raise

    def run(self):
        """Runs the ingestion process for both batch and streaming datasets."""
        self.ingest_batch()
        self.ingest_streaming()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("DataIngestion").getOrCreate()
    ingestion = DataIngestion(spark, "configs/ingestion_config.yaml")
    ingestion.run()
