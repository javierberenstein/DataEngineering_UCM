# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession


def create_cloud_files_df(spark: SparkSession, path: str, options: dict):
    """
    Creates a DataFrame using Databricks Autoloader with cloudFiles options for batch ingestion.

    :param spark: Spark session
    :param path: Path to the data files
    :param options: Options for cloudFiles autoloader
    :return: DataFrame
    """
    reader = (
        spark.read.format("cloudFiles")
        .option("cloudFiles.format", options.get("cloudFiles.format", "json"))
        .option(
            "cloudFiles.inferColumnTypes",
            options.get("cloudFiles.inferColumnTypes", "true"),
        )
        .option(
            "cloudFiles.schemaEvolutionMode",
            options.get("cloudFiles.schemaEvolutionMode", "addNewColumns"),
        )
    )

    if "cloudFiles.schemaHints" in options:
        reader = reader.option(
            "cloudFiles.schemaHints", options["cloudFiles.schemaHints"]
        )

    return reader.load(path)


def create_cloud_files_streaming_df(spark: SparkSession, path: str, options: dict):
    """
    Creates a streaming DataFrame using Databricks Autoloader with cloudFiles options.

    :param spark: Spark session
    :param path: Path to the data files
    :param options: Options for cloudFiles autoloader
    :return: Streaming DataFrame
    """
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", options.get("cloudFiles.format", "json"))
        .option(
            "cloudFiles.inferColumnTypes",
            options.get("cloudFiles.inferColumnTypes", "true"),
        )
        .option(
            "cloudFiles.schemaEvolutionMode",
            options.get("cloudFiles.schemaEvolutionMode", "addNewColumns"),
        )
    )

    if "cloudFiles.schemaHints" in options:
        reader = reader.option(
            "cloudFiles.schemaHints", options["cloudFiles.schemaHints"]
        )

    return reader.load(path)


def create_kafka_streaming_df(
    spark: SparkSession,
    topic_pattern: str,
    key_subject: str,
    value_subject: str,
    kafka_options: dict,
):
    """
    Creates a streaming DataFrame from Kafka based on the topic pattern and subjects.

    :param spark: Spark session
    :param topic_pattern: Kafka topic pattern
    :param key_subject: Key subject for Kafka messages
    :param value_subject: Value subject for Kafka messages
    :param kafka_options: Kafka configuration options
    :return: Streaming DataFrame
    """
    kafka_reader = (
        spark.readStream.format("kafka")
        .option("subscribePattern", topic_pattern)
        .option("kafka.bootstrap.servers", kafka_options.get("bootstrap_servers", ""))
        .option("startingOffsets", kafka_options.get("startingOffsets", "latest"))
    )

    if "security_protocol" in kafka_options:
        kafka_reader = kafka_reader.option(
            "kafka.security.protocol", kafka_options["security_protocol"]
        )

    if "sasl_mechanism" in kafka_options:
        kafka_reader = kafka_reader.option(
            "kafka.sasl.mechanism", kafka_options["sasl_mechanism"]
        )

    if "sasl_jaas_config" in kafka_options:
        kafka_reader = kafka_reader.option(
            "kafka.sasl.jaas.config", kafka_options["sasl_jaas_config"]
        )

    return kafka_reader.load().selectExpr(
        f"CAST({key_subject} AS STRING) as key",
        f"CAST({value_subject} AS STRING) as value",
    )
