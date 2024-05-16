# -*- coding: utf-8 -*-
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro


def create_cloud_files_df(
    spark: SparkSession,
    path: str,
    options: dict,
) -> DataFrame:
    reader = spark.readStream

    # Ensure the schemaLocation option is provided
    if "cloudFiles.schemaLocation" not in options:
        raise ValueError("cloudFiles.schemaLocation must be specified in options")

    df = (
        reader.format("cloudFiles")
        .option("cloudFiles.format", options.get("cloudFiles.format", "csv"))
        .option("cloudFiles.schemaLocation", options["cloudFiles.schemaLocation"])
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
        df = df.option("cloudFiles.schemaHints", options["cloudFiles.schemaHints"])

    return df.load(path)


def create_kafka_streaming_df(
    spark: SparkSession,
    topic_pattern: str,
    key_subject: str,
    value_subject: str,
    kafka_options: dict,
    message_format: str,
    value_schema: str = None,
):
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

    df = kafka_reader.load()

    if message_format == "avro" and value_schema:
        df = (
            df.withColumn("key", F.col("key").cast("string"))
            .withColumn(
                "value",
                from_avro(
                    F.expr("substring(value, 6, length(value) - 5)"), value_schema
                ),
            )
            .withColumn("_ingested_at", F.current_timestamp())
        )
    else:
        df = df.selectExpr(
            f"CAST({key_subject} AS STRING) as key",
            f"CAST({value_subject} AS STRING) as value",
        )

    return df
