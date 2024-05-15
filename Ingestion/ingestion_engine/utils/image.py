# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp
from pyspark.ml.image import ImageSchema


def process_images(spark: SparkSession, path: str):
    """
    Processes image files and adds metadata columns.

    :param spark: Spark session
    :param path: Path to the image files
    :return: DataFrame with image data and metadata columns
    """
    df = ImageSchema.readImages(path)
    return df.withColumn("ingestion_date", current_timestamp()).withColumn(
        "source_file", input_file_name()
    )
