# -*- coding: utf-8 -*-
from pyspark.sql.functions import current_timestamp, input_file_name


def add_metadata_columns(df):
    """
    Adds metadata columns (ingestion date and source file) to the DataFrame.

    :param df: Input DataFrame
    :return: DataFrame with added metadata columns
    """
    return df.withColumn("ingestion_date", current_timestamp()).withColumn(
        "source_file", input_file_name()
    )
