# -*- coding: utf-8 -*-
from setuptools import find_packages, setup

setup(
    name="ingestion_engine",
    version="1.0.11",
    packages=find_packages(),
    install_requires=[
        "pyspark",
        "confluent_kafka",
        "pyyaml",
    ],
)
