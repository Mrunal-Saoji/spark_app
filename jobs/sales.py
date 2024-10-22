#!/usr/bin/python3
import logging
import os, json, re, sys
from typing import Callable, Optional


project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_FILE = f"{project_dir}/logs/job-{os.path.basename(__file__)}.log"
LOG_FORMAT = f"%(asctime)s - LINE:%(lineno)d - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
logging.basicConfig(filename=LOG_FILE,level=logging.DEBUG, format=LOG_FORMAT)
logger = logging.Logger('py4j')

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

sys.path.insert(1, project_dir)
from classes import class_pyspark


def main():
    class_pyspark.Sparkclass({}).sparkStart({})

if __name__ == '__main__':
    main()