#!/usr/bin/python3

import os, json, re, sys
from typing import Callable, Optional
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession

class Sparkclass:
    def __init__(self,config:dict) -> None:
        self.config = config

    def sparkStart(self,kwargs:dict) -> SparkSession:
        pass