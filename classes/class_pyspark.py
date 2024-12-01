#!/usr/bin/python3

import os, json, re, sys
from typing import Callable, Optional
from colorama import Fore,Style,Back


from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark.context import SparkContext

class Sparkclass:
    def __init__(self,config:dict) -> None:
        self.config = config

    def sparkStart(self,kwargs:dict) -> SparkSession:
        MASTER = kwargs['spark_conf']['master']
        APP_NAME = kwargs['spark_conf']['app_name']
        LOG_LEVEL = kwargs['log']['level']

        def createSession(master:Optional[str]="local[*]",app_name:Optional[str]="myapp") -> SparkSession:
            """creates a spark session"""

            spark = (SparkSession
                    .builder
                    .appName(app_name)
                    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .config("spark.driver.bindAddress", "127.0.0.1") \
                    .config("spark.driver.host", "127.0.0.1") \
                    .master(master)
                    .getOrCreate()
                    )
            return spark
        
        def setLogging(spark:SparkSession, log_level:Optional[str]=None) -> None:
            spark.sparkContext.setLogLevel(log_level) if isinstance(log_level,str) else None

        def getSettings(spark:SparkSession) -> None:
            """ show spark settings"""
            print(Fore.BLUE , spark)
            print(Fore.GREEN,spark.sparkContext.getConf().getAll())
            print(Style.RESET_ALL)

        spark = createSession(MASTER,APP_NAME)
        setLogging(spark,LOG_LEVEL)
        # getSettings(spark)

        return spark

    def importData(self,spark:SparkSession,datapath:str,pattern:Optional[str]=None) -> DataFrame:
        
        def fileOrDirectory(datapath:str) -> str:
            if isinstance(datapath,str) and os.path.exists(datapath):
                if os.path.isdir(datapath):
                    return 'dir'
                elif os.path.isfile(datapath):
                    return 'file'

        def openDirectory(spark:SparkSession,getUniqueFileExtension,datapath:str,pattern:Optional[str]=None):
            newlist = Sparkclass(self.config).listDirectory(datapath,pattern)
            filetype = getUniqueFileExtension(newlist)
            if filetype:
                Sparkclass(self.config).createDataFrame( spark, newlist, filetype ) 

        def openFile(filepath:str):
            filelist = [filepath]

        def getUniqueFileExtension(filelist:list) -> list:
            # if isinstance(filelist,list) and len(filelist) > 0:
                
            exts = list(set(os.path.splitext(f)[1] for f in filelist))
            return exts[0] if len(exts) == 1 else None

        pathtype = fileOrDirectory(datapath)
        openDirectory(spark,getUniqueFileExtension,datapath,pattern) if pathtype == "dir" else None


    def listDirectory(self,directory,pattern=None) -> list:
        def recursiveFilelist(directory):
            if os.path.exists(directory):
                filelist = []

                for dirpath,dirname,filenames in os.walk(directory):
                    for filename in filenames:
                        filelist.append(f"{dirpath}/{filename}")
                print(filelist)
                return filelist
            
        def filterFiles(filelist:list,pattern:str):
            if filelist:
                return [x for x in filelist if re.search(f"{pattern}",x)]
            else:
                return ["json"]
            
        filelist = recursiveFilelist(directory)
        return filterFiles(filelist,pattern) if (pattern is not None or pattern != "") else filelist

    def createDataFrame(self, spark:SparkSession, fileList: list, filetype:str ) -> DataFrame:
        print("inside dataframe")
        def dfFromCSV(fileList:list) -> DataFrame:
            pass

        def dfFromJSON():
            pass