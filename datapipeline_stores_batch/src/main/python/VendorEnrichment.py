# This script is performing some aggregations on DF's and merging with USD ref from another source and feeding to sink.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType,DoubleType
from pyspark.sql import functions as psf
from datetime import datetime, date, time, timedelta
from src.main.python.functions import read_schema
import configparser
spark = SparkSession.builder.appName("EnrichProductReference").getOrCreate()

# Fetching config file
config = configparser.ConfigParser()
config.read(r'../projectconfigs/config.ini')
inputLocation = config.get('paths', 'inputLocation')
outputLocation = config.get('paths', 'outputLocation')
landingSchemaFromConf = config.get('schema', 'landingFileSchema')

currDayZoneSuffix = "_03122021"
prevDayZoneSuffix = "_02122021"

productEnrichedInputSchema = StructType([
    StructField('Sale_ID',StringType(), True),
    StructField('Product_ID',StringType(), True),
    StructField('Product_Name',StringType(), True),
    StructField('Quantity_Sold',IntegerType(), True),
    StructField('Vendor_ID',StringType(), True),
    StructField('Sale_Date',TimestampType(), True),
    StructField('Sale_Amount',DoubleType(), True),
    StructField('Sale_Currency',StringType(), True)
])

vendorReferenceSchema = StructType([
    StructField('Vendor_ID',StringType(), True),
    StructField('Vendor_Name',StringType(), True),
    StructField('Vendor_Add_Street',StringType(), True),
    StructField('Vendor_Add_City',StringType(), True),
    StructField('Vendor_Add_State',StringType(), True),
    StructField('Vendor_Add_Country',StringType(), True),
    StructField('Vendor_Add_Zip',StringType(), True),
    StructField('Vendor_Updated_Date',TimestampType(), True)
])

usdReferenceSchema = StructType([
    StructField('Currency', StringType(), True),
    StructField('Currency_Code', StringType(), True),
    StructField('Exchange_Rate', FloatType(), True),
    StructField('Currency_Updated_Date', TimestampType(), True)
])
# reading product enriched from location
productEnrichedDF = spark.read\
    .schema(productEnrichedInputSchema)\
    .option("delimiter", "|")\
    .option("header", True)\
    .csv(outputLocation + "Enriched/SaleAmountEnrichment/SaleAmountEnriched" + currDayZoneSuffix)
productEnrichedDF.createOrReplaceTempView("productEnrichedDF")
# fetching usd reference from another source
usdReferenceDF = spark.read\
    .schema(usdReferenceSchema)\
    .option("delimiter", "|")\
    .csv(inputLocation + "USD_Rates")
usdReferenceDF.createOrReplaceTempView("usdReferenceDF")
# fetching vendor records from different location feeded by another source
vendorReferenceDF = spark.read\
    .schema(vendorReferenceSchema)\
    .option("delimiter", "|")\
    .option("header", False)\
    .csv(inputLocation + "Vendors")
vendorReferenceDF.createOrReplaceTempView("vendorReferenceDF")
# we are checking the performance of each individual vendor by joining with product enriched to do further analysis
vendorEnrichedDF = spark.sql("select a.*, b.Vendor_Name FROM "
                             "productEnrichedDF a INNER JOIN vendorReferenceDF b "
                             "ON a.Vendor_ID = b.Vendor_ID")
vendorEnrichedDF.createOrReplaceTempView("vendorEnrichedDF")
# we are converting our sales across the globe to a single currency with base value
usdEnrichedDF = spark.sql("select * , ROUND((a.Sale_Amount / b.Exchange_Rate), 2) as Amount_USD from "
                          "vendorEnrichedDF a JOIN usdReferenceDF b "
                          "ON a.Sale_Currency = b.Currency_Code")
# we are saving this to a location to use it later for internal reporting or many other use cases.
usdEnrichedDF.write\
    .option("delimiter", "|")\
    .option("header", True)\
    .mode("overwrite")\
    .csv(outputLocation + "Enriched/Vendor_USD_Enriched/Vendor_USD_Enriched" + currDayZoneSuffix)
# you can feed this directly to any consumer.
# example to feed this to a sql sink
# # MySql connectivity
usdEnrichedDF.write.format('jdbc').options(
     url='jdbc:mysql://....../dataprocessingpipelinedb',
     driver='com.mysql.jdbc.Driver',
     dbtable='readydetail',
     user='root',
     password='admin').mode('append').save()
