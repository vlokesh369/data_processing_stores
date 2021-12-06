# This script is deriving a column by joining the existing df from one location and merging it with
# product reference from another source and writing out the file to a different location.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType,DoubleType
from pyspark.sql import functions as psf
from datetime import datetime, date, time, timedelta
from src.main.python.functions import read_schema
import configparser
spark = SparkSession.builder.appName("EnrichProductReference").master("local").getOrCreate()

# Fetching config file
config = configparser.ConfigParser()
config.read(r'../projectconfigs/config.ini')
inputLocation = config.get('paths', 'inputLocation')
outputLocation = config.get('paths', 'outputLocation')
landingSchemaFromConf = config.get('schema', 'landingFileSchema')

currDayZoneSuffix = "_03122021"
prevDayZoneSuffix = "_02122021"

# Reading the schema
validFileSchema = read_schema(landingSchemaFromConf)
productPriceReferenceSchema = StructType([
    StructField('Product_ID',StringType(), True),
    StructField('Product_Name',StringType(), True),
    StructField('Product_Price',IntegerType(), True),
    StructField('Product_Price_Currency',StringType(), True),
    StructField('Product_updated_date',TimestampType(), True)
])

# Reading Valid Data from our location
validDataDF = spark.read\
    .schema(validFileSchema)\
    .option("delimiter", "|")\
    .option("header", True)\
    .csv(outputLocation + "Valid/ValidData"+currDayZoneSuffix)
validDataDF.createOrReplaceTempView("validData")

# Reading Project Reference dumped from different source to different location
productPriceReferenceDF = spark.read\
    .schema(productPriceReferenceSchema)\
    .option("delimiter", "|")\
    .option("header", True)\
    .csv(inputLocation + "Products")
productPriceReferenceDF.createOrReplaceTempView("productPriceReferenceDF")
# deriving sale_amount column from product_price and QTY_sold
productEnrichedDF = spark.sql("select a.Sale_ID, a.Product_ID, b.Product_Name, "
                              "a.Quantity_Sold, a.Vendor_ID, a.Sale_Date, "
                              "b.Product_Price * a.Quantity_Sold as Sale_Amount,"
                              "a.Sale_Currency "
                              "from validData a INNER JOIN productPriceReferenceDF b "
                              "ON a.Product_ID = b.Product_ID")
# writing out to a different location
productEnrichedDF.write\
    .option("header", True)\
    .option("delimiter","|")\
    .mode("overwrite")\
    .csv(outputLocation + "Enriched/SaleAmountEnrichment/SaleAmountEnriched" + currDayZoneSuffix)
