#  This script is loading the .csv files from landing zone and merging with previous hold data and creating a refreshed
# landing zone. We are segregating the load into valid and invalid based on some conditions.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType,DoubleType
from pyspark.sql import functions as psf
from datetime import datetime, date, time, timedelta
import configparser
from src.main.python.functions import read_schema

# Creating spark session
spark = SparkSession.builder.appName("DataIngestAndRefine").master("local").getOrCreate()

# Creating spark context (if required)
sc = spark.sparkContext

# Fetching config file
config = configparser.ConfigParser()
config.read(r'../projectconfigs/config.ini')  # Read config file properties
inputLocation = config.get('paths', 'inputLocation')
outputLocation = config.get('paths', 'outputLocation')
landingSchemaFromConf = config.get('schema', 'landingFileSchema')
holdFileSchemaFromConf = config.get('schema', 'holdFileSchema')

# Defining Landing and Hold File schema
landingFileSchema = read_schema(landingSchemaFromConf)
holdFileSchema = read_schema(holdFileSchemaFromConf)

# Defining current date landing zone
dateToday = datetime.now()
yesterDate = dateToday - timedelta(1)
# currDayZoneSuffix = "_" + dateToday.strftime("%d%m%Y")  # _05062020
# prevDayZoneSuffix = "_" + yesterDate.strftime("%d%m%Y")  # _04062020
currDayZoneSuffix = "_03122021"  # hardcoded for my sample data
prevDayZoneSuffix = "_02122021"  # hardcoded for my sample data

# Reading input data
landingFileDF = spark.read\
    .schema(landingFileSchema)\
    .option("delimiter", "|")\
    .csv(inputLocation + "Sales_Landing/SalesDump"+currDayZoneSuffix)

# Creating a view on the spark data frame
landingFileDF.createOrReplaceTempView("landingFileDF")


# Checking if updates are received on any previous HOLD Data
previousHoldDF = spark.read \
    .schema(holdFileSchema) \
    .option("delimiter", "|") \
    .option("header", True) \
    .csv(outputLocation + "Hold/HoldData"+prevDayZoneSuffix)

previousHoldDF.createOrReplaceTempView("previousHoldDF")
# refreshing the current landing schema
refreshedLandingData = spark.sql("select a.Sale_ID, a.Product_ID, "
          "CASE "
          "WHEN (a.Quantity_Sold IS NULL) THEN b.Quantity_Sold "
          "ELSE a.Quantity_Sold "
          "END AS Quantity_Sold, "
          "CASE "
          "WHEN (a.Vendor_ID IS NULL) THEN b.Vendor_ID "
          "ELSE a.Vendor_ID "
          "END AS Vendor_ID, "
          "a.Sale_Date, a.Sale_Amount, a.Sale_Currency "
          "from landingFileDF a left outer join previousHoldDF b on a.Sale_ID = b.Sale_ID ")

refreshedLandingData.createOrReplaceTempView("refreshedLandingData")
# segregating valid data
validLandingData = refreshedLandingData.filter(psf.col("Quantity_Sold").isNotNull() & psf.col("Vendor_ID").isNotNull())
validLandingData.createOrReplaceTempView("validLandingData")
# fetch records that are released from hold
releasedFromHold = spark.sql("select vd.Sale_ID "
                             "from validLandingData vd inner join previousHoldDF phd "
                             "on vd.Sale_ID = phd.Sale_ID")
releasedFromHold.createOrReplaceTempView("releasedFromHold")
# fetch records that are not released from hold
notReleasedFromHold = spark.sql("select * from previousHoldDF "
                                "where Sale_ID not in (select Sale_ID from releasedFromHold)")
notReleasedFromHold.createOrReplaceTempView("notReleasedFromHold")
# segregating current invalid data and adding a hold reason column
inValidLandingData = refreshedLandingData.filter(psf.col("Quantity_Sold").isNull() | psf.col("Vendor_ID").isNull() |
                                                 psf.col("Sale_Currency").isNull())\
    .withColumn("Hold_Reason", psf
                .when(psf.col("Quantity_Sold").isNull(), "Qty Sold Missing")
                .otherwise(psf.when(psf.col("Vendor_ID").isNull(), "Vendor ID Missing")))\
    .union(notReleasedFromHold)

# Separate Valid and invalid data into valid and Hold zone and writing out to respective locations
validLandingData.write\
    .mode("overwrite")\
    .option("delimiter", "|")\
    .option("header", True)\
    .csv(outputLocation + "Valid/ValidData"+currDayZoneSuffix)

inValidLandingData.write\
    .mode("overwrite")\
    .option("delimiter", "|")\
    .option("header", True)\
    .csv(outputLocation + "Hold/HoldData"+currDayZoneSuffix)