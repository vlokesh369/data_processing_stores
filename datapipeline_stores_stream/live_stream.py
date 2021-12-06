# This script streams the live data and calculate sale amount for live sales
# It also gets the aggregated values of total sales and quantities of products being sold by each vendor in streaming data.

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType,DoubleType
from pyspark.sql import functions as psf
from datetime import datetime, date, time, timedelta
from src.main.python.functions import read_schema
import configparser
spark = SparkSession.builder.appName("live_stream").getOrCreate()
ProductInputLocation= # product ref location
StreamInputLocation= # location of stream
LiveStreamSchema=StructType([
    StructField('Sale_ID',StringType(), True),
    StructField('Product_ID',StringType(), True),
    StructField('Quantity_Sold',IntegerType(), True),
    StructField('Vendor_ID',StringType(), True),
    StructField('Sale_Date',TimestampType(), True),
    StructField('Sale_Amount',DoubleType(), True),
    StructField('Sale_Currency',StringType(), True)])
ProductReferenceSchema=StructType([
    StructField('Product_ID',StringType(), True),
    StructField('Product_Name',StringType(), True),
    StructField('Product_Price',IntegerType(), True),
    StructField('Product_Price_Currency',StringType(), True),
    StructField('Product_updated_date',TimestampType(), True)
])
# Reading product ref file
ProductPriceReferenceDF=spark.read\
    .schema(ProductReferenceSchema)\
    .option("delimiter","|")\
    .option("header",True)\
    .csv(ProductInputLocation)
ProductPriceReferenceDF.createOrReplaceTempView("ProductPriceReferenceDF")
# Read live stream
LiveStreamDF=spark.readStream\
    .option("delimiter","|")\
    .schema(LiveStreamSchema)\
    .csv(streaminputlocation)
LiveStreamDF.createOrReplaceTempView("LiveStreamDF")
# writing out the stream to check if we are getting data.
LiveStreamDF.writeStream\
    .format("console")\
    .outputmode("complete")\
    .start()\
    .awaittermination()
# we can have any stream generator/be it kafka producer or any other live data source feeding data to input location
# Doing some aggregations on live data with reference from different source.
LiveStreamDFWithAmount=spark.sql("select ls.Sale_ID,ls.Product_ID,ls.Quantity_Sold,ls.Vendor_ID,ls.Sale_Date,"
                                 "pr.Product_Price*ls.Quantity_Sold as Sale_Amount,"
                                 "ls.Sale_Currency" 
                                 "from LiveStreamDF ls left outer join ProductPriceReferenceDF pr"
                                 "on ls.Product_ID=pr.Product_ID")

LiveStreamDFWithAmount= createOrReplaceTempView("LiveStreamDFWithAmount")

LiveStreamDF_aggregated= spark.sql("select Product_ID,Vendor_ID,sum(Quantity_Sold) as Quantity_Sold"
                                   ",sum(Sale_Amount) as Sale_Amount"
                                   "from LiveStreamDFWithAmount"
                                   "group by Product_ID,Vendor_ID")
# writing out stream data to console/we can write to kafka sink as well
# writeStream
#     .format("parquet")        // can be "orc", "json", "csv", etc.
#     .option("path", "path/to/destination/dir")
#     .start()
LiveStreamDF_aggregated.writeStream\
    .format("console")\
    .option("numRows",50)\
    .outputmode("complete") \  # streaming aggregations need the o/p mode to complete
    .start() \
    .awaittermination()

