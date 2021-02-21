from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, FloatType, BooleanType, ArrayType, DateType

# Using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
customerRiskJSONschema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", StringType())
    ]
)

spark = SparkSession.builder.appName("customer-fall-risk").getOrCreate()
spark.sparkContext.setLogLevel('WARN')
customerRiskRawDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe","stedi-events")\
    .option("startingOffsets","earliest")\
    .load()
                                   
# Casting the value column in the streaming dataframe as a STRING 
customerRiskDF = customerRiskRawDF.selectExpr("cast(key as string) key","cast(value as string) value")

# Parsing the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
# storing them in a temporary view called CustomerRisk
customerRiskDF.withColumn("value", from_json("value",customerRiskJSONschema))\
    .select("value.*")\
    .createOrReplaceTempView("CustomerRisk")
# Executing a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF=spark.sql("select customer,score from CustomerRisk")
# Sinking the customerRiskStreamingDF dataframe to the console in append mode
# 
# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----
customerRiskStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()
#redisCustomerRiskDF.writeStream.outputMode("append").format("console").start().awaitTermination()
# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafka-streaming.sh
# Verify the data looks correct 