from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, FloatType, ArrayType, DateType

#create a StructType for the Kafka redis-server topic which has all changes made to Redis - before Spark 3.0.0, schema inference is not automatic
redisCustomerInfoSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue", StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr", StringType()),
        StructField("zSetEntries", ArrayType( \
            StructType([
                    StructField("element", StringType()),\
                    StructField("score", StringType()) \
        ])))                                           \
    ]
)

#create a StructType for the Customer JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic
customerInfoJSONschema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("birthDay", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType())
    ]
)

#create a StructType for the Kafka stedi-events topic which has the Customer Risk JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic
customerRiskJSONschema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType())
    ]
)

#creating a spark application object
spark = SparkSession.builder.appName("customer-fall-risk").getOrCreate()

#setting the spark log level to WARN
spark.sparkContext.setLogLevel('WARN')

#using the spark application object to read a streaming dataframe from the Kafka topic redis-server as the source
#specifying "--from-beginning" option to read all events from the topic including those published before spark stream started
redisCustomerInfoRawDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe","redis-server")\
    .option("startingOffsets","earliest")\
    .load()

#casting the value column in the streaming dataframe as a STRING 
redisCustomerInfoDF = redisCustomerInfoRawDF.selectExpr("cast(key as string) key","cast(value as string) value")

#parsing the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"key":"Q3..|
# +------------+
#
# with this JSON format: {"key":"Q3VzdG9tZXI=",
# "existType":"NONE",
# "Ch":false,
# "Incr":false,
# "zSetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "Score":0.0
# }],
# "zsetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "score":0.0
# }]
# }
# 
# (The Redis Source for Kafka has redundant fields zSetEntries and zsetentries, only parsing zSetEntries)
#
# and creating separated fields like this:
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
#
# and storing them in a temporary view called RedisSortedSet
redisCustomerInfoDF.withColumn("value", from_json("value",redisCustomerInfoSchema))\
    .select("value.*")\
    .createOrReplaceTempView("RedisSortedSet")

#Executing a sql statement against the temporary view, which statement takes the element field from the 0th element in the array of structs and create a column called encodedCustomer
# the reason we do it this way is that the syntax available select against a view is different than a dataframe, and it makes it easy to select the nth element of an array in a sql column
encodedCustomer = spark.sql("select key, zSetEntries[0].element as customer from RedisSortedSet")

# Taking the encodedCustomer column which is base64 encoded at first like this:
# +--------------------+
# |            customer|
# +--------------------+
# |[7B 22 73 74 61 7...|
# +--------------------+

# and converting it to clear json like this:
# +--------------------+
# |            customer|
# +--------------------+
# |{"customerName":"...|
#+--------------------+
#
# with this JSON format: {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}
decodedCustomer = encodedCustomer.withColumn("customer",unbase64(encodedCustomer.customer).cast("string"))

# Parsing the JSON in the Customer record and store in a temporary view called CustomerRecords
decodedCustomer\
    .withColumn("customer", from_json("customer",customerInfoJSONschema))\
    .select(col("customer.*"))\
    .createOrReplaceTempView("CustomerRecords")

# JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF
emailAndBirthDayStreamingDF=spark.sql("select email, birthDay from CustomerRecords where email is not null")

# from the emailAndBirthDayStreamingDF dataframe select the email and the birth year (using the split function)

# Splitting the birth year as a separate field from the birthday
# Selecting only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
emailAndBirthYearStreamingDF=emailAndBirthDayStreamingDF.select("email",split(emailAndBirthDayStreamingDF.birthDay,"-").getItem(0).alias("birthYear"))

# Sinking the emailAndBirthYearStreamingDF dataframe to the console in append mode
# 
# The output should look like this:
# +--------------------+-----               
# | email         |birthYear|
# +--------------------+-----
# |Gail.Spencer@test...|1963|
# |Craig.Lincoln@tes...|1962|
# |  Edward.Wu@test.com|1961|
# |Santosh.Phillips@...|1960|
# |Sarah.Lincoln@tes...|1959|
# |Sean.Howard@test.com|1958|
# |Sarah.Clark@test.com|1957|
# +--------------------+-----
#
emailAndBirthYearStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()

# Run the python script by running the command from the terminal:
# /home/workspace/submit-redis-kafka-streaming.sh
# Ideally verifying the data looks correct 