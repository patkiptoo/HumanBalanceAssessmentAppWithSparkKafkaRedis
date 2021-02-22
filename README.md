# HumanBalanceAssessmentAppWithSparkKafkaRedis
# A data engineering project utilizing Kafka, Apache Spark and Redis to provide data to populate an existing graph showing the fall risk for senior citizen patients. 
# The fall risk test is conducted by taking timings how quickly a senior takes 30 steps walking. 
# The timings is an array of 30 readings. 4 such tests are carried out and a risk calculation is obtained. 
This data is fed into a kafka topic. Separate biographic data about the patient is stored during registration in a redis database. 
The redis data is extracted into a separate kafka topic using kafka connect for redis.
The data from the two topis is consumed by spark and a join performed to produce a dataframe with the data needed for the end-user patient graph. 
This is in turn fed into a kafka topic where it is consumed by the gragh and presented to clinic manager for  analysis and care of the patient.

Consuming graph sample

