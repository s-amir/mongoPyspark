from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, IntegerType

my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27018/") \
    .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27018/") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
    .getOrCreate()
# -------------------------------------------------------------------------------------------
#
# dataFrame = my_spark.read\
#                  .format("mongodb")\
#                  .option("database", "people")\
#                  .option("collection", "contacts")\
#                  .load()
# # dataFrame.show(100)
# dataFrame.printSchema()
#
# dataFrame.show()
#
# dataFrame.createOrReplaceTempView("temp")
# my_spark.sql('select * from temp').show(100)


# -----------------------------------------------------------------------------

'''
as far as mongodb create collection and database on the fly , there is no need to create them before insertion

mode:append and overwrite
'''

# dataFrame = my_spark.createDataFrame(
#     [('20', "amir", 50), ('21', "Gandalf", 1000), ('22', "Thorin", 195), ('23', "Balin", 178), ('4', "Kili", 77),
#      ('5', "Dwalin", 169)], ["_id", "name", "age"])
# dataFrame.write.format("mongodb") \
#     .mode("append") \
#     .option("database", "people") \
#     .option("collection", "contactssss") \
#     .option("upsertDocument", "true") \
#     .option('idFieldList', "name") \
#     .save()

# -------------------------------------------------------------
# define the schema of the source collection
# readSchema = (StructType()
#   .add('_id', StringType())
#   .add('name', StringType())
#   .add('age', IntegerType())
# )
# # define a streaming query
# dataStreamWriter = (my_spark.readStream
#   .format("mongodb")
#   .option('spark.mongodb.database','people')
#   .option('spark.mongodb.collection', 'contacts')
#   .option ("forceDeleteTempCheckpointLocation", "true")
#   .option("checkpointLocation", "/tmp")
#   .schema(readSchema)
#   .load()
#   # manipulate your streaming data
#   .writeStream
#   .format("console")
#   .option("checkpointLocation", "/tmp")
#   .trigger(continuous="3 second")
#   .outputMode("append")
# )
# # run the query
# query = dataStreamWriter.start().awaitTermination()


# _____________________________________________________________________________
# def print_log():
#     print('add to mongo')
#
# csv_schema = (StructType()
#               .add('name', StringType())
#               .add('country', StringType())
#               .add('email', IntegerType())
#               )
#
# dataStreamWriter = (my_spark.readStream
#                     .format("csv")
#                     .option("header", "true")
#                     .schema(csv_schema)
#                     .load("/home/amirhosein/PycharmProjects/mongoPyspark4/csv_data/")
#                     # manipulate your streaming data
#                     .writeStream
#                     .format("mongodb")
#                     .option("checkpointLocation", "/tmp/pyspark7/")
#                     .option("forceDeleteTempCheckpointLocation", "true")
#                     .option("spark.mongodb.database", 'people')
#                     .option("spark.mongodb.collection", 'contacts3')
#                     .outputMode("append")
#                     )
#
# dataStreamWriter.start()
