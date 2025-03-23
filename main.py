import json

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType

#
spark = SparkSession.builder.master('local[6]').appName("adfaf").getOrCreate()

df = spark.read.csv("C:\\Users\\mahen\\PycharmProjects\\Pytest\\mahender_pytest_project\\input_files\\test.csv",
                    header=True, inferSchema=True, sep=',')
# df = spark.read.option('multiline',True).json(
#     "C:\\Users\\mahen\\PycharmProjects\\Pytest\\mahender_pytest_project\\input_files\\Complex2.json")
#
df.show()
df.withColumn('status',col('id') == col('id_1')).show()
df.withColumn('status',when(col('id') == col('id_1'),'pass').otherwise('fail')).show()
df.printSchema()
# print(df.schema)
# print(df.schema.json())
#
#
# def read_schema():
#     with open(
#             r"C:\Users\mahen\PycharmProjects\Pytest\mahender_pytest_project\tests\table10_csv_to_postgress_validations\schema.json",
#             'r') as file:
#         data = StructType.fromJson(json.load(file))
#         return data
#
#
# schema = read_schema()
# df = spark.read.schema(schema).csv(
#     "C:\\Users\\mahen\\PycharmProjects\\Pytest\\mahender_pytest_project\\input_files\\source_file.csv", header=True,
#     sep=',')
# df.show()
# print(df.schema)
# print(df.schema.json())

# source_schema=spark.createDataFrame(df.schema)
# source_schema.show()
#
# print(None==None)
#
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, isnan
#
# spark = SparkSession.builder.appName("NaN Example").getOrCreate()
#
# # data = [(1, float('nan')), (2, 5.0), (3, None), (4, float('nan'))]
# data = [('1', float('nan')), ('2', 5.0), ('3', None), ('4', float('nan'))]
# columns = ["id", "value"]
#
# df = spark.createDataFrame(data, columns)
#
# df.select(col("id"), col("value"), isnan(col("value")).alias("is_nan")).show()
# df.printSchema()
#
# print(df.collect())
#
# from pyspark.sql import SparkSession
# from pyspark.sql import Row
#
# spark = SparkSession.builder.appName("CollectExample").getOrCreate()
#
# Creating a DataFrame
# data = [Row(id=1, name=""), Row(id=2, name="")]
# df = spark.createDataFrame(data)
# df.show()
# # Collecting data
# collected_data = df.collect()
#
# # Printing results
# print(collected_data)
