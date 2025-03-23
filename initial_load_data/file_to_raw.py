import random

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, col

snow_jar = r'C:\Users\mahen\PycharmProjects\Pytest\mahender_pytest_project\jars\snowflake-jdbc-3.14.3.jar'
postgres_jar = r'C:\Users\mahen\PycharmProjects\Pytest\mahender_pytest_project\jars\postgresql-42.7.3.jar'
jar_path = snow_jar+','+postgres_jar
spark = (SparkSession.builder.master("local[4]")
        .appName("pytest_framework")
        .config("spark.jars", jar_path)
        .config("spark.driver.extraClassPath", jar_path)
        .config("spark.executor.extraClassPath", jar_path)
        .getOrCreate())


batch_id = random.randint(1000, 9999)
current_time=current_timestamp()
df = spark.read.csv(r"C:\Users\mahen\PycharmProjects\Pytest\mahender_pytest_project\input_files\Contact_info.csv",header=True, inferSchema=True)
df.show()
df=df.withColumn('batch_id',lit(batch_id)).withColumn('start_date',current_time).withColumn('update_date',current_time).filter(col('identifier')==8)

(df.write.format("jdbc")
.option('url',"jdbc:postgresql://localhost:5432/postgres")
.option('driver', "org.postgresql.Driver")
.option('user', 'postgres')
.option('password', 'postgres')
.option('dbtable', 'test_table_raw').mode('append').save())

(df.write.format("jdbc")
.option('url',"jdbc:snowflake://gryzysc-de38647.snowflakecomputing.com/")
.option('driver', "net.snowflake.client.jdbc.SnowflakeDriver")
.option('user', 'MC')
.option('password', 'WZ98vXyNeK,%p4D')
.option('warehouse', 'COMPUTE_WH')
.option('db', 'MAHENDER')
.option('schema', 'TEST')
.option('dbtable', 'test_table_raw')
.mode('append').save())