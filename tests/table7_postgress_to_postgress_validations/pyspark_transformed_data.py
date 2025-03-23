from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, LongType


def pyspark_transformed_data(df):
    transfermed_df = df.withColumn('employee_id',col('employee_id').cast(LongType()))
    return transfermed_df

