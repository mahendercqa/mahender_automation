from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType


def pyspark_transformed_data(df):
    transfermed_df = df.withColumn('given_name',upper(col('given_name')))
    return transfermed_df

