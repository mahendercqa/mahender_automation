import os
import subprocess

from general_lib.flatten import flatten
from pyspark.sql import SparkSession
import pytest
import yaml
import json

from pyspark.sql.types import StructType
from tests.table10_csv_to_postgress_validations.pyspark_transformed_data import pyspark_transformed_data


@pytest.fixture(scope='session')
def spark_session(request):
    #dir_path = request.node.fspath.dirname
    snow_jar = r'C:\Users\mahen\PycharmProjects\Pytest\mahender_pytest_project\jars\snowflake-jdbc-3.14.3.jar'
    postgres_jar = r'C:\Users\mahen\PycharmProjects\Pytest\mahender_pytest_project\jars\postgresql-42.7.3.jar'
    # azure_storage = '/Users/admin/PycharmProjects/test_automation_project/jar/azure-storage-8.6.6.jar'
    # hadoop_azure = '/Users/admin/PycharmProjects/test_automation_project/jar/hadoop-azure-3.3.1.jar'
    # sql_server = '/Users/admin/PycharmProjects/taf/jars/mssql-jdbc-12.2.0.jre8.jar'
    #jar_path = snow_jar + ',' + postgres_jar + ',' + azure_storage + ',' + hadoop_azure + ',' + sql_server
    jar_path = snow_jar+','+postgres_jar
    spark = SparkSession.builder.master("local[4]") \
        .appName("pytest_framework") \
        .config("spark.jars", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .config("spark.executor.extraClassPath", jar_path) \
        .getOrCreate()
    return spark


def read_config(dir):
    path = os.path.join(dir, 'config.yml')
    # path = dir + r'\config.yml'
    with open(path, 'r') as file:
        data = yaml.safe_load(file)
    return data


# def read_explicit_schema(dir):
#     path = os.path.join(dir, 'schema.json')
#     # path = dir + r'\schema.json'
#     print("****************************,&&&&&&&&&&&&&&&&&&&&&&&&&")
#     print(path)
#     with open(path, 'r') as file:
#         data = StructType.fromJson(json.load(file))
#     return data

#
# def read_file(config, spark, dir):
#     if config['type'] == 'csv':
#         if config['schema'] == 'Y':
#             schema = read_explicit_schema(dir=dir)
#             df = spark.read.schema(schema).csv(config['path'], header=config['options']['header'],
#                                                sep=config['options']['sep'])
#         else:
#             df = spark.read.csv(config['path'], header=config['options']['header'],
#                                 inferSchema=config['options']['inferschema'], sep=config['options']['sep'])
#     elif config['type'] == 'json':
#         if config['schema'] == 'Y':
#             schema = read_explicit_schema(dir=dir)
#             df = spark.read.option("multiline", config['options']['multiline']).schema(schema).json(config['path'])
#             df = flatten(df)
#             df.show()
#         else:
#             df = spark.read.option("multiline", config['options']['multiline']).json(config['path'])
#             df = flatten(df)
#             df.show()
#     elif config['type'] == 'parquet':
#         df = spark.read.parquet(config['path'])
#     elif config['type'] == 'avro':
#         if config['schema'] == 'Y':
#             schema = read_explicit_schema(dir=dir)
#             df = spark.read.format("avro").schema(schema).load(config['path'])
#         else:
#             df = spark.read.format('avro').load(config['path'])
#
#     return df


def load_credentails(database, dir):
    parent_path = os.path.dirname(os.path.dirname(dir)) + '/config/database_connection.json'
    with open(parent_path, 'r') as file:
        data = json.load(file)[database]
        return data

# def read_query(dir):
#     path=os.path.join(dir,'sql_query.sql')
#     with open(path,'r') as file:
#         data=file.read()
#         return data


def read_db(spark, config, dir):
    database = config['cred_lookup']
    cred_config = load_credentails(database, dir)
    df = (spark.read.format("jdbc").option('url', cred_config['url']).option('driver', cred_config['driver']).
          option('user', cred_config['user']).option('password', cred_config['password']).option('dbtable', config['expected_table']).load())
    return df

def read_snowflake_db(spark, config, dir):
    database = config['cred_lookup']
    cred_config = load_credentails(database, dir)
    # # if config['sql_query']=='Y':
    #     # sql_query=read_query(dir)
    #     df = (spark.read.format("jdbc").option('url', cred_config['url']).option('driver', cred_config['driver']).option('warehouse', cred_config['warehouse'])
    #           .option('db', cred_config['db']).option('schema', cred_config['schema']).
    #           option('user', cred_config['user']).option('password', cred_config['password']).option('query', sql_query).load())
    #

    df = (

        spark.read.format("jdbc").option('url', cred_config['url']).option('driver', cred_config['driver']).option(
            'warehouse', cred_config['warehouse'])

        .option('db', cred_config['db']).option('schema', cred_config['schema']).

        option('user', cred_config['user']).option('password', cred_config['password']).option('dbtable',
                                                                                               config['table']).load())

    return df




@pytest.fixture(scope='module')
def read_data(spark_session, request):
    dir = request.node.fspath.dirname
    config = read_config(dir)
    spark = spark_session
    source_config = config['source']
    target_config = config['target']
    if source_config['type'] == 'database':
        print("*"*100)
        print(dir)
        print(source_config)
        if source_config['transformation'][0].lower() == 'y' and source_config['transformation'][1].lower() == 'python':
            python_file_path = os.path.join(dir,'pyspark_transformed_data.py')
            print("*" * 100)
            print(python_file_path)
            subprocess.run(python_file_path)
        source = read_db(spark=spark, config=source_config, dir=dir)
    elif source_config['type'] == 'snowflake_database':
        source = read_snowflake_db(spark=spark, config=source_config, dir=dir)
    # else:
    #     source = read_file(spark=spark, config=source_config, dir=dir)

    if target_config['type'] == 'database':
        target = read_db(spark=spark, config=target_config, dir=dir)
    elif target_config['type'] == 'snowflake_database':
        target = read_snowflake_db(spark=spark, config=target_config, dir=dir)
    # else:
    #     target = read_file(spark=spark, config=target_config, dir=dir)

    return source, target, config
