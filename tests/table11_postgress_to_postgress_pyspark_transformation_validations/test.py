import subprocess
import os


from general_lib.flatten import flatten
from pyspark.sql import SparkSession

import yaml
import json
print(yaml.__version__)

from pyspark.sql.functions import col
from pyspark.sql.types import StructType

print("test")

#subprocess.run(r"C:\Users\mahen\PycharmProjects\Pytest\mahender_pytest_project\tests\table11_postgress_to_postgress_pyspark_transformation_validations\pyspark_transformed_data.py")
subprocess.run(["python", "C:\\Users\\mahen\\PycharmProjects\\Pytest\\mahender_pytest_project\\tests\\table11_postgress_to_postgress_pyspark_transformation_validations\\pyspark_transformed_data.py"])