from src.validations.count_check import count_check
from src.validations.data_compare_check import data_compare_check
from src.validations.duplicate_check import duplicate_check
from src.validations.null_check import null_check
from src.validations.schema_check import schema_check
from src.validations.uniqueness_check import uniqueness_check


# def test_schema_check(read_data, spark_session):
#     spark = spark_session
#     source, target, config = read_data
#     assert schema_check(source=source, target=target, spark=spark) == 'PASS'


def test_count_check(read_data, spark_session):
    spark = spark_session
    source, target, config = read_data
    key_col = config['validations']['count_check']['key_col']
    assert count_check(source=source, target=target, key_col=key_col) == 'PASS'

#
# def test_duplicate_check(read_data, spark_session):
#     spark = spark_session
#     source, target, config = read_data
#     key_col = config['validations']['duplicate_check']['key_col']
#     assert duplicate_check(target=target, key_col=key_col) == 'PASS'
#
#
# def test_uniqueness_check(read_data, spark_session):
#     spark = spark_session
#     source, target, config = read_data
#     key_col = config['validations']['uniqueness_check']['key_col']
#     assert uniqueness_check(target=target, key_cols=key_col) == 'PASS'
#
# def test_null_check(read_data, spark_session):
#     spark = spark_session
#     source, target, config = read_data
#     key_col = config['validations']['null_check']['key_col']
#     assert null_check(target=target, key_cols=key_col) == 'PASS'
#
# def test_data_compare_check(read_data, spark_session):
#     spark = spark_session
#     source, target, config = read_data
#     key_col = config['validations']['data_compare_check']['key_col']
#     assert data_compare_check(source=source,target=target, key_col=key_col) == 'PASS'