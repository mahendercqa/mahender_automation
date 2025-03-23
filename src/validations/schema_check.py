from pyspark.sql.functions import col, when, lower, trim, length
from src.utility.report_lib import write_output


def schema_check(source,target,spark):
    source_schema=spark.createDataFrame([(field.name,field.dataType.simpleString()) for field in source.schema ],['col_name','data_type'])
    source_schema.select("*").show()
    target_schema = spark.createDataFrame([(field.name, field.dataType.simpleString()) for field in target.schema], ['col_name', 'data_type'])
    target_schema.select("*").show()
    temp_join=source_schema.join(target_schema,lower(source_schema.col_name)==lower(target_schema.col_name),'full').select(lower((source_schema.col_name)).alias('source_col_name'),
                                                                           lower(source_schema.data_type).alias('source_data_type'),
                                                                           lower((target_schema.col_name)).alias('target_col_name'),
                                                                           lower(target_schema.data_type).alias('target_data_type'))
    temp_join.show()
    temp_join=temp_join.withColumn("comparision",when(col('source_data_type')==col('target_data_type'),'PASS').otherwise('FAIL')).filter(col('comparision')=='FAIL')
    temp_join.show()
    temp_count=temp_join.count()
    failed_records=temp_join.collect()
    failed_records_preview=[row.asDict() for row in failed_records ]
    if temp_count>0:
        status = 'FAIL'
        write_output(validation_type="schema_check",status=status,details=failed_records_preview)
    else:
        status = 'PASS'
        write_output(validation_type="schema_check", status=status, details=failed_records_preview)
    return status

