from pyspark.sql.functions import  col
from src.utility.report_lib import write_output


def duplicate_check(target,key_col):
    dup=target.groupBy(key_col).count().filter(col('count')>1)
    dup_count=dup.count()
    failed_records=dup.collect()
    failed_records_preview=[row.asDict() for row in failed_records]
    if dup_count>0:
        status='FAIL'
        write_output(validation_type='duplicate_check',status=status,details=failed_records_preview)
    else:
        status = 'PASS'
        write_output(validation_type='duplicate_check', status=status, details='no duplicate records')

    return status