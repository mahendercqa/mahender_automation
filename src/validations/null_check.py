from pyspark.sql.functions import col,trim,upper,isnan
from src.utility.report_lib import write_output


def null_check(target,key_cols):
    failed_records_preview=[]
    failed_records_count_review={}
    for column in key_cols:
        null_records=target.filter((col(column).isNull()) |
                                   (upper(col(column))=='NULL') |
                                   (trim(col(column))=='') |
                                   (isnan(col(column))) |
                                   (upper(col(column))=='NONE')
                                   )
        null_records_count=null_records.count()
        failed_records_count_review[column]=null_records_count
        null_records_collect=null_records.select(column).collect()
        failed_records_preview.append([row.asDict() for row in null_records_collect])
    if all(count==0 for count in failed_records_count_review.values()):
        status='PASS'
        write_output(validation_type='null_check',status=status,details=f"failed_records_preview:{failed_records_preview}\nfailed_records_count_review:{failed_records_count_review}")
    else:
        status = 'FAIL'
        write_output(validation_type='null_check', status=status,
                     details=f"failed_records_preview:{failed_records_preview}\nfailed_records_count_review:{failed_records_count_review}")
    return status



