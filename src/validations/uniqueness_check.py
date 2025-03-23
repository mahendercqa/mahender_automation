from pyspark.sql.functions import col,trim
from src.utility.report_lib import write_output


def uniqueness_check(target,key_cols):
    failed_records_preview=[]
    failed_records_count={}
    for column in key_cols:
        dup=target.groupBy(trim(column).alias(f"{column}")).count().filter(col('count')>1)
        dup_records=dup.collect()
        failed_dup_records_preview=[row.asDict() for row in dup_records]
        failed_records_preview.append(failed_dup_records_preview)
        dup_count=dup.count()
        print("********************************",type(dup_count))
        failed_records_count[column]=dup_count
    if all(count==0 for count in failed_records_count.values()):
        print(failed_records_count)
        status='PASS'
        write_output(validation_type='uniqueness_check',status=status,details=f"failed_records_preview:{failed_records_preview}\nfailed_records_count:{failed_records_count}")
    else:
        print(failed_records_count)
        status='FAIL'
        write_output(validation_type='uniqueness_check', status=status,
                     details=f"failed_records_preview:{failed_records_preview}\nfailed_records_count:{failed_records_count}")
    return status