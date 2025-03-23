from pyspark.sql.functions import  col,lit,when
from src.utility.report_lib import write_output


def data_compare_check(source,target,key_col):
    column_list=source.columns
    smt=source.exceptAll(target).withColumn("data_from",lit("source"))
    tms=target.exceptAll(source).withColumn("data_from",lit("target"))
    failed_records=smt.unionAll(tms)
    failed_records_count=failed_records.count()
    # failed_records.show()
    failed_records_preview=[]
    failed_records_preview_count = {}
    if failed_records_count>0:
        for column in column_list:
            if column not in key_col:
                key_col.append(column)
                source_temp=source.select(key_col).withColumnRenamed(column,"source_"+column)
                target_temp = target.select(key_col).withColumnRenamed(column, "target_" + column)
                key_col.remove(column)
                full_join=source_temp.alias('src').join(target_temp.alias('tgt'),key_col,'full')
                full_join=(full_join.withColumn("status",when(col('source_'+column)==col('target_'+column),'PASS').otherwise('FAIL'))
                           .filter(col('status')=='FAIL'))
                full_join_count=full_join.count()
                failed_records_collect=full_join.limit(5).collect()
                failed_records_preview.append([ row.asDict() for row in failed_records_collect])
                failed_records_preview_count[column]=full_join_count
        status = 'FAIL'
        write_output(validation_type='data_compare_check',status=status,details=f"failed_records_preview_count:{failed_records_preview_count}\nfailed_records_count:{failed_records_preview}")
    else:
        status = 'PASS'
        write_output(validation_type='data_compare_check', status=status,
                     details=f"failed_records_preview_count:{failed_records_preview_count}\nfailed_records_count:{failed_records_preview}")

    return status







    #
