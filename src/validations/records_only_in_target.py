from src.utility.report_lib import write_output


def records_only_in_target(source,target,key_col):
    records_only_in_target=target.select(key_col).exceptAll(source.select(key_col))
    records_only_in_target_count=records_only_in_target.count()
    failed_records = records_only_in_target.collect()
    failed_records_preview = [row.asDict() for row in failed_records]
    if records_only_in_target_count>0:
        status='FAIL'
        write_output(validation_type='records_only_in_target',status=status,details=failed_records_preview)
    else:
        status='PASS'
        write_output(validation_type='records_only_in_target', status=status, details=failed_records_preview)


