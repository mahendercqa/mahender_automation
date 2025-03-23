from src.utility.report_lib import write_output
from src.validations.records_only_in_source import records_only_in_source
from src.validations.records_only_in_target import records_only_in_target


def count_check(source,target,key_col):
    src_count=source.count()
    tgt_count = target.count()
    diff=abs(tgt_count-src_count)
    if diff>0:
        status='FAIL'
        write_output(validation_type='count_check',status=status,details=f"source_count:{src_count} and target_count:{tgt_count}")
        records_only_in_source(source,target,key_col)
        records_only_in_target(source,target,key_col)
    else:
        status = 'PASS'
        write_output(validation_type='count_check', status=status,
                     details=f"source_count:{src_count} and target_count:{tgt_count}")
        records_only_in_source(source, target, key_col)
        records_only_in_target(source, target, key_col)
    return status
