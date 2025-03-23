import os
import datetime

report_dir='report'
dir=os.getcwd()
# print(dir,"*********************************************")
os.makedirs(report_dir,exist_ok=True)
timestamp=datetime.datetime.now().strftime("%d%m%Y%H%M%S")
# print(timestamp)
report_filename=os.path.join(report_dir,f"report_{timestamp}.txt")

def write_output(validation_type,status,details):
    with open(report_filename,'a') as report:
        report.write(f"validation_type:{validation_type}\nstatus:{status}\ndeatils:{details}\n\n")
