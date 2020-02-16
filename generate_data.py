

base_file = '/home/sysadmin/Projects/spark-jobs/hp7.txt'

new_file = '/home/sysadmin/Projects/spark-jobs/hp7_inflated.txt'

with open(file=base_file, mode='r') as base:
    base_lines = base.readlines()

with open(file=new_file, mode='a') as new:
    for _ in range(10):
        new.writelines(base_lines)
    new.close()



