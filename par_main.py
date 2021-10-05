import time as t
import os

# import csv_to_parquet
import parquet_count

# Execution start time
start_time = t.time()

print(os.path.basename(__file__))
# print('os path1: ',os.path)
# Path which contains .parquet files or directories which contain
# .parquet files
v_path = 'C:\\Workspace\\abc_export_data\\parquet\\pems_sorted'
# v_path = 'C:\\Workspace\\abc_export_data\\parquet'
# print('os current directory: ',os.getcwd())

f_cnt = 0
rec_cnt = 0

for n in os.listdir(v_path):
    # print ('list dir: ', v_path+'\\'+n)
    for root, directory, file in os.walk(v_path + '\\' + n):
        # print('root: ',root)
        # print('dir: ', directory)
        # print('file: ', file)
        for fl in file:
            if fl.endswith('.parquet'):
                # print ('parquet', v_path+'\\'+n+'\\'+fl)
                rec_cnt = rec_cnt + \
                          parquet_count.cnt_par(os.path.basename(__file__), v_path + '\\' + n + '\\' + fl)
                # csv_to_parquet.par_2_csv(v_path + '\\', fl, n)
                f_cnt = f_cnt + 1
                # print(f_cnt)


print('Total Execution Time in seconds: ', t.time() - start_time)
print('Total Files read: ', f_cnt)
print('Total Records Count: ', rec_cnt)
