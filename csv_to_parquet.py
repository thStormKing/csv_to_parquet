import pandas as pd
import os

def csv_2_par(v_path, v_name):
    print(os.path.basename(__file__))
    df = pd.read_csv(v_path+v_name+'.csv')
    df.to_parquet(v_path+v_name+'.parquet')

def par_2_csv(v_path, v_name, v_dir):
    # print(os.path.basename(__file__))
    df = pd.read_parquet(v_path+'\\'+v_dir+'\\'+v_name)
    if not(os.path.isdir('C:\\Workspace\\abc_export_data\\parquet\\par_csv\\'+v_dir)):
        os.makedirs('C:\\Workspace\\abc_export_data\\parquet\\par_csv\\'+v_dir)
        df.to_csv('C:\\Workspace\\abc_export_data\\parquet\\par_csv\\' + v_dir + '\\' + v_name + '.csv')
    else:
        df.to_csv('C:\\Workspace\\abc_export_data\\parquet\\par_csv\\' + v_dir + '\\' + v_name + '.csv')