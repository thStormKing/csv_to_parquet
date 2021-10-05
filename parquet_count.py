import pyarrow.parquet as pq
import os

def cnt_par(s_file, v_name):
    parquet_file = pq.ParquetFile(v_name)
    # print('main prog name: ', s_file)
    #parquet_file.metadata
    print('file record count: ',parquet_file.metadata.num_rows)
    return parquet_file.metadata.num_rows
