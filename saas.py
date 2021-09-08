import pandas as pd
import os
import shutil
cn = pd.read_excel('/home/de/jhoon/data_extraction/periperal/CN.xlsx')
jp = pd.read_excel('/home/de/jhoon/data_extraction/periperal/JP.xlsx')
cn_lst = list(cn['file_name'])
jp_lst = list(jp['file_name'])
full_lst = jp_lst+cn_lst
local_path = '/data/collection/SaaS_batch/full/'
count = 0
toward = '/data/collection/9315/'
for elem in full_lst:
    shutil.move(local_path+elem, toward + elem)