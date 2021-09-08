import os
import pandas as pd
import shutil
df = pd.read_excel('/data/collection/SaaS_8_27/result/result.xlsx')
path = '/data/collection/SaaS_8_27/result/'
out_path = '/data/collection/SaaS_8_27/result2/'
lst = df['result']
print(len(lst))
with open ('/data/collection/no2.txt','a') as f:
    for item in lst:
        try:
            shutil.move(path+item, out_path+item)
        except:
            f.write(item+'\n')