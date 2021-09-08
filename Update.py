from model.path.PathUtil import PathUtil
from model.google.storage.GcsStorageUtil import GcsStorageUtil
from google.cloud import storage
from tqdm import tqdm
from pydub import AudioSegment
import concurrent.futures
import os
import logging
import re

Total = {}
gcsDownloadBucketCode = 'cw_platform'
localpath = '/data/collection/data1732/'
def chg_streo(dest):
    sound = AudioSegment.from_wav(dest)
    sound = sound.set_channels(1)
    sound.export(dest, format="wav")

def setThread(result):
    index = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        bucket = storage_client.get_bucket('cw_platform')
        
        for key,val in tqdm(result.items()):
            executor.submit(
                    ThreadWorker, index = index, key = key, val = val, bucket=bucket)
            index += 1
def ThreadWorker(index,key, val, bucket):
    print('Thread:: Processing...[', index, ']')
    FilePath = PathUtil.getGcsResultFilePath(val['company_id'], val['project_id'])
    path = PathUtil.getGCSResultFileName(key,val['project_id'])
    json = GcsStorageUtil.json_loads_with_prefix_openfile(bucket, gcsDownloadBucketCode, FilePath, path)
    seq = PathUtil.getSeqnum(json)									   
    src = FilePath + path+'_'+str(seq)
    dest = localpath+path+'_'+str(seq)+".wav"
    GcsStorageUtil.download_blob_openfile(bucket,gcsDownloadBucketCode, src, dest)  
    chg_streo(dest)
    GcsStorageUtil.upload_blob(gcsDownloadBucketCode, dest, src, user_email=None)
    logging.info(str(key))
    os.remove(dest)





logging_path = '/home/de/jhoon/log/data1732/'

try:
    os.mkdir(logging_path)
    os.mkdir(localpath)
except:
    pass
logging.basicConfig(filename=logging_path+'fin3.log',format='%(asctime)s %(message)s', level=logging.INFO)
PathUtil = PathUtil()
GcsStorageUtil = GcsStorageUtil()
storage_client = storage.Client()
with open ('dids_stereo_sorted.txt','r') as f:
    full_list = f.read()
    full_list = full_list.split('\n')
    for elem in tqdm(full_list):
        inner = {}
        splited = elem.split(' ')
        inner['company_id'] = splited[1]
        inner['project_id'] = splited[2]
        Total[str(splited[3])] = inner
with open('/home/de/jhoon/data_extraction/periperal/fin.log','r') as f:
    finished = f.read()
lst = finished.split('\n')
uploaded = []
p = re.compile(' ([0-9]{8})')
for i in lst:
    try:
        uploaded.append((re.search(p,i).group(1)))
    except:
        pass
set1 = set(uploaded)

for i in uploaded:
    del Total[i]

with open('/home/de/jhoon/data_extraction/periperal/fin2.log','r') as f:
    finished = f.read()
lst = finished.split('\n')
uploaded = []
p = re.compile(' ([0-9]{8})')
for i in lst:
    try:
        uploaded.append((re.search(p,i).group(1)))
    except:
        pass
set1 = set(uploaded)

for i in uploaded:
    del Total[i]
with open('/home/de/jhoon/data_extraction/periperal/fin3.log','r') as f:
    finished = f.read()
lst = finished.split('\n')
uploaded = []
p = re.compile(' ([0-9]{8})')
for i in lst:
    try:
        uploaded.append((re.search(p,i).group(1)))
    except:
        pass
set1 = set(uploaded)

for i in uploaded:
    del Total[i]
print(len(Total))
# setThread(Total)
    

    