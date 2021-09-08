import pandas as pd
import os
from PIL import Image, ImageDraw
from PIL import ImagePath
from tqdm import tqdm
import re
from cleanse import tools
from PIL import ImageColor
from google.cloud import storage
from model.path.PathUtil import PathUtil
from model.google.storage.GcsStorageUtil import GcsStorageUtil
import concurrent.futures
import datetime
import librosa
import json
import csv
import shutil
import zipfile

class cleaner:
    localDownloadDefaultPath = ''    
    def __init__(self,localDownloadDefaultPath):
        self.PathUtil = PathUtil()
        self.local = localDownloadDefaultPath

    def ThreadWorker(self, img, item, path):
        im = Image.open(img)
        with open(path+str(item['id'])+'.txt','w',encoding = 'utf-8') as f:
            f.write(item['data'][0]['value'])
        coord = []
        for dot in item['dots']:
            if(dot['idx'] == 0):
                coord.append(dot['x'])
                coord.append(dot['y']) 
            if(dot['idx'] == 2):
                coord.append(dot['x'])
                coord.append(dot['y'])
        im2 = im.crop((coord))
        im2.save(path+str(item['id'])+'.jpg',"JPEG")
        print(path+str(item['id'])+'.jpg')
    def setThread(self,jpg, js,path):
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            with open(js,encoding='utf-8-sig') as json_file:
                json_data = json.load(json_file)
                data = json_data['data']

                for item in data:
                    executor.submit(self.ThreadWorker, img =jpg, item = item,path = path)


    def preprocess(self,result):
        for file in tqdm(result):
            print(file)
            try:
                os.mkdir(local+tools.name_without_extension(file))
            except:
                pass
            self.setThread(local+tools.name_without_extension(file)+'.jpg',local+tools.name_without_extension(file)+'.json',local+tools.name_without_extension(file)+'/')
        


if __name__ == '__main__':
    projectCode = 'DATA1731'
    
    uploaded_path =  'gs://cw_pm_upload_files/kb/labeled_data_2231_20191121.zip'
    localDownloadDefaultPath = '/data/collection/'+projectCode+'/'
    
    try:
        os.mkdir(localDownloadDefaultPath)
    except:
        print('directory exists')
    
    # os.system('gsutil cp {gsutil_Uri} {download_target_path}'.format(gsutil_Uri=uploaded_path,download_target_path=localDownloadDefaultPath+'file.zip') )
    # zip_file_path = localDownloadDefaultPath+'file.zip'
    # zip_file = zipfile.ZipFile(zip_file_path)
    # os.system('unzip {zip_file_name} -d {path}'.format(zip_file_name = zip_file_path, path=localDownloadDefaultPath))
    local = '/data/collection/'+projectCode+'/'
    file_list = []
    for file in os.listdir(local):
        if file.endswith(".json"):
            file_list.append(file)
    start_clean = cleaner(local)
    start_clean.preprocess(file_list)


    df.to_excel(final_dir)