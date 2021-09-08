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

class cleaner:
    localDownloadDefaultPath = ''    
    def __init__(self,localDownloadDefaultPath,gcsUploadPrefixFileCode,jiraCode,projectID,groupID):
        self.PathUtil = PathUtil()
        self.localDownloadDefaultPath = localDownloadDefaultPath
        self.gcsUploadPrefixFileCode = gcsUploadPrefixFileCode
        self.jiraCode = jiraCode
        self.projectID = projectID
        self.GcsStorageUtil = GcsStorageUtil()
        self.groupID = groupID
        self.storage_client = storage.Client()


    def ThreadWorker(self, index, item, path, bucket):
        print('Thread:: Processing...[', index, ']')
        # path = path+'result/'
        data_idx = item['data_idx']
        total = item['data_set']
        file_name = (item['source_data']['file_name'])
        print(file_name)
        json_name = tools.name_without_extension((item['source_data']['file_name']))
        print(json_name)
        tmp = {}
        
        tmp['data_idx'] = str(data_idx)
        tmp['source_data'] = file_name
        
        tmp['value'] = total
        
        tmp['check_end_date'] = str(item['check_edate'])
        tmp['check_id'] = item['CHECK_nm']
        
        tmp['work_end_date'] = str(item['work_edate'])
        tmp['work_id'] = item['WORK_nm']
        
        json_file = open(path+json_name+'.json', 'w', encoding='utf-8')
        json_file.write(json.dumps(tmp , ensure_ascii=False,indent = 4))
        json_file.close()
        
    def setThread(self,result,path):
        index =0
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            bucket = self.storage_client.get_bucket('cw_platform')
            for item in tqdm(result):
                executor.submit(
                        self.ThreadWorker, index = index, item = item,path = path, bucket=bucket)
                index += 1



    def preprocess(self,result):
        storage_client = storage.Client()
        self.setThread(result,self.localDownloadDefaultPath)
        
        # for idx,data in tqdm(df.iterrows()):
        #     try:
        #         os.mkdir('/data/collection/prj2885/'+self.projectID+'/')
        #     except:
        #         pass
        #     try:
        #         os.mkdir('/data/collection/prj2885/'+self.projectID+'/'+str(data['member_nm'])+'/')
        #     except:
        #         pass
        #     num = data['name_S3GZS3']
        #     gcsResultFilePath = self.PathUtil.getGcsResultFilePath('500', self.projectID)
        #     gcsResultFileName = self.PathUtil.getGCSResultFileName(str(data['data_idx']), self.projectID)
        #     self.GcsStorageUtil.download_blob_openfile(bucket, 'cw_platform', gcsResultFilePath+gcsResultFileName+'_'+str(num[0]["seqNum"]), '/data/collection/prj2885/'+self.projectID+'/'+data['member_nm']+'/'+ gcsResultFileName+'.wav')
            
                    
        # df.to_excel(self.localDownloadDefaultPath+'/result.xlsx')




        
if __name__ == '__main__':
    projectCode = 'Undefined'
    projectId = 'Undefined'
    jiraCode = 'DATA1716'
    gcsUploadPrefixFileCode = jiraCode + '_' + projectId + "_"
    localDownloadDefaultPath = '/data/collection/'+projectCode+'/' + PathUtil.getTodayYYYYMMDD() + '/'
    os.listdir(localDownloadDefaultPath)
    #start_clean = cleaner(localDownloadDefaultPath,gcsUploadPrefixFileCode,jiraCode,projectId)



    # df.to_excel(final_dir)