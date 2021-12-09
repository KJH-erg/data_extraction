from google.api_core.exceptions import exception_class_for_grpc_status
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
import re
import csv
import pathlib
import shutil

class cleaner:
    flavor = pd.read_excel('/home/de/jhoon/data_extraction/periperal/flavor.xlsx')
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
    def get_flavor(self,x):
        res = self.flavor[self.flavor['label_button'] == x]
        try:
            return res['label_name'].values[0],res['label_id'].values[0]
        except:
            print('error with '+x)
            return '',''
    def ThreadWorker(self, index, item, path, bucket):
        tmp = {}
        tmp['source_image'] = (item['file_name'])
        annot = {}
        annot['annotation'] = "CUBOID"
        try:
            
            f_points=[]
            b_points=[]
            for each in item['result'][0][0]['name_YFSTS7']:
                each['front'].pop('top')
                each['front'].pop('left')
                each['front'].pop('width')
                each['front'].pop('height')
                each['back'].pop('top')
                each['back'].pop('left')
                each['back'].pop('width')
                each['back'].pop('height')
                each.pop('warnings')
                each.pop('timestamp')
            
                for k,v in each['front']['coords'].items():
                    f_points.append(v)
                for k,v in each['back']['coords'].items():
                    b_points.append(v)
            f_points[3],f_points[2]=f_points[2],f_points[3]
            b_points[3],b_points[2]=b_points[2],b_points[3]
            annot['front'] = {'points':f_points}
            annot['back'] = {'points':b_points}
            tmp['cuboid_preset'] = json.dumps([annot])
        except:
            tmp['cuboid_preset'] = "[{\"annotation\": \"CUBOID\", \"front\": {\"points\": [{\"x\": 0, \"y\": 0}, {\"x\": 0, \"y\": 0}, {\"x\": 0, \"y\": 0}, {\"x\": 0, \"y\": 0}]}, \"back\": {\"points\": [{\"x\": 0, \"y\": 0}, {\"x\": 0, \"y\": 0}, {\"x\": 0, \"y\": 0}, {\"x\": 0, \"y\": 0}]}}]"
#         for k,v in each['front']['coords'].items():
#             points.append(v)
#         for k,v in each['back']['coords'].items():
#             points.append(v)
        
        # print('Thread:: Processing...[', index, ']')
        return tmp
        
        
            
    
            
    def setThread(self,result,path):
        index = 0
        futures = []
        count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            total = []
            bucket = self.storage_client.get_bucket('cw_platform')
            for item in tqdm(result):
                futures.append(executor.submit(
                        self.ThreadWorker, index = index, item = item,path = path, bucket=bucket))
                index += 1
            
            with open(path+'preset_data.json', 'w', encoding='utf-8') as json_file:
                for future in concurrent.futures.as_completed(futures):
                    elem = future.result()
                    json_file.write(json.dumps(elem)+'\n')
        
        

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