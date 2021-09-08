import pandas as pd
import os
from PIL import Image, ImageDraw,ImageFont
from PIL import ImagePath
from tqdm import tqdm
import re
from cleanse import tools
from PIL import ImageColor
from google.cloud import storage
from model.path.PathUtil import PathUtil
from model.google.storage.GcsStorageUtil import GcsStorageUtil
import concurrent.futures
import json

class cleaner:
    localDownloadDefaultPath = ''    
    def __init__(self,localDownloadDefaultPath,gcsUploadPrefixFileCode,jiraCode,projectID):
        self.PathUtil = PathUtil()
        self.localDownloadDefaultPath = localDownloadDefaultPath
        self.gcsUploadPrefixFileCode = gcsUploadPrefixFileCode
        self.jiraCode = jiraCode
        self.projectID = projectID
        self.GcsStorageUtil = GcsStorageUtil()

    def ThreadWorker(self, index, item):
        print('Thread:: Processing...[', index, '] : ')
        img = Image.open(item['img'])
        result = item['result']['data']
        w,h = img.size
        for each_bound in result:
            each_data = each_bound['data'][0]
            text = each_data['value']
            each_dot = each_bound['dots']
            tmp = []
            x= []
            y=[]
            for point in each_dot:
                tmp.append(point['x'])
                x.append(point['x'])
                tmp.append(point['y'])
                y.append(point['y'])
            fnt = ImageFont.truetype("/home/de/jhoon/data_extraction/periperal/godic.ttf", int(w/75))    
            img1 = ImageDraw.Draw(img)  
            img1.polygon(tmp, outline = (255,0,0,)) 
        rgb_im = img.convert('RGB')
        rgb_im.save(item['img'])
        rgb_im.close
        img.close()

    def setThread(self,result):

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)
        index = 0
        futures = []
        for item in tqdm(result):
            futures.append(executor.submit(
                    self.ThreadWorker, index = index, item = item)
                )
            index += 1

        
        # for future in concurrent.futures.as_completed(futures):
        #     key, value = future.result()
        #     final_dict[key] = value
        # return final_dict
        

        

    def preprocess(self,result):
        storage_client = storage.Client()
        bucket = storage_client.get_bucket('cw_platform')
        self.setThread(result)
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
    localDownloadDefaultPath = '/data/collection/'+jiraCode+'/'
    p = re.compile('(.*).jpg')
    file_list = []
    for file in os.listdir(localDownloadDefaultPath+'kb_labeled_data_2231_20191121/'):
        try:
            file = re.search(p,file).group(1)
            file_list.append(file)
        except:
            pass
    result =[]
    for item in file_list:
        tmp = {}
        with open(localDownloadDefaultPath+'kb_labeled_data_2231_20191121/'+item+'.json', "r",encoding='UTF-8-sig') as f:
            d = json.loads(f.read())
        tmp['img'] = localDownloadDefaultPath+'kb_labeled_data_2231_20191121/'+item+ '.jpg'
        tmp['result'] = d
        result.append(tmp)

    start_clean = cleaner(localDownloadDefaultPath,gcsUploadPrefixFileCode,jiraCode,projectId)
    start_clean.preprocess(result)


    # df.to_excel(final_dir)
