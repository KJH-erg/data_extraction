from model.path.PathUtil import PathUtil
from model.db.MysqlConf import MysqlConf
from model.request.RequestInfo import RequestInfo
import json
import pymysql
import os
from model.thread_response.DataLoad_New_Format import DataLoad
import zipfile
import csv

def unzip(source_file, dest_path):
    with zipfile.ZipFile(source_file, 'r') as zf:
        zipInfo = zf.infolist()
        for member in zipInfo:
            try:
                try:
                    member.filename = member.filename.encode('cp437').decode('euc-kr', 'ignore')
                except:
                    print(member.filename.encode('utf-8').decode('utf-8', 'ignore'))
                    member.filename = member.filename.encode('utf-8').decode('utf-8', 'ignore')
                zf.extract(member, dest_path)
                
            except:
                print(source_file)
                raise Exception('what?!')
    
if __name__ == '__main__':
    PathUtil = PathUtil()
    gid = '538'
    projectCode = '11187'
    upload_list =[]
    
    uploaded_path =  'gs://cw_pm_upload_files/Aihub_1차_53번/일반차량\ 바운딩12개\ 데이터/Total_4500_1208.zip'
    localDownloadDefaultPath = '/data/collection/'+projectCode+'/'
    os.system('gsutil cp {gsutil_Uri} {download_target_path}'.format(gsutil_Uri=uploaded_path,download_target_path=localDownloadDefaultPath+'file.zip'))
    os.system('mkdir {path}_content_data'.format(path = localDownloadDefaultPath))
    try:
        os.mkdir(localDownloadDefaultPath)
    except:
        print('directory exists')
        
    zip_file_path = localDownloadDefaultPath+'file.zip'
    zip_file = zipfile.ZipFile(zip_file_path)
    file_list = []
    for file_info in zip_file.infolist():
        if file_info.filename[-1]!='/':
            file_list.append(file_info.filename)
            
    print('Total count of uploading files is')
    print(len(file_list))
    
    with open(localDownloadDefaultPath+'file_list.csv', 'a',encoding ='utf-8-sig') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(["file_name"])
        for file_name in file_list:
            csvwriter.writerow([file_name])
            
    os.system('unzip {zip_file_name} -d {path}'.format(zip_file_name = zip_file_path, path=localDownloadDefaultPath+'_content_data'))
    
    for item in upload_list :
        inner = str(item)
        print('gsutil -m cp -r {path} gs://cw_platform/{gid}/{pid}_content/'.format(path = localDownloadDefaultPath+'_content_data', gid=gid,pid = inner))
        # os.system('gsutil -m cp -r {path} gs://cw_platform/{gid}/{pid}_content/'.format(path = localDownloadDefaultPath+'_content_data', gid=gid,pid = inner))