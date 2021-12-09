from model.path.PathUtil import PathUtil
from model.db.MysqlConf import MysqlConf
from model.request.RequestInfo import RequestInfo
import json
import pymysql
import os
from model.thread_response.DataLoad_New_Format import DataLoad
import zipfile
import csv
# from './tools' import *

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
    localDownloadDefaultPath = '/data/collection/'+projectCode+'/'
    os.system('mkdir {path}_content_data'.format(path = localDownloadDefaultPath))
    zip_file_path = localDownloadDefaultPath+'file.zip'
    zip_file = zipfile.ZipFile(zip_file_path)
    os.system('unzip {zip_file_name} -d {path}'.format(zip_file_name = zip_file_path, path=localDownloadDefaultPath+'_content_data'))
    # get_csv(localDownloadDefaultPath,localDownloadDefaultPath+'_content_data')
    print('gsutil -m cp -r {path} gs://cw_platform/{gid}/{pid}_content/'.format(path = localDownloadDefaultPath+'_content_data', gid=gid,pid = projectCode))
    os.system('gsutil -m cp -r {path} gs://cw_platform/{gid}/{pid}_content/'.format(path = localDownloadDefaultPath+'_content_data', gid=gid,pid = projectCode))