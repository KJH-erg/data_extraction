
from model.path.PathUtil import PathUtil
from model.db.MysqlConf import MysqlConf
from model.request.RequestInfo import RequestInfo
from unicodedata import normalize
import json
import pymysql
import os
from model.thread_response.DataLoad_New_Format import DataLoad
import zipfile
import csv
from tqdm import tqdm

if __name__ == '__main__':
    PathUtil = PathUtil()
    gid = '436'
    projectCode = '9665'
    
    uploaded_paths =['gs://cw_pm_upload_files/Aihub_1차_53번/일반차량\ 바운딩2개\ 데이터/Parts_0906.zip']
    localDownloadDefaultPath = '/data/collection/'+projectCode+'/'
    
    try:
        os.mkdir(localDownloadDefaultPath)
        os.system('mkdir {path}_content_data'.format(path = localDownloadDefaultPath))
    except:
        print('directory exists')

    
    # file_list = []
    # for uploaded_path in uploaded_paths:
    #     os.system('gsutil cp {gsutil_Uri} {download_target_path}'.format(gsutil_Uri=uploaded_path,download_target_path=localDownloadDefaultPath+'file.zip') )
    #     zip_file_path = localDownloadDefaultPath+'file.zip'
    #     zip_file = zipfile.ZipFile(zip_file_path)
    #     for file_info in zip_file.infolist():
    #         if file_info.filename[-1]!='/':
    #             file_list.append(file_info.filename)
    #     os.system('unzip {zip_file_name} -d {path}'.format(zip_file_name = zip_file_path, path=localDownloadDefaultPath+'_content_data'))
   
  
    # # # mysql_config = json.loads(os.environ["MySQL_CONF"])
    # # # env_mysql_config = mysql_config['prd']
    # # # dbHost = env_mysql_config['host']
    # # # dbName = env_mysql_config['dbName']
    # # # dbUser = env_mysql_config['user']
    # # # dbPwd = env_mysql_config['password']
    # # # db = MysqlConf(dbHost, dbUser, dbPwd, dbName)
    # # # dataStatusCode = ['WORK_END']
    # # # requestInfo = RequestInfo(projectCode, gid, projectCode, dataStatusCode)
    # # # responseData = DataLoad('cw_platform', 'cw-downloads')
    # # # responseData.getdbData(db, requestInfo)
    # # # print(responseData.resultDict)
    

    
    



    # with open(localDownloadDefaultPath+'file_list.csv', 'w',encoding ='utf-8-sig') as csvfile:
    #     csvwriter = csv.writer(csvfile)
    #     csvwriter.writerow(["file_name"])
    #     for file_name in file_list:
    #         csvwriter.writerow([os.path.dirname(file_name),file_name])

    os.system('gsutil -m cp -r {path} gs://cw_platform/{gid}/{pid}_content/'.format(path = localDownloadDefaultPath+'_content_data/', gid=gid,pid = projectCode))