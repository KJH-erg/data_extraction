import sys
import os
import json
import pymysql
import datetime
import time
import hashlib
import requests
import csv
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
import shutil
from cleanse.clean import cleaner
from GCSupload import uploader
import logging
from cleanse import df_clean
from model.sql import mysql_query

from model.request.RequestInfo import RequestInfo
from model.db.MysqlConf import MysqlConf
from model.path.PathUtil import PathUtil
from model.google.storage.GcsStorageUtil import GcsStorageUtil
from model.thread_response.DataLoad_New_Format import DataLoad
from model.thread_response.DataExtract_New_Format import DataExtract
from model.thread_response.DataHeaderValues import HeaderValues
from model.external.extract import data


# Module Load
GcsStorageUtil = GcsStorageUtil()
PathUtil = PathUtil()

'''
동작시간 설정
''' 
start_time = time.time()
'''
고정변수
'''
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/de/yj/crowdworks-platform-2ca586b71168.json"
mysql_config = json.loads(os.environ["MySQL_CONF"])
env_mysql_config = mysql_config['prd']
dbHost = env_mysql_config['host']
dbName = env_mysql_config['dbName']
dbUser = env_mysql_config['user']
dbPwd = env_mysql_config['password']
'''
환경변수들
'''
mode = str(sys.argv[1])			# 'file' : 파일까지만 만든다 (=GCS에 업로드 하지 않는다)  /  'upload' : 업로드 한다
dataOffsetStartNum = '0'	# 0 # 데이터 건수를 분리하여 처리하도록 개수 정의
dataLimit = '1000000'			# 300 # 데이터 건수를 분리하여 처리하도록 개수 정의
groupId = '568'			# 294
projectId = 'Samsung'	# 9999
jiraCode = 'PRJ3511'	
fileFormat = 'csv'		# 결과파일(매칭) 포멧 - json, csv, excel
grand_mail = 'eastblack2@crowdworks.kr'# 권한 부여할 메일
if '/' in grand_mail:
	grand_mail = grand_mail.split('/')
'''
extract data from external source
'''
# Extract = data('/home/de/jhoon/data_extraction/periperal/lst.xlsx')
# imported = Extract.get_list()
'''
query 변수
'''
dataStatusCode =[]
# dataStatusCode = ["CHECK_END", "CHECK_ING", "CHECK_REJECT", "CHECK_REWORK", "WORK_END", "ALL_FINISHED",'WORK_ING'] # list
#dataStatusCode = ['ALL_FINISHED',"CHECK_REWORK"] # "ALL_FINISHED", "WORK_END"
#dataStatusCode = ["CHECK_END", "CHECK_ING", "CHECK_REJECT", "CHECK_REWORK", "WORK_END",'WORK_ING']
'''
작업 가능 데이터 여부
'''
#queryfilter = ''
queryfilter = 'pjd.problem_yn="0" ' # 작업가능 데이터만 추출
# queryfilter = ' AND pjd.problem_yn="1" ' # 작업 불가능 데이터만 추출
'''
데이터 아이디별 추출시
'''
data_idx = []
# data_idx =imported 
#data_idx = [55835104,55835121]
'''
기간 선택
'''
# queryfilter = ''
# queryfilter += 'AND pjd.check_edate BETWEEN "'+PathUtil.getPreviousWeekYYYYMMDD()+' 00:00:00" AND "'+PathUtil.getYesterdayYYYYMMDD()+' 23:59:59" '
############# TODO : check_edate 인지 work_edate 인지 잘 보기 (all_fiinsied만 추출 시 : check_edate가 맞음)
############# TODO : check_edate 인지 work_edate 인지 잘 보기 (모든데이터 추출 시 : work_edate가 맞음)
# ALL_FINISHED 작업 시
# queryfilter += ' AND pjd.check_edate BETWEEN "2021-10-06 00:00:00" AND "2021-11-07 23:59:59" '
# queryfilter += ' AND pjd.check_edate <= "2021.11.29 23:59:59" '
# WORK_END 작업 시
# queryfilter += 'AND pjd.work_edate BETWEEN "2021-09-23 00:00:00" AND "2021-11-01 23:59:59"'
# queryfilter += ' AND pjd.work_edate >= "2021-11-07 00:00:00" '
#queryfilter += 'AND pjd.work_sdate BETWEEN "2021-07-09 11:01:00" AND "2021-07-15 00:00:00"'
'''
sort
'''
orderBy = 'ORDER BY 3 ASC'	 

# 이미지 사용 시
sourceFileJsonKeyName = 'file_name'		# 소스파일다운로드 할 Json 키명(file_name, image_url, upload_files, filenames)


#*************** //수동 설정 변수 ***************
###### 프로그램 설정 변수  바뀔필요 적음
#zipPassword = 'cw#data!'+projectId

localDownloadDefaultPath = '/data/collection/'+jiraCode+'/' + PathUtil.getTodayYYYYMMDD() + '/'
logging_path = '/home/de/jhoon/log/'+jiraCode+'/'
PathUtil.mkdir_p(logging_path)
logging.basicConfig(filename=logging_path+'{}.log'.format(PathUtil.getTodayYYYYMMDD()),format='%(asctime)s %(message)s', level=logging.INFO)
PathUtil.mkdir_p(localDownloadDefaultPath)	# 다운로드 파일 생성
gcsDownloadBucketCode = 'cw_platform'
# gcsUploadBucketCode = 'cw_pm_upload_files' # 고객사 전용(임시) : 'cw_pm_upload_files', 내부 : 'cw-downloads'
gcsUploadBucketCode = 'cw-downloads'
gcsUploadPrefixFileCode= projectId + "_"
gcsUploadDefaultPath = 'pm_download/' + jiraCode + '/'
historyDataIdFilePath = '/home/de/jhoon/filter/filter_'+projectId+'.csv'
###### //프로그램 설정 변수 ######




# # 과거 업로드 된 데이터목록 CSV 파일 읽기
# if not os.path.exists(historyDataIdFilePath):
# 	open(historyDataIdFilePath, 'w').close()
# csvDataFile = open(historyDataIdFilePath, 'r', encoding='utf-8')
# csvDataList = csv.reader(csvDataFile)
# csvDataDict = {}
# for line in csvDataList:
# 	if line == None:
# 		continue
# 	csvDataDict[line[0]] = line[1]
# 	print(line)
# csvDataFile.close()

# 응답 포멧 정의
responseData = DataLoad(gcsDownloadBucketCode, gcsUploadBucketCode)
extract = DataExtract(gcsDownloadBucketCode, gcsUploadBucketCode)
# header value 추가

headerValues = HeaderValues(gcsDownloadBucketCode, gcsUploadBucketCode)

# 기본 요청 정보 설정


# MySQL 설정
db = MysqlConf(dbHost, dbUser, dbPwd, dbName)

# DB 정보 가져오기
sql = mysql_query.setDBdata_nick_phone_query

prj_list = [10288,10289,10290]
full_list = []
total = []
projectId = in_to_string = ','.join(map(lambda x: '"%s"' % x, prj_list))
# 
requestInfo = RequestInfo(groupId, projectId, dataStatusCode)
responseData.setDbDatas(db, requestInfo, dataOffsetStartNum, dataLimit, queryfilter, orderBy, data_idx,sql)
# 최종 데이터ID 목록 만들기(쿼리값 모두 담음)
finalDataDict = {}
logging.info(responseData.sql)
logging.info('total length is '+str(len(responseData.sql)))
finalDataData = responseData.resultData
# 추출할 것이 없으면 프로그램 종료

'''
get label info
'''
#label_dict = headerValues.setLabels(requestInfo, responseData.resultData[0],projectId)

'''
source_json
'''
source_error = []
source_error = responseData.setGcsSourceJsonThread()

'''
result_json
'''
result_error = []
result_error = responseData.setGcsResultJsonThread()


# ########### Header #####gl#######
#header = headerValues.setLabels(requestInfo,responseData.resultData,projectId)
sourceFileJsonKeyName = 'file_name'
start_clean = cleaner(localDownloadDefaultPath,gcsUploadPrefixFileCode,jiraCode,projectId,groupId)

# ########### Values ##############
#파일 다운로드용

# try:
# 	responseData.setGcsContentFileThread(sourceFileJsonKeyName, localDownloadDefaultPath)
# except:
# 	pass
values = responseData.resultData
# start_clean.preprocess(values)
localDefaultPath = '/home/de/jhoon/'+jiraCode+'/'

df = pd.DataFrame.from_dict(responseData.resultData)
pickle_path = "/home/de/jhoon/dump/%s" %jiraCode
df.to_pickle(pickle_path)

# print('sample pickle data path is '+pickle_path)

end_time = time.time() - start_time
times = str(datetime.timedelta(seconds=end_time)).split(".")[0]
if 'AND pjd.problem_yn="0"' in queryfilter:
	print("문제 없음")
	file_key = PathUtil.getTodayYYYYMMDD()+'_problem_n'
elif 'AND pjd.problem_yn="1"' in queryfilter:
	print("문제 있음")
	file_key = PathUtil.getTodayYYYYMMDD()+'_problem_y'
elif 'AND pjd.problem_yn=' not in queryfilter:
	file_key = PathUtil.getTodayYYYYMMDD()
else:
	print('errorr')
	raise 1

payloadStr = "프로젝트 번호 : "+requestInfo.projectId + "\n"

payloadStr += "고객사 번호 : "+groupId + "\n"
total = len(responseData.resultData)
payloadStr += "데이터(작업) 건수 : "+ str(total) + "\n"
payloadStr += "GcsResultJsonFile Count : " + str(total-len(result_error)) + "\n"
payloadStr += "GcsResultJsonFile Fail Count : " + str(len(result_error)) + "\n"
if len(result_error) > 0:
	payloadStr += "GcsResultJsonFile Fail Data : " + str(result_error) + "\n"

payloadStr += "GcsSourceJsonFile Count : " + str(total-len(source_error)) + "\n"
payloadStr += "GcsSourceJsonFile Fail Count : " + str(len(source_error)) + "\n"
if len(source_error) > 0:
	payloadStr += "GcsSourceJsonFile Fail Data : " + str(source_error) + "\n"

print(payloadStr)
# payloadStr += "GcsResultValueFail Count : " + str(headerValues.gcsResultValueFailCount) + "\n"
# if headerValues.gcsResultValueFailCount > 0:
# 	payloadStr += "GcsSourceJsonFile Fail Data : " + ", ".join(list(headerValues.gcsResultValueNotFind.keys())) + "\n"
start_upload = uploader(localDownloadDefaultPath, file_key, gcsUploadDefaultPath, gcsUploadPrefixFileCode,grand_mail,historyDataIdFilePath,payloadStr)
if (mode == "upload"):
	payloadStr = start_upload.upload()
logging.info(payloadStr)
csvDataFile = open(historyDataIdFilePath, 'a', encoding='utf-8')
wr = csv.writer(csvDataFile)
for key,val in finalDataDict.items():
	wr.writerow([key, str(PathUtil.getTodayYYYYMMDD())])
csvDataFile.close()

end_time = time.time() - start_time
times = str(datetime.timedelta(seconds=end_time)).split(".")[0]
payloadStr += "소요시간 : %s" % times
print(payloadStr)
print("Success!!")