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

from model.request.RequestInfo import RequestInfo
from model.db.MysqlConf import MysqlConf
from model.path.PathUtil import PathUtil
from model.google.storage.GcsStorageUtil import GcsStorageUtil
from model.thread_response.DataLoad_New_Format import DataLoad
from model.thread_response.DataExtract_New_Format import DataExtract
from model.thread_response.DataHeaderValues import HeaderValues
from cleanse import df_clean

flag = str(sys.argv[1])
###### 동작시간 설정 ######
start_time = time.time()
###### 고정 설정 변수 ######
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/de/yj/crowdworks-platform-2ca586b71168.json"
mysql_config = json.loads(os.environ["MySQL_CONF"])
env_mysql_config = mysql_config['prd']
dbHost = env_mysql_config['host']
dbName = env_mysql_config['dbName']
dbUser = env_mysql_config['user']
dbPwd = env_mysql_config['password']
###### //고정 환경 변수 ######
# Module Load
GcsStorageUtil = GcsStorageUtil()
PathUtil = PathUtil()

mode = 'upload'					# 'file' : 파일까지만 만든다 (=GCS에 업로드 하지 않는다)  /  'upload' : 업로드 한다
dataOffsetStartNum = '0'	# 0 # 데이터 건수를 분리하여 처리하도록 개수 정의
dataLimit = '100000'			# 300 # 데이터 건수를 분리하여 처리하도록 개수 정의
groupId = '450'			# 294
projectId = '9398'	# 9999
projectCode = 'TOBEWITHUS'		# hyundai-autoever
jiraCode = 'prj2995'	
fileFormat = 'csv'		# 결과파일(매칭) 포멧 - json, csv, excel
grand_mail = 'jeyfree@crowdworks.kr'# 권한 부여할 메일
if '/' in grand_mail:
	grand_mail = grand_mail.split('/')

###### //파라미터 변수 ######
#*************** TODO: 수동 설정 변수 ***************
#dataStatusCode = ["CHECK_END", "CHECK_ING", "CHECK_REJECT", "CHECK_REWORK", "WORK_END", "ALL_FINISHED",'WORK_ING'] # list
dataStatusCode = ['WORK_END'] # "ALL_FINISHED", "WORK_END"
#dataStatusCode = ["CHECK_END", "CHECK_ING", "CHECK_REJECT", "CHECK_REWORK", "WORK_END",'WORK_ING']

# 작업가능 데이터 추출 관련 쿼리 수정
#queryfilter = ''
queryfilter = ' AND pjd.problem_yn="0" ' # 작업가능 데이터만 추출
#queryfilter = ' AND pjd.problem_yn="1" ' # 작업 불가능 데이터만 추출


# 기간 선택
#queryfilter = ''
# queryfilter += 'AND pjd.check_edate BETWEEN "'+PathUtil.getPreviousWeekYYYYMMDD()+' 00:00:00" AND "'+PathUtil.getYesterdayYYYYMMDD()+' 23:59:59" '
############# TODO : check_edate 인지 work_edate 인지 잘 보기 (all_fiinsied만 추출 시 : check_edate가 맞음)
############# TODO : check_edate 인지 work_edate 인지 잘 보기 (모든데이터 추출 시 : work_edate가 맞음)
# ALL_FINISHED 작업 시
#queryfilter += ' AND pjd.check_edate BETWEEN "2021-08-13 00:00:00" AND "2021-09-01 23:59:59" '
#queryfilter += ' AND pjd.check_edate <= "2021.07.04 59:59:59" '
# WORK_END 작업 시
#queryfilter += 'AND pjd.work_edate BETWEEN "2021-07-26 00:00:00" AND "2021-08-05 23:59:59"'
# queryfilter += ' AND pjd.work_edate <= "2021-03-05 12:00:00" '
#queryfilter += 'AND pjd.work_sdate BETWEEN "2021-07-09 11:01:00" AND "2021-07-15 00:00:00"'

# 이미지 사용 시
sourceFileJsonKeyName = 'file_name'		# 소스파일다운로드 할 Json 키명(file_name, image_url, upload_files, filenames)


#*************** //수동 설정 변수 ***************
###### 프로그램 설정 변수  바뀔필요 적음
zipPassword = 'cw#data!'+projectId
orderBy = 'ORDER BY 3 ASC'	 # data_id
localDownloadDefaultPath = '/data/collection/'+projectCode+'/' + PathUtil.getTodayYYYYMMDD() + '/'
logging_path = '/home/de/jhoon/log/'+jiraCode+'/'
PathUtil.mkdir_p(logging_path)
logging.basicConfig(filename=logging_path+'{}.log'.format(PathUtil.getTodayYYYYMMDD()),format='%(asctime)s %(message)s', level=logging.INFO)
PathUtil.mkdir_p(localDownloadDefaultPath)	# 다운로드 파일 생성
gcsDownloadBucketCode = 'cw_platform'
# gcsUploadBucketCode = 'cw_pm_upload_files' # 고객사 전용(임시) : 'cw_pm_upload_files', 내부 : 'cw-downloads'
gcsUploadBucketCode = 'cw-downloads'
gcsUploadPrefixFileCode= jiraCode + '_' + projectId + "_"
gcsUploadDefaultPath = 'pm_download/' + projectCode + '/'
historyDataIdFilePath = '/home/de/jhoon/filter/filter_'+projectId+'.csv'
###### //프로그램 설정 변수 ######


# 과거 업로드 된 데이터목록 CSV 파일 읽기
if not os.path.exists(historyDataIdFilePath):
	open(historyDataIdFilePath, 'w').close()
csvDataFile = open(historyDataIdFilePath, 'r', encoding='utf-8')
csvDataList = csv.reader(csvDataFile)
csvDataDict = {}
for line in csvDataList:
	if line == None:
		continue
	csvDataDict[line[0]] = line[1]
	print(line)
csvDataFile.close()

# 응답 포멧 정의
responseData = DataLoad(gcsDownloadBucketCode, gcsUploadBucketCode)
extract = DataExtract(gcsDownloadBucketCode, gcsUploadBucketCode)
# header value 추가
headerValues = HeaderValues(gcsDownloadBucketCode, gcsUploadBucketCode)

# 기본 요청 정보 설정
requestInfo = RequestInfo(projectCode, groupId, projectId, dataStatusCode)

# MySQL 설정
db = MysqlConf(dbHost, dbUser, dbPwd, dbName)

# DB 정보 가져오기
responseData.setDbDatas(db, requestInfo, dataOffsetStartNum, dataLimit, queryfilter, orderBy)
# 최종 데이터ID 목록 만들기(쿼리값 모두 담음)
finalDataDict = {}

for key, val in responseData.resultDict.items():
	if key in csvDataDict:
		print('history data_id pass : ' + key)
	else:
		finalDataDict[key] = val
logging.info(mode)
logging.info(responseData.sql)
responseData.resultDict = finalDataDict
print("Filter responseData.resultDict : " + str(len(responseData.resultDict)))

# 추출할 것이 없으면 프로그램 종료
if len(responseData.resultDict) == 0:
	print('Data Length : %s !!' % str(len(responseData.resultDict)))
	sys.exit(0)
	

# Label 정보 가져오기
# responseData.setLabels(requestInfo)


# ResultJson 결과 정보 가져오기
responseData.setGcsSourceJsonThread(requestInfo)
responseData.setuncleanGcsResultJsonThread(requestInfo)
# ########### Header #####gl#######
values = responseData.resultDict
start_clean = cleaner(localDownloadDefaultPath,gcsUploadPrefixFileCode,jiraCode,projectId,groupId)
#파일 다운로드용

# try:
# 	responseData.setGcsContentFileThread(requestInfo, sourceFileJsonKeyName, localDownloadDefaultPath)
# except:
# 	pass
localDefaultPath = '/home/de/jhoon/'+jiraCode+'/'

df = pd.DataFrame.from_dict(values,orient='index')
pickle_path = "/home/de/jhoon/dump/%s" %jiraCode
df.to_pickle(pickle_path)
print('sample pickle data path is '+pickle_path)

# df['result_data'] = df['result_data'].apply(lambda x:x[1:][0][0:])
# result = df_clean.clean(df)


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
payloadStr = "프로젝트 코드 : "+projectCode + "\n"
payloadStr += "프로젝트 번호 : "+requestInfo.projectId + "\n"
payloadStr += "고객사 번호 : "+groupId + "\n"
payloadStr += "데이터(작업) 건수 : "+ str(len(list(responseData.resultDict.values()))) + "\n"
payloadStr += "GcsResultJsonFile Count : " + str(responseData.gcsResultCount) + "\n"
payloadStr += "GcsResultJsonFile Fail Count : " + str(responseData.gcsResultFailCount) + "\n"
if responseData.gcsResultFailCount > 0:
	payloadStr += "GcsResultJsonFile Fail Data : " + ", ".join(list(responseData.gcsResultJsonNotFind.keys())) + "\n"
payloadStr += "GcsSourceJsonFile Count : " + str(responseData.gcsSourceCount) + "\n"
payloadStr += "GcsSourceJsonFile Fail Count : " + str(responseData.gcsSourceFailCount) + "\n"
if responseData.gcsSourceFailCount > 0:
	payloadStr += "GcsSourceJsonFile Fail Data : " + ", ".join(list(responseData.gcsSourceJsonNotFind.keys())) + "\n"
payloadStr += "GcsContentFile Count : " + str(responseData.gcsContentFileCount) + "\n"
payloadStr += "GcsContentFile Fail Count : " + str(responseData.gcsContentFileFailCount) + "\n"
if responseData.gcsContentFileFailCount > 0:
	payloadStr += "GcsContentFile Fail Data : " + ", ".join(list(responseData.gcsContentFileNotFind.keys())) + "\n"

payloadStr += "GcsResultValueFail Count : " + str(headerValues.gcsResultValueFailCount) + "\n"
if headerValues.gcsResultValueFailCount > 0:
	payloadStr += "GcsSourceJsonFile Fail Data : " + ", ".join(list(headerValues.gcsResultValueNotFind.keys())) + "\n"

start_upload = uploader(projectCode, localDownloadDefaultPath, file_key,zipPassword, gcsUploadDefaultPath, gcsUploadPrefixFileCode,grand_mail,historyDataIdFilePath,payloadStr)
if (flag == "upload"):
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