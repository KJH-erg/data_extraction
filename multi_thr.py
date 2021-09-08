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
dataLimit = '10000'			# 300 # 데이터 건수를 분리하여 처리하도록 개수 정의
groupId = '517'			# 294
projectCode = 'EMART24'		# hyundai-autoever
jiraCode = 'prj3037'			# prj769
fileFormat = 'csv'		# 결과파일(매칭) 포멧 - json, csv, excel

###### //파라미터 변수 ######
#*************** TODO: 수동 설정 변수 ***************
#dataStatusCode = ["CHECK_END", "CHECK_ING", "CHECK_REJECT", "CHECK_REWORK", "WORK_END", "ALL_FINISHED",'WORK_ING'] # list
#dataStatusCode = ["ALL_FINISHED"] # "ALL_FINISHED", "WORK_END"
dataStatusCode = ['ALL_FINISHED']

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
queryfilter += ' AND pjd.check_edate BETWEEN "2021-08-13 00:00:00" AND "2021-09-01 23:59:59" '
#queryfilter += ' AND pjd.check_edate <= "2021.07.04 59:59:59" '
# WORK_END 작업 시
#queryfilter += 'AND pjd.work_edate BETWEEN "2021-07-26 00:00:00" AND "2021-08-05 23:59:59"'
# queryfilter += ' AND pjd.work_edate <= "2021-03-05 12:00:00" '
#queryfilter += 'AND pjd.work_sdate BETWEEN "2021-07-09 11:01:00" AND "2021-07-15 00:00:00"'

# 이미지 사용 시
sourceFileJsonKeyName = 'file_name'		# 소스파일다운로드 할 Json 키명(file_name, image_url, upload_files, filenames)


#*************** //수동 설정 변수 ***************
###### 프로그램 설정 변수  바뀔필요 적음
orderBy = 'ORDER BY 3 ASC'	 # data_id
localDownloadDefaultPath = '/data/collection/'+projectCode+'/' + PathUtil.getTodayYYYYMMDD() + '/'
logging_path = '/home/de/jhoon/log/'+jiraCode+'/'
PathUtil.mkdir_p(logging_path)
logging.basicConfig(filename=logging_path+'{}.log'.format(PathUtil.getTodayYYYYMMDD()),format='%(asctime)s %(message)s', level=logging.INFO)
PathUtil.mkdir_p(localDownloadDefaultPath)	# 다운로드 파일 생성
gcsDownloadBucketCode = 'cw_platform'
# gcsUploadBucketCode = 'cw_pm_upload_files' # 고객사 전용(임시) : 'cw_pm_upload_files', 내부 : 'cw-downloads'
gcsUploadBucketCode = 'cw-downloads'
gcsUploadDefaultPath = 'pm_download/' + projectCode + '/'
###### //프로그램 설정 변수 ######

total_df = pd.DataFrame()
total_id = [9192,9262,9263,9264,9265]
# total_id= total_id +[8757,8758,8759,8760,8761]
for id in total_id:
	projectId = str(id)
	gcsUploadPrefixFileCode= projectCode + '_' + projectId + "_"
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

	logging.info(responseData.sql)
	print("Filter responseData.resultDict : " + str(len(responseData.resultDict)))
	# Label 정보 가져오기
	# responseData.setLabels(requestInfo)
	# ResultJson 결과 정보 가져오기
	#responseData.setGcsSourceJsonThread(requestInfo)
	responseData.setuncleanGcsResultJsonThread(requestInfo)
	# ########### Header #####gl#######
	header = headerValues.getHeaderArr(responseData)
	sourceFileJsonKeyName = 'file_name'
	#start_clean = cleaner(localDownloadDefaultPath,gcsUploadPrefixFileCode,jiraCode,projectId,groupId)

	# ########### Values ##############
	values = responseData.resultDict
	# try:
	# 	responseData.setGcsContentFileThread(requestInfo, sourceFileJsonKeyName, localDownloadDefaultPath)
	# except:
	# 	pass
	#start_clean.preprocess(values)
	df = pd.DataFrame.from_dict(values,orient='index')
	total_df = total_df.append(df)

total_df.to_excel('/data/collection/result.xlsx')
	# total_dict = total_dict+values
# df = pd.DataFrame.from_dict(total_dict)
# result = start_clean.clean_df(df)
# result.to_csv('/data/collection/HYUNDAI/test.csv')
print('wait')

	#파일 다운로드용