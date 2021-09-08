import concurrent.futures
import sys
from tqdm import tqdm
import hashlib
import logging


import os
import pathlib
from google.cloud import storage
from model.path.PathUtil import PathUtil
from model.google.storage.GcsStorageUtil import GcsStorageUtil
from model.sql import mysql_query
import re
class DataLoad:
	sql = ''
	tmp = []
	PathUtil = None
	GcsStorageUtil = None
	gcsDownloadBucketCode = ''
	gcsUploadBucketCode = ''
	dataRows = 0
	gcsSourceCount = 0
	gcsSourceFailCount = 0
	gcsResultCount = 0
	gcsResultFailCount = 0
	gcsContentFileCount = 0
	gcsContentFileFailCount = 0

	labelDict = {} # Label
	resultDict = {}	# DB + ResultJson

	gcsResultJsonNotFind = {}

	def __init__(self, gcsDownloadBucketCode, gcsUploadBucketCode):
		self.PathUtil = PathUtil()
		self.GcsStorageUtil = GcsStorageUtil()
		self.gcsDownloadBucketCode = gcsDownloadBucketCode
		self.gcsUploadBucketCode = gcsUploadBucketCode
		self.resultData = []
	# DB 조회
	def add_query_arg(self,input, sql):
		if(len(input) == 0):
			return ""
		elif(len(input) == 1):
			return sql.format('"'+str(input[0])+'"')
		else:
			in_to_string = ','.join(map(lambda x: '"%s"' % x, input))
			return sql.format(in_to_string)
			

	def getdbData(self, db, requestInfo):
		db.connect()

		query_argu = {}
		query_argu['proId'] = requestInfo.projectId
		################################
		# TODO : 상태 검색
		################################
		whereInList = requestInfo.dataStatus

		self.sql = self.dataSet_query.format(**query_argu)
		rows = db.getRows(self.sql)
		for query_result in tqdm(rows):
			self.__setQueryResult(query_result)
		db.disconnect()
		self.dataRows = len(self.resultData)

		print("DB Rows : ", len(self.resultData))
	def setDbDatas(self, db, requestInfo, offset, limit, queryFilter, orderBy,data_idx,sql):
		db.connect()

		query_argu = {}
		query_argu['proId'] = requestInfo.projectId
		################################
		# TODO : 상태 검색
		################################
		whereInList = requestInfo.dataStatus
		
		whereinlist = 'pjd.prog_state_cd IN ({}) AND '
		datalist = 'pjd.data_idx IN ({}) AND' 

		query_argu['whereInList'] = self.add_query_arg(whereInList, whereinlist)
		query_argu['data_idx'] = self.add_query_arg(data_idx, datalist)
		query_argu['queryFilter'] = queryFilter
		query_argu['orderBy'] = orderBy
		query_argu['limit'] = str(limit)
		query_argu['offset_str'] = str(offset)
		self.sql = sql.format(**query_argu)
		print(self.sql)
		rows = db.getRows(self.sql)
		for query_result in tqdm(rows):
			self.__setQueryResult(query_result)
		db.disconnect()
		self.dataRows = len(self.resultData)

		print("DB Rows : ", len(self.resultData))

	# DB 조회 결과 값 Dict 대입
	def __setQueryResult(self, query_result):
		self.resultData.append(query_result)

	# GCS 결과 파일을 하나 읽어서 Label Dict 대입
	# def setLabels(self, requestInfo):
	# 	index = 0
	# 	items = self.resultDict.items()
	# 	for key, value in items:
	# 		if index > 0:
	# 			break
	# 		dataIdx = value['data_idx']
	# 		projectId = requestInfo.projectId

	# 		gcsResultFilePath = self.PathUtil.getGcsResultFilePath(requestInfo.groupId, requestInfo.projectId)
	# 		gcsResultFileName = self.PathUtil.getGCSResultFileName(dataIdx, projectId)
	# 		gcsResultJson = self.GcsStorageUtil.json_loads_with_prefix(self.gcsDownloadBucketCode, gcsResultFilePath, gcsResultFileName)

	# 		print('gcsResultJson : ', gcsResultJson)

	# 		fieldIndex = 0
	# 		for field in gcsResultJson['fields']:

	# 			# TODO : 범용적으로 모듈화 로직 예정
	# 			for children in field['children']:
	# 				print(children)
	# 				# # Header Text
	# 				# if children['id'] == 101:	
	# 				# 	name_uid = 'name_' + children['values']['uid']
	# 				# 	self.labelDict[name_uid] = {'id': children['id'], 'label': children['values']['label']}

	# 				# # Line Break (pass)
	# 				# elif children['id'] == 102:
	# 				# 	continue

	# 				# # Rich Editor
	# 				# elif children['id'] == 103:	
	# 				# 	name_uid = 'name_' + children['values']['uid']
	# 				# 	self.labelDict[name_uid] = {'id': children['id']}

	# 				# # Import Data
	# 				# elif children['id'] == 104:	
	# 				# 	continue

	# 				# # Long Text
	# 				# elif children['id'] == 202:
	# 				# 	name_uid = children['values']['name']
	# 				# 	self.labelDict[name_uid] = {'id': children['id'], 'label': children['values']['label'], 'description' : children['values']['description']}

	# 				# # Short Text
	# 				# elif children['id'] == 201:
	# 				# 	name_uid = children['values']['name']
	# 				# 	self.labelDict[name_uid] = {'id': children['id'], 'label': children['values']['label'], 'description' : children['values']['description']}

	# 				# # Radio Button
	# 				# elif children['id'] == 203:
	# 				# 	name_uid = children['values']['name']
	# 				# 	options = {}
	# 				# 	for option in children['values']['children']:
	# 				# 		options[option['value']] = option['label']
	# 				# 	self.labelDict[name_uid] = {'id': children['id'], 'label':children['values']['label'], 'options': options, 'description' : children['values']['description']}

	# 				# # CheckBox
	# 				# elif children['id'] == 204:
	# 				# 	name_uid = children['values']['name']
	# 				# 	options = {}
	# 				# 	for option in children['values']['children']:
	# 				# 		options[str(option['value'])] = str(option['label'])
	# 				# 	self.labelDict[name_uid] = {'id': children['id'], 'label':children['values']['label'], 'options': options, 'description' : children['values']['description']}

	# 				# # Multi Select
	# 				# elif children['id'] == 205:
	# 				# 	name_uid = children['values']['name']
	# 				# 	options = {}
	# 				# 	for option in children['values']['children']:
	# 				# 		options[str(option['value'])] = str(option['label'])
	# 				# 	self.labelDict[name_uid] = {'id': children['id'], 'label':children['values']['label'], 'options': options, 'description' : children['values']['description']}

	# 				# # Drop Down
	# 				# elif children['id'] == 206:
	# 				# 	name_uid = children['values']['name']
	# 				# 	options = {}
	# 				# 	for option in children['values']['children']:
	# 				# 		options[str(option['value'])] = str(option['label'])
	# 				# 	self.labelDict[name_uid] = {'id': children['id'], 'label':children['values']['label'], 'options': options, 'description' : children['values']['description']}

	# 				# # File Upload
	# 				# elif children['id'] == 207:
	# 				# 	name_uid = children['values']['name']
	# 				# 	self.labelDict[name_uid] = {'id': children['id'], 'label': children['values']['label'], 'description' : children['values']['description']}

	# 				# # lookup
	# 				# elif children['id'] == 208:
	# 				# 	name_uid = children['values']['name']
	# 				# 	self.labelDict[name_uid] = {'id': children['id'], 'label': children['values']['label'], 'description' : children['values']['description']}
	# 				# # Take Picture
	# 				# elif children['id'] == 301:
	# 				# 	name_uid = children['values']['name']
	# 				# 	self.labelDict[name_uid] = {'id': children['id'], 'label': children['values']['label'], 'description' : children['values']['description']}

	# 				# # Image Bounding
	# 				# elif children['id'] == 302:
	# 				# 	name_uid = children['values']['name']
	# 				# 	self.labelDict[name_uid] = {'id': children['id'], 'label': 'Image Bounding', 'value': children['values']}

	# 				# # Recording
	# 				# elif children['id'] == 304:
	# 				# 	name_uid = children['values']['name']
	# 				# 	self.labelDict[name_uid] = {'id': children['id'], 'label': children['values']['label'], 'description' : children['values']['description']}

	# 				# # 쌔로운거
	# 				# elif children['id'] == 305:
	# 				# 	name_uid = children['values']['name']
	# 				# 	self.labelDict[name_uid] = {'id': children['id'], 'label': ''}
					
	# 				# # Take Video
	# 				# elif children['id'] == 307:
	# 				# 	name_uid = children['values']['name']
	# 				# 	self.labelDict[name_uid] = {'id': children['id'], 'label': children['values']['label'], 'description' : children['values']['description']}

	# 				# # Text Tagging (TODO : 개발 필요)
	# 				# elif children['id'] == 309:
	# 				# 	name_uid = children['values']['name']
	# 				# 	self.labelDict[name_uid] = {'id': children['id'], 'label': 'Text Tagging'}

	# 				# # 처음 보는 케이스 시 중지
	# 				# else:
	# 				# 	print("Result ID Type ERROR :: " + str(children['id']))
	# 				# 	sys.exit()

	# 			fieldIndex+=1
	# 		index+=1

	# GCS SourceJson 데이터 Dict 대입
	def setGcsSourceJsonThread(self, requestInfo):
		# ThreadPool
		executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)

		storage_client = storage.Client()
		bucket = storage_client.get_bucket(self.gcsDownloadBucketCode)

		index = 0
		failIndex = 0
		futures = []
		items = self.resultData
		for item in items:
			futures.append(executor.submit(self.getGcsSourceJsonWorker, requestInfo=requestInfo, index=index, item = item, bucket=bucket))
			index += 1

		index = 0
		for future in concurrent.futures.as_completed(futures):
			data = future.result()

			if data['status'] <= 0:
				self.gcsResultJsonNotFind[data['data_idx']] = data['data_idx']
				failIndex += 1
			else :
				self.resultDict[data['data_idx']]['source_data'] = data['source_data']

			index += 1


		self.gcsSourceCount = index
		self.gcsSourceFailCount = failIndex
		print('GcsSourceJsonFile Count : ', index)
		print('GcsSourceJsonFile Fail Count : ', failIndex)
		if failIndex > 0 :
			print("Result ID Type ERROR :: Fail Data : " + ", ".join(list(self.gcsResultJsonNotFind.keys())))





	def getGcsSourceJsonWorker(self, requestInfo, index, item, bucket):

		print('setGcsSourceJson:: Processing...[', index, ']')
		key = item['data_idx']
		return_data = {'status': 1, 'data_idx': key, 'source_data': {}}

		dataIdx = str(item['data_idx'])
		projectId = requestInfo.projectId
		sourceIdx = str(item['source_id'])
		gcsSourceFilePath = self.PathUtil.getGcsSourceFilePath(requestInfo.groupId, projectId)
		gcsSourceFileName = self.PathUtil.getGcsSourceFileName(sourceIdx, projectId)

		try:
			# TODO : 버킷 정보 변수화
			gcsSourceJson = self.GcsStorageUtil.json_loads_with_prefix_openfile(bucket, self.gcsDownloadBucketCode, gcsSourceFilePath,
																	   gcsSourceFileName)

			return_data['source_data'] = gcsSourceJson
			print(gcsSourceJson)
			return_data['status'] = 1
			return return_data

		except:
			logging.exception('Fail : ', dataIdx)
			return_data['source_data'] = ''
			return_data['status'] = 0
			return return_data


	# GCS ResultJson 데이터 Dict 대입
	def setGcsResultJsonThread(self, requestInfo):

		# ThreadPool
		executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)

		storage_client = storage.Client()
		bucket = storage_client.get_bucket(self.gcsDownloadBucketCode)

		index = 0
		failIndex = 0
		futures = []
		items = self.resultDict.items()
		for key, value in items:
			futures.append(executor.submit(
				self.getGcsResultJsonWorker
				, requestInfo=requestInfo
				, index=index, key=key, value=value, bucket=bucket)
			)

			index += 1

		index = 0
		for future in concurrent.futures.as_completed(futures):
			data = future.result()
			if data['status'] <= 0:
				self.gcsResultJsonNotFind[str(data['data_idx'])] = data['data_idx']
				failIndex += 1
			else:
				self.resultDict[data['data_idx']]['result_data'] = data

				# for k, v in data.items():

				# 	if k == "status" or k == "data_idx":
				# 		continue
				# 	else:
				# 		self.resultDict[data['data_idx']][k] = v
				# 		pass

			# 이름에 대해 카운트가 필요할 때(name_count_dict)
			# self.resultDict['hanwha_file_list'] = {}

			index += 1

		self.gcsResultCount = index
		self.gcsResultFailCount = failIndex
		print('GcsResultJsonFile Count : ', index)
		print('GcsResultJsonFile Fail Count : ', failIndex)
		if failIndex > 0 :
			print("GcsResultJsonFile Fail Data : " + ", ".join(list(self.gcsResultJsonNotFind.keys())))

	def getGcsResultJsonWorker(self, requestInfo, index, key, value, bucket):
		
		print('setGcsResultJson:: Processing...[', index, '] : ', key)

		return_data = {'status': 1, 'data_idx':key, 'result_data':{}}
		dataIdx = value['data_idx']
		projectId = requestInfo.projectId # 단일 프로젝트 ID

		gcsResultFilePath = self.PathUtil.getGcsResultFilePath(requestInfo.groupId, projectId)
		gcsResultFileName = self.PathUtil.getGCSResultFileName(dataIdx, projectId)

		# TODO : 버킷 정보 변수화
		try:
			gcsResultJson = self.GcsStorageUtil.json_loads_with_prefix_openfile(
				bucket
				, self.gcsDownloadBucketCode
				, gcsResultFilePath
				, gcsResultFileName
			)
		except Exception as e:
			logging.exception('Fail : ', dataIdx)
			return_data['status'] = 0
			return return_data

		try:
			tmp = []
			for field in gcsResultJson['results']:
				tmp.append(field)
				for result in field:
					
					if result != None:
						for rKey, rVal in result.items():
							return_data['result_data'].update({rKey: rVal})
						# 간혹 결과 값이 비워져 있어서 신뢰 할수 없음.
						# self.resultDict[key]['source'].update({rKey: gcsResultJson['sources'][fieldIndex]})
			return_data['status'] = 1

		except Exception as e:
			logging.exception('No result_json : ', dataIdx)
			return_data['status'] = 0
			return return_data
		return_data['result_data'] = tmp
		return return_data

	def setGcsContentFileThread(self, requestInfo, source_obj_name, localDownloadDefaultPath):

		# ThreadPool
		executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)

		storage_client = storage.Client()
		bucket = storage_client.get_bucket(self.gcsDownloadBucketCode)

		index = 0
		failIndex = 0
		futures = []
		items = self.resultDict.items()
		for key, value in items:

			futures.append(executor.submit(self.setGcsContentFileWorker, requestInfo=requestInfo, index=index, key=key, value=value, source_obj_name=source_obj_name, localDownloadDefaultPath=localDownloadDefaultPath, bucket=bucket))

			index += 1

		index = 0
		for future in concurrent.futures.as_completed(futures):
			data = future.result()
			if data['status'] <= 0:
				self.gcsContentFileNotFind[data['data_idx']] = data['data_idx']
				failIndex += 1
			else :
				self.resultDict[data['data_idx']]['content_data'] = data['content_data']

			index += 1

		self.gcsContentFileCount = index
		self.gcsContentFileFailCount = failIndex
		print('GcsContentFile Count : ', index)
		print('GcsContentFile Fail Count : ', failIndex)
		if failIndex > 0 :
			print("GcsContentFile Fail Data : " + ", ".join(list(self.gcsContentFileNotFind.keys())))

	def setGcsContentFileWorker(self, requestInfo, index, key, value, source_obj_name, localDownloadDefaultPath, bucket):

		print('setGcsResultJson:: Processing...[', index, '] : ', key)

		return_data = {'status': 1, 'data_idx':key, 'content_data':{}}
		dataIdx = value['data_idx']
		projectId = requestInfo.projectId # 단일 프로젝트 ID

		contents_path = self.PathUtil.getGcsContentFilePath(requestInfo.groupId, projectId)
		contents_file_path = contents_path + value['source_data'][source_obj_name]
		try:
			######
			## TODO 소스 파일명 변경
			######
			# source_value = value['source_data'][source_obj_name].split('/')[1]
			source_value = value['source_data'][source_obj_name]
			dest_parent_path = pathlib.Path(localDownloadDefaultPath + source_value).parent.absolute()
			
			if os.path.isdir(dest_parent_path) == False:
				self.PathUtil.mkdir_p(dest_parent_path)

			self.GcsStorageUtil.download_blob_openfile(bucket, self.gcsDownloadBucketCode, contents_file_path, localDownloadDefaultPath + source_value)
			return_data['content_data'] = {'path' : localDownloadDefaultPath, 'file_name' : value['source_data'][source_obj_name]}
			return_data['status'] = 1
			return return_data

		except:
			logging.exception('Fail : ', dataIdx)

			return_data['status'] = 0
			return return_data

	def setuncleanGcsResultJsonThread(self, requestInfo):

		# ThreadPool
		executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)

		storage_client = storage.Client()
		bucket = storage_client.get_bucket(self.gcsDownloadBucketCode)

		index = 0
		failIndex = 0
		futures = []
		items = self.resultDict.items()
		for key, value in items:
			futures.append(executor.submit(
				self.getsetuncleanGcsResultJsonWorker
				, requestInfo=requestInfo
				, index=index, key=key, value=value, bucket=bucket)
			)

			index += 1

		index = 0
		for future in concurrent.futures.as_completed(futures):
			data = future.result()
			if data['status'] <= 0:
				self.gcsResultJsonNotFind[str(data['data_idx'])] = data['data_idx']
				failIndex += 1
			else:
				self.resultDict[data['data_idx']]['result_data'] = {}

				for k, v in data.items():

					if k == "status" or k == "data_idx":
						continue
					else:
						self.resultDict[data['data_idx']][k] = v
						pass

			# 이름에 대해 카운트가 필요할 때(name_count_dict)
			# self.resultDict['hanwha_file_list'] = {}

			index += 1

		self.gcsResultCount = index
		self.gcsResultFailCount = failIndex
		print('GcsResultJsonFile Count : ', index)
		print('GcsResultJsonFile Fail Count : ', failIndex)
		if failIndex > 0 :
			print("GcsResultJsonFile Fail Data : " + ", ".join(list(self.gcsResultJsonNotFind.keys())))

	def getsetuncleanGcsResultJsonWorker(self, requestInfo, index, key, value, bucket):
		
		print('setGcsResultJson:: Processing...[', index, '] : ', key)

		return_data = {'status': 1, 'data_idx':key, 'result_data':{}}
		dataIdx = value['data_idx']
		projectId = requestInfo.projectId # 단일 프로젝트 ID

		gcsResultFilePath = self.PathUtil.getGcsResultFilePath(requestInfo.groupId, projectId)
		gcsResultFileName = self.PathUtil.getGCSResultFileName(dataIdx, projectId)

		# TODO : 버킷 정보 변수화
		try:
			gcsResultJson = self.GcsStorageUtil.json_loads_with_prefix_openfile(
				bucket
				, self.gcsDownloadBucketCode
				, gcsResultFilePath
				, gcsResultFileName
			)
		except Exception as e:
			logging.exception('Fail : ', dataIdx)
			return_data['status'] = 0
			return return_data
		return_data['result_data'] = gcsResultJson['results']
		return_data['status'] = 1

		return return_data

		
