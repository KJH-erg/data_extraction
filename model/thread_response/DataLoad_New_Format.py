import concurrent.futures
import sys
from tqdm import tqdm
import hashlib
import logging
import json

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
	gcsError = []

	def __init__(self, gcsDownloadBucketCode, gcsUploadBucketCode):
		self.PathUtil = PathUtil()
		self.GcsStorageUtil = GcsStorageUtil()
		self.gcsDownloadBucketCode = gcsDownloadBucketCode
		self.gcsUploadBucketCode = gcsUploadBucketCode
		self.resultData = []
	# DB 조회
	'''
	query related methods
	'''
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

	'''
	get_source_data methods
	'''
	def setGcsSourceJsonThread(self):
		# ThreadPool
		storage_client = storage.Client()
		bucket = storage_client.get_bucket(self.gcsDownloadBucketCode)
		with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
			index = 0
			futures = []
			items = self.resultData
			for item in items:
				futures.append(executor.submit(self.getGcsSourceJsonWorker, index=index, item = item, bucket=bucket))
				index += 1
			source_error = []
			for future in concurrent.futures.as_completed(futures):
				elem = future.result()
				if (elem['source_status'] <= 0):
					source_error.append(elem['data_idx'])	
				else:
					pass
		return source_error


	def getGcsSourceJsonWorker(self, index, item, bucket):
		print('setGcsSourceJson:: Processing...[', index, ']')
		key = item['data_idx']
		item.update({'source_status' : 1})
		dataIdx = str(item['data_idx'])
		projectId = str(item['project_id'])
		sourceIdx = str(item['source_id'])
		gcsSourceFilePath = self.PathUtil.getGcsSourceFilePath(str(item['customer_group_id']), projectId)
		gcsSourceFileName = self.PathUtil.getGcsSourceFileName(sourceIdx, projectId)

		try:
			# TODO : 버킷 정보 변수화
			gcsSourceJson = self.GcsStorageUtil.json_loads_with_prefix_openfile(bucket, self.gcsDownloadBucketCode, gcsSourceFilePath,
																		gcsSourceFileName)
			for key,val in gcsSourceJson.items():
				item[key] = val
			item['source_status'] = 1
			return item

		except:
			item.update({'source_status' : 0})
			return item

	'''
	result_json 관련 methods
	'''	
	# GCS ResultJson 데이터 Dict 대입
	def setGcsResultJsonThread(self):

		# ThreadPool
		storage_client = storage.Client()
		bucket = storage_client.get_bucket(self.gcsDownloadBucketCode)
		with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
			index = 0
			failIndex = 0
			futures = []
			items = self.resultData
			for item in items:
				futures.append(executor.submit(
					self.getGcsResultJsonWorker
					, index=index, item=item, bucket=bucket)
				)

				index += 1
			
			result_error = []
			for future in concurrent.futures.as_completed(futures):
				elem = future.result()
				if (elem['result_status'] <= 0):
					result_error.append(elem['data_idx'])	
				else:
					pass
		return result_error
	def field_recursive(self, field,result,total):
		if type((field)) == dict:
			total.append(result)
		else:
			for f,r in zip(field,result):
				self.field_recursive(f,r,total)
		return total

	
	def getGcsResultJsonWorker(self, index, item, bucket):
		print('setGcsResultJson:: Processing...[', index, '] : ')
		key = item['data_idx']
		item.update({'result_status' : 1})
		dataIdx = str(item['data_idx'])
		projectId = str(item['project_id'])
		gcsResultFilePath = self.PathUtil.getGcsResultFilePath(str(item['customer_group_id']), projectId)
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
			item['result_status'] = 0
			return item
	
     	
		try:
			tmp = []
			# for json download
			# with open ('/data/collection/PRJ3394/'+gcsResultFileName,'w')as f:
			# 	json.dump(gcsResultJson,f,indent=4)
			# total_keys = []
			# t= self.field_recursive(gcsResultJson['fields'], gcsResultJson['results'],tmp)
			# print(t)
				
				# for second in first:
				# 	if second !=[]
			item['result'] = gcsResultJson['results']
			# item['reason'] = gcsResultJson['works']['histories'][0]['description']
			# item['fields'] = gcsResultJson['fields'][0]['children'][0]['values']['advanced']['config']
			# for field in gcsResultJson['results']:
			# 	for result in field:
			# 		print(result)
			# 		if result != None:
			# 			for rKey, rVal in result.items():
			# 				return_data['result_data'].update({rKey: rVal})
			# 			# 간혹 결과 값이 비워져 있어서 신뢰 할수 없음.
			# 			# self.resultDict[key]['source'].update({rKey: gcsResultJson['sources'][fieldIndex]})
			item['result_status'] = 1

		except Exception as e:
			logging.exception('No result_json : ', dataIdx)
			item['result_status'] = 0
			return item
		item['result_data'] = tmp
		return item


	def setGcsContentFileThread(self, source_obj_name, localDownloadDefaultPath):
		content_error = []
		# ThreadPool
		with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
			storage_client = storage.Client()
			bucket = storage_client.get_bucket(self.gcsDownloadBucketCode)

			index = 0
			failIndex = 0
			futures = []
			items = self.resultData
			for item in items:
				futures.append(executor.submit(self.setGcsContentFileWorker, index=index, item = item, source_obj_name=source_obj_name, localDownloadDefaultPath=localDownloadDefaultPath, bucket=bucket))
				index += 1
    
			for future in concurrent.futures.as_completed(futures):
				elem = future.result()
			if (elem['content_status'] <= 0):
				content_error.append(elem['data_idx'])	
			else:
				pass
			return content_error
		
  		# for future in concurrent.futures.as_completed(futures):
		# 	data = future.result()
		# 	if data['status'] <= 0:
		# 		self.gcsContentFileNotFind[data['data_idx']] = data['data_idx']
		# 		failIndex += 1
		# 	else :
		# 		self.resultDict[data['data_idx']]['content_data'] = data['content_data']

		# 	index += 1

		# self.gcsContentFileCount = index
		# self.gcsContentFileFailCount = failIndex
		# print('GcsContentFile Count : ', index)
		# print('GcsContentFile Fail Count : ', failIndex)
		# if failIndex > 0 :
		# 	print("GcsContentFile Fail Data : " + ", ".join(list(self.gcsContentFileNotFind.keys())))

	def setGcsContentFileWorker(self, index, item, source_obj_name, localDownloadDefaultPath, bucket):

		print('setGcsResultJson:: Processing...[', index, '] : ')

		item.update({'content_status' : 1})
		projectId = str(item['project_id'])

		contents_path = self.PathUtil.getGcsContentFilePath(str(item['customer_group_id']), projectId)
		
		contents_file_path = contents_path + item[source_obj_name]

		try:
			######
			## TODO 소스 파일명 변경
			######
			# source_value = value['source_data'][source_obj_name].split('/')[1]
			source_value = item[source_obj_name]
			dest_parent_path = pathlib.Path(localDownloadDefaultPath + source_value).parent.absolute()
			
			
			if os.path.isdir(dest_parent_path) == False:
				self.PathUtil.mkdir_p(dest_parent_path)

			self.GcsStorageUtil.download_blob_openfile(bucket, self.gcsDownloadBucketCode, contents_file_path, localDownloadDefaultPath + source_value)
			# return_data['content_data'] = {'path' : localDownloadDefaultPath, 'file_name' : value['source_data'][source_obj_name]}
			# return_data['status'] = 1
			# return return_data
			item['content_status'] = 1
			return item

		except:
			item['content_status'] = 0
			return item