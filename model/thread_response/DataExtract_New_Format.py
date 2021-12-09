import sys
import os
import pandas
import datetime
import zipfile
import hashlib
import csv
import json
import numpy as np
from PIL import ImageDraw, Image
import math
import matplotlib.pyplot as plt
import subprocess
from Crypto.Cipher import AES
import base64
import shutil
import cleanse.clean

from model.path.PathUtil import PathUtil
from model.google.storage.GcsStorageUtil import GcsStorageUtil


class DataExtract:
	PathUtil = None
	GcsStorageUtil = None

	extractCount = 0

	fileNameDict = {}	# 중복 파일을 처리하기 위해서 맵

	gcsDownloadBucketCode = ''
	gcsUploadBucketCode = ''
	def __init__(self, gcsDownloadBucketCode, gcsUploadBucketCode):
		self.PathUtil = PathUtil()
		self.GcsStorageUtil = GcsStorageUtil()
		self.gcsDownloadBucketCode = gcsDownloadBucketCode
		self.gcsUploadBucketCode = gcsUploadBucketCode


	# Excel 파일 생성
	def makeExcel(self, list_data, columns, path, file_name, order_column=False):
		dataFrame = pandas.DataFrame(list_data, columns=columns)
		# 컬럼 네임으로 정렬
		if order_column is not False:
			dataFrame = dataFrame.sort_values(by=order_column)
		writer = pandas.ExcelWriter(path+file_name+'.xlsx', engine='xlsxwriter')
		dataFrame.to_excel(
			excel_writer=writer
			, sheet_name='Sheet1'
			, columns=None
			, header=True
			, index=False
			, startrow=0
			, startcol=0
			, encoding='euc-kr'
			, verbose=True
		)
		writer.save()

	# CSV 파일 생성
	def makeCsv(self, list_data, columns, path, file_name, sort_option=None):

		f = open(path+file_name+'.csv', 'w', encoding='utf-8-sig')

		header = ''
		index = 0
		for column in columns:
			header += column
			if (index+1) < len(columns):
				header+=','
			index+=1

		f.write(header+'\n')

		# wr = csv.writer(f, delimiter=' ') # 구분 공백
		wr = csv.writer(f)
		# wr.writerow(columns)

		# sort start 0
		if sort_option is not None:
			list_data = sorted(list_data, key=lambda row:row[int(sort_option)]) #

		for data in list_data:
			wr.writerow(data)
		f.close()
		return path+file_name+'.csv'

	# CSV --> Json 파일 변환
	def makeJson(self, projectCode, csvFilePathName):
		csvfile = open(csvFilePathName, 'r', encoding='utf-8')
		jsonFilePathName = csvFilePathName.replace('.csv','.json')
		jsonfile = open(jsonFilePathName, 'w', encoding='utf-8')
		reader = csv.DictReader(csvfile)
		out = json.dumps( [ row for row in reader ] , ensure_ascii=False)
		jsonfile.write(out)

		self.deleteFileName(projectCode, '.csv', csvFilePathName)

	# 1:n json:jpg 할 경우
	def makeJsonAllList(self, header, values, save_path, file_name):

		json_list = [dict(zip(header, values[i])) for i in range(len(values))]
		jsonFilePathName = save_path + file_name + '.json'
		jsonfile = open(jsonFilePathName, 'w', encoding='utf-8')
		out = json.dumps(json_list, ensure_ascii=False)
		jsonfile.write(out)

	# 1:1 json:jpg 할 경우
	def makeJson2List(self, header, json_list, save_path, file_name):
		json_list = [dict(zip(header, json_list))]
		jsonFilePathName = save_path + file_name + '.json'
		jsonfile = open(jsonFilePathName, 'w', encoding='utf-8')
		out = json.dumps(json_list, ensure_ascii=False)
		jsonfile.write(out)

	# CSV 파일 생성 (값만)
	def makeValueCsv(self, list_data, columns, path, file_name):
		f = open(path+file_name+'.csv', 'w', encoding='utf-8')
		wr = csv.writer(f)

		for data in list_data:
			wr.writerow(data)
		f.close()

    # Prefix 압축 파일 생성 (원본파일은 읽지 않고 바로 압축파일로 이동된다) # makeZipCompress('aaa','/data/collection/','홍길동','mp4')
	def makeZipPrefixCompress(self, projectCode, localDownloadDefaultPath, prefixKey, ext):
		print("Make Zip Files...")
		zipFileName = prefixKey + '.zip'
		zipFilePathName = localDownloadDefaultPath + zipFileName
		new_zips= zipfile.ZipFile(zipFilePathName, 'w')
		for folder, subfolders, files in os.walk(localDownloadDefaultPath):
			for file in files:
				if file.find(prefixKey) > -1 and file.find('.'+ext) > -1:	# Prefix 포함된 키워드만, 대상 확장자 체크
					new_zips.write(os.path.join(folder, file), os.path.relpath(os.path.join(folder,file), localDownloadDefaultPath), compress_type = zipfile.ZIP_STORED)
					print(os.path.join(folder, file))
					if localDownloadDefaultPath.find('/data/collection/'+projectCode) > -1:	# projectCode 방어로직
						# 압축하고 원본 삭제
						os.remove(os.path.join(folder, file))
		new_zips.close()
		return zipFileName

	# 압축 파일 생성 (비밀번호 추가) (원본파일은 읽지 않고 바로 압축파일로 이동된다)
	def makeZipCompressPassword(self, projectCode, localDownloadDefaultPath, srcFileName, passowrd):
		# 압축 하기
		print("Make Zip Files...")
		print("Make Zip Files : " + localDownloadDefaultPath)
		print("Make Zip Files : " + srcFileName)

		filename, fileExtension = os.path.splitext(srcFileName)
		secureZipFileName = 'secure_'+filename+'.zip'

		resultcode = subprocess.call(['zip','-m','-1','-j','-P',passowrd,localDownloadDefaultPath+secureZipFileName, localDownloadDefaultPath+srcFileName])
		print(resultcode)

		if resultcode == 0:
			return secureZipFileName
		else:
			print("Zip CompressPassword Error")
			return ''

	# 압축 파일 생성 (원본파일은 읽지 않고 바로 압축파일로 이동된다)
	def makeZipCompress(self, localDownloadDefaultPath, fileKey):
		# 압축 하기
		print("Make Zip Files...")
		zipFileName = fileKey + '.zip'
		zipFilePathName = localDownloadDefaultPath + zipFileName
		new_zips= zipfile.ZipFile(zipFilePathName, 'w')
		for folder, subfolders, files in os.walk(localDownloadDefaultPath):
			for file in files:
				# if file.find('.zip') > -1 or file.find('.csv') > -1 or file.find('.json') > -1:	# ZIP 파일 필터
				if file.find('.zip') > -1:	# ZIP 파일 필터
					continue
				else:
					new_zips.write(
						os.path.join(folder, file)
						, os.path.relpath(os.path.join(folder, file), localDownloadDefaultPath)
						, compress_type = zipfile.ZIP_STORED
					)
					prjFilePath = '/'.join(localDownloadDefaultPath.split('/')[:-1])
					if localDownloadDefaultPath.find(prjFilePath) > -1:	# projectCode 방어로직
						# 압축하고 원본 삭제
						os.remove(os.path.join(folder, file))
		new_zips.close()

		# 디렉토리가 있을 시 디렉토리 제거
		only_dir_list = [n for n in os.listdir(localDownloadDefaultPath) if
						 os.path.isdir(os.path.join(localDownloadDefaultPath, n))]
		if only_dir_list:
			for dir in only_dir_list:
				shutil.rmtree(os.path.join(localDownloadDefaultPath, dir))

		return zipFileName

	def makeFolderRangeCompress(self, localDownloadDefaultPath, file_list, start, end):
		# 갯수로 압축 하기
		FileName = 'file_cnt_%s' % str(end)
		FileNamePath = os.path.join(localDownloadDefaultPath, FileName)
		print("Make Directory Files... %s " % FileNamePath)
		FileList = file_list[start: end]
		for file in FileList:
			if not os.path.isdir(FileNamePath):
				os.mkdir(FileNamePath)
			shutil.move(os.path.join(localDownloadDefaultPath, file), os.path.join(FileNamePath, file))

		return FileName

	def makeZipRangeCompress(self, localDownloadDefaultPath, start, end, _prefix=None):
		# 갯수로 압축 하기
		print("Make Zip Files...")
		if _prefix == None:
			_prefix = '.' # 모든파일 압축하기
		else:
			_prefix = '.'+_prefix
		zipFileName = 'file_cnt_%s' % str(end) + '.zip'
		zipFilePathName = os.path.join(localDownloadDefaultPath, zipFileName)
		print(zipFilePathName)
		new_zips= zipfile.ZipFile(zipFilePathName, 'w')
		zip_file_list = [f for f in os.listdir(localDownloadDefaultPath) if _prefix in f][start : end]
		for file in zip_file_list:
			new_zips.write(
				os.path.join(localDownloadDefaultPath, file)
				, file
				, compress_type=zipfile.ZIP_STORED
		   )
			prjFilePath = '/'.join(localDownloadDefaultPath.split('/')[:-1])
			if localDownloadDefaultPath.find(prjFilePath) > -1:  # projectCode 방어로직
				# 압축하고 원본 삭제
				# os.remove(os.path.join(localDownloadDefaultPath, file))
				if file in zip_file_list:
					print(os.path.join(localDownloadDefaultPath, file))

		new_zips.close()
		return zipFileName

	# GCS 압축파일 업로드
	def gcsUploadZipFile(self, zipFilePath, zipFileName, uploadFilePath, uploadFileName, user_mail=None):
		now = datetime.datetime.now()
		print(now)
		print("GCS Uploading...")
		print(uploadFilePath+uploadFileName)
		uploadFilePathName = uploadFilePath+uploadFileName
		if user_mail is None:
			self.GcsStorageUtil.upload_blob(self.gcsUploadBucketCode, zipFilePath+zipFileName, uploadFilePathName)
		else:
			self.GcsStorageUtil.upload_blob(self.gcsUploadBucketCode, zipFilePath + zipFileName, uploadFilePathName, user_mail)
		now = datetime.datetime.now()
		print(now)
		return uploadFilePathName

	# 압축 파일 삭제
	def deleteZipFile(self, projectCode, zipFilePath, zipFileName):
		zipFilePathName = zipFilePath+zipFileName
		prjFilePath = '/'.join(zipFilePath.split('/')[:-1])
		if zipFilePathName.find(prjFilePath) > -1 and zipFilePathName.find('.zip') > -1:
			os.remove(zipFilePathName)

	# 파일 삭제
	def deleteFile(self, projectCode, ext, zipFilePath, zipFileName):
		zipFilePathName = zipFilePath+zipFileName
		prjFilePath = '/'.join(zipFilePath.split('/')[:-1])
		if zipFilePathName.find(prjFilePath) > -1 and zipFilePathName.find(ext) > -1:
			os.remove(zipFilePathName)

	# 파일 삭제
	def deleteFileName(self, projectCode, ext, fileName):
		if fileName.find('/data/collection/'+projectCode) > -1 and fileName.find(ext) > -1:
			os.remove(fileName)


class AESCipherCBC(object):
	"""
	DB에 사용자 핸드폰 번호가 AES128로 암호화 되어있다.
	암호화 된 핸드폰 번호를 풀 때 사용하셈 ㅇㅇ
	"""

	# 암호화 및 복호화시 필요한 요소 정의
	def __init__(self, key, iv):
		self.key = key
		self.iv = iv
		self.BS = AES.block_size
		self.pad = lambda s: s + (self.BS - len(s) % self.BS) * chr(self.BS - len(s) % self.BS)
		self.un_pad = lambda s: s[0:-ord(s[-1])]

	# 복호화
	def decrypt(self, encrypt):
		"""AES128/CBC/PKCS5 복호화(hexadecimal)"""
		cipher = AES.new(self.key, AES.MODE_CBC, IV=self.iv)
		deciphed_str= cipher.decrypt(base64.b64decode(encrypt))
		return_decrypt = self.un_pad(deciphed_str.decode('utf-8'))

		return return_decrypt
