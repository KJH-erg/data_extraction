import sys
import csv
import datetime
import os
import pandas as pd
import zipfile
import concurrent.futures
from google.cloud import storage
import xlsxwriter
from PIL import Image
import numpy as np
import math
import re
import json
import io
import moviepy.editor as mpe
import time
import uuid
from Crypto.Cipher import AES
import base64
from scipy.io.wavfile import read as read_wav
import librosa
import soundfile
import shutil
import soundfile as sf
from model.google.storage.GcsStorageUtil import GcsStorageUtil
from model.path.PathUtil import PathUtil
from model.thread_response.DataExtract_New_Format import AESCipherCBC
from cleanse import tools

class HeaderValues:
    PathUtil = None
    GcsStorageUtil = None

    gcsResultValueFailCount = 0
    gcsResultValueNotFind = {}

    def __init__(self, gcsDownloadBucketCode, gcsUploadBucketCode):
        self.PathUtil = PathUtil()
        self.GcsStorageUtil = GcsStorageUtil()
        self.gcsDownloadBucketCode = gcsDownloadBucketCode
        self.gcsUploadBucketCode = gcsUploadBucketCode

    # def makeExcelZipCompress(self, projectCode, localDownloadDefaultPath, fileKey):
    #     # 압축 하기
    #     print("Make Zip Files...")
    #     zipFileName = fileKey + '.zip'
    #     zipFilePathName = localDownloadDefaultPath + zipFileName
    #     new_zips = zipfile.ZipFile(zipFilePathName, 'w')
    #     for folder, subfolders, files in os.walk(localDownloadDefaultPath):
    #         for file in files:
    #             if file.find('.xlsx') <= -1:  # ZIP 파일 필터
    #                 continue
    #             else:
    #                 new_zips.write(os.path.join(folder, file),
    #                                os.path.relpath(os.path.join(folder, file), localDownloadDefaultPath),
    #                                compress_type=zipfile.ZIP_STORED)
    #                 print(os.path.join(folder, file))
    #                 # print(os.path.relpath(os.path.join(folder,file), localDownloadDefaultPath))
    #                 if localDownloadDefaultPath.find('/data/collection/' + projectCode) > -1:  # projectCode 방어로직
    #                     # 압축하고 원본 삭제
    #                     os.remove(os.path.join(folder, file))
    #     new_zips.close()
    #     return zipFileName

    # 압축 파일 생성 (원본파일은 읽지 않고 바로 압축파일로 이동된다)
    # def makeZipCompress(self, projectCode, localDownloadDefaultPath, fileKey):
    #     # 압축 하기
    #     print("Make Zip Files...")
    #     zipFileName = fileKey + '.zip'
    #     zipFilePathName = localDownloadDefaultPath + zipFileName
    #     new_zips = zipfile.ZipFile(zipFilePathName, 'w')
    #     for folder, subfolders, files in os.walk(localDownloadDefaultPath):
    #         for file in files:
    #             if file.find('.zip') > -1:  # ZIP 파일 필터
    #                 # if file.find('.zip') > -1 or file.find('.xlsx') > -1 or file.find('.csv') > -1 or file.find('.json') > -1:	# ZIP 파일 필터
    #                 continue
    #             else:
    #                 new_zips.write(os.path.join(folder, file),
    #                                os.path.relpath(os.path.join(folder, file), localDownloadDefaultPath),
    #                                compress_type=zipfile.ZIP_STORED)
    #                 print(os.path.join(folder, file))
    #                 # print(os.path.relpath(os.path.join(folder,file), localDownloadDefaultPath))
    #                 if localDownloadDefaultPath.find('/data/collection/' + projectCode) > -1:  # projectCode 방어로직
    #                     # 압축하고 원본 삭제
    #                     os.remove(os.path.join(folder, file))
    #     new_zips.close()
    #     return zipFileName

    # responseData : resultDict > source_data - 소스 데이터 내용

    #데이터 컬럼 배열에 넣기
    def getHeaderArr(self, responseData):
        header = []
        header.append('DataID')
        header.append('UserID')

        name_map = ''
        # 추출 포멧 정의
        index = 0
        # by id
        for labelKey, labelVal in responseData.labelDict.items():

            if labelVal['id'] == 101:
                index += 1
                continue

            elif labelVal['id'] == 103:
                index += 1
                continue

            if labelVal['label'] == '':
                pass
            else:
                if isinstance(name_map, dict) is True:
                    header.append(name_map[labelKey].replace(',', ' ').replace('\n', ''))
                else:
                    # 레이블
                    header.append(labelVal['label'].replace(',',' ').replace('\n',''))
                    # 값
                    # header.append(labelKey.replace(',', ' ').replace('\n', ''))
                    pass
            index += 1
        print('Header : ', header)
        return header

    #데이터 값 배열에 넣기
    def getValuesArr(self, responseData, requestInfo, localDownloadDefaultPath):
        resultData = []
        sourceData = []
        data_index = []
        data = []
        index = 0

        tmpKey = ''
        tmpIndex = 0

        takePictureFileNameIndex = 0
        takeVideoFileNameIndex = 1
        failIndex = 0
        # ThreadPool
        futures = []

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=40) # default 30
        storage_client = storage.Client()
        self.storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.gcsDownloadBucketCode)

        now = datetime.datetime.now()
        # self.aess = AESCipherCBC("___freebee_db___", "___freebee_db___")
        for rKey, rVal in responseData.resultDict.items():
            rKey = str(rKey)

            # dataID 이외의 값 건너뛰기wwwsasdad
            if rKey.isdigit() is False:
                continue
            # normal process
            futures.append(executor.submit(self.getValuesArrWorker, responseData=responseData, requestInfo=requestInfo,
                                           localDownloadDefaultPath=localDownloadDefaultPath, index=index, rKey=rKey,
                                           rVal=rVal, bucket=bucket))
            index += 1

        index = 0
        for future in concurrent.futures.as_completed(futures):
            data = future.result()
            # if data['status'] <= 0:
            #     self.gcsResultValueFailCount += 1
            #     self.gcsResultValueNotFind[data['data_idx']] = data['data_idx']
            #     failIndex += 1
            # else:
            resultData.append(data)

            index += 1

        now = datetime.datetime.now()
        print(now)
        self.extractCount = index
        print("Data Rows : ", index)

        return resultData, sourceData

    def getValuesArrWorker(self, responseData, requestInfo, localDownloadDefaultPath, index, rKey, rVal, bucket):
        
        print('Thread Processing...[', index, '] : ', rKey)
        data = []
        source_data = []
        return_data = {}

        try:
            return_data = {'status': 1, 'data_idx': rKey}
            for keys,elems in rVal.items():
                if (keys != 'result_data'):
                    return_data[keys] = elems
                else:
                    for keys,elems in rVal['result_data'].items():
                        keys = tools.get_header(keys)
                        return_data[keys] = elems
            index += 1
            
            return return_data
        except Exception as ex:
            print("ERROR : ", ex)
            return_data = {'status': 0, 'data_idx': rKey, 'data': None}
            return return_data

