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
from model.path.PathUtil import PathUtil
from model.google.storage.GcsStorageUtil import GcsStorageUtil
from model.thread_response.DataLoad_New_Format import DataLoad
from model.thread_response.DataExtract_New_Format import DataExtract
from model.thread_response.DataHeaderValues import HeaderValues
import csv
class uploader:
    GcsStorageUtil = GcsStorageUtil()
    PathUtil = PathUtil()
    
    def __init__(self, localDownloadDefaultPath, file_key, gcsUploadDefaultPath, gcsUploadPrefixFileCode,grand_mail,historyDataIdFilePath,payloadStr):
        self.localDownloadDefaultPath =localDownloadDefaultPath
        self.file_key= file_key
        self.gcsUploadDefaultPath = gcsUploadDefaultPath
        self.gcsUploadPrefixFileCode = gcsUploadPrefixFileCode
        self.grand_mail = grand_mail
        self.payloadStr = payloadStr
    def upload(self):
        gcsUploadBucketCode = 'cw-downloads'   
        gcsDownloadBucketCode = 'cw_platform'
        extract = DataExtract(gcsDownloadBucketCode, gcsUploadBucketCode)
        zipFileName = extract.makeZipCompress(self.localDownloadDefaultPath, self.file_key)
        gcsUploadFilePathName = extract.gcsUploadZipFile(self.localDownloadDefaultPath, zipFileName, self.gcsUploadDefaultPath, self.gcsUploadPrefixFileCode + zipFileName, self.grand_mail)
        self.payloadStr += "압축파일명 : " + self.gcsUploadPrefixFileCode+zipFileName + "\n"
        self.payloadStr += "다운로드(권한) 경로 : " + "https://console.cloud.google.com/storage/browser/_details/" + gcsUploadBucketCode + "/" + gcsUploadFilePathName + "\n"
        return self.payloadStr
    
if __name__ == '__main__':
    ##############
    #Upload solely
    ##############
    GcsStorageUtil = GcsStorageUtil()
    PathUtil = PathUtil()
    gcsUploadBucketCode = 'cw-downloads'   
    gcsDownloadBucketCode = 'cw_platform'
    #################################################
    projectCode = 'Data'
    projectId = 'kb'
    grand_mail = 'jisun.kim@crowdworks.kr'
    jiraCode = 'DATA1731'
    #################################################
    localDownloadDefaultPath = '/data/collection/'+projectCode+'/'
    localDownloadDefaultPath = '/data/collection/DATA1731'
    file_key = PathUtil.getTodayYYYYMMDD()
    zipPassword = 'cw#data!'+projectId
    gcsUploadPrefixFileCode= jiraCode + '_' + projectId + "_"
    gcsUploadDefaultPath = 'pm_download/' + projectCode + '/'
    extract = DataExtract(gcsDownloadBucketCode, gcsUploadBucketCode)
    zipFileName = extract.makeZipCompress(projectCode, localDownloadDefaultPath, file_key)
    zipFileName = extract.makeZipCompressPassword(projectCode, localDownloadDefaultPath, zipFileName, zipPassword)
    gcsUploadFilePathName = extract.gcsUploadZipFile(localDownloadDefaultPath, zipFileName, gcsUploadDefaultPath, gcsUploadPrefixFileCode + zipFileName, grand_mail)
    payloadStr = ''
    payloadStr += "압축파일명 : " + gcsUploadPrefixFileCode+zipFileName + "\n"
    payloadStr += "압축 비밀번호 : " + zipPassword + "\n"
    payloadStr += "다운로드(권한) 경로 : " + "https://console.cloud.google.com/storage/browser/_details/" + gcsUploadBucketCode + "/" + gcsUploadFilePathName + "\n"
    print(payloadStr)
