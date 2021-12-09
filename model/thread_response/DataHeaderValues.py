import sys
import csv
import datetime
import os
import pandas as pd
import zipfile
import concurrent.futures
from google.cloud import storage
from PIL import Image
import numpy as np
import math
import re
import json
import io
import time
import uuid
from Crypto.Cipher import AES
import base64
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

    def interchange_dict(self,table):
        lab = table['label']
        val = table['value']
        return {val:lab}
    
    def label_template(self,elem):
        return_dict = {}
        # print(elem)
        try:
            id = str(elem['id'])
            values = elem['values']
            field = values['label']
        except:
            pass

    #203 Radiobutton
        if (id == '203'):
            name = values['name']
            val_lab = values['children']
            return_dict.update({'label':values['label']})
            for each in val_lab:
                each = self.interchange_dict(each)
                return_dict.update(each)


    #204 checkbox
        elif (id == '204'):
            name = values['name']
            val_lab = values['children']
            return_dict.update({'label':values['label']})
            for each in val_lab:
                each = self.interchange_dict(each)
                return_dict.update(each)
                
    #205 multiselect
        elif (id == '205'):
            name = values['name']
            val_lab = values['children']
            return_dict.update({'label':values['label']})
            for each in val_lab:
                each = self.interchange_dict(each)
                return_dict.update(each)


    #206 Drop Down
        elif (id=='206'):
            name = values['name']
            val_lab = values['children']
            return_dict.update({'label':values['label']})
            for each in val_lab:
                each = self.interchange_dict(each)
                return_dict.update(each)

    #304 Recording
        elif (id == '304'):
            name = values['name']
            return_dict.update({'label' : values['label']})

    #unspecified or none
        else:
            return {}
        return {name:return_dict}


    def setLabels(self, requestInfo, result, projectId):
        total_labels = {}
        val_to_label = {}
        result =result[0]
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.gcsDownloadBucketCode)
        gcsResultFilePath = self.PathUtil.getGcsResultFilePath(requestInfo.groupId, projectId)
        gcsResultFileName = self.PathUtil.getGCSResultFileName(str(result['data_idx']), projectId)
        gcsResultJson = self.GcsStorageUtil.json_loads_with_prefix_openfile(
				bucket
				, self.gcsDownloadBucketCode
				, gcsResultFilePath
				, gcsResultFileName
			)
        fields = gcsResultJson['fields']
        for each_field in fields:
            if (each_field['id']) == 999:
                sub_child = each_field['children']
            else:
                pass
            for child in sub_child:
                total_labels.update(self.label_template(child))
        print('total labels \n\n')
        print(total_labels)  
            
    #     print('gcsResultJso  n : ', gcsResultJson)

    #     fieldIndex = 0
    #     for field in gcsResultJson['fields']:

    #         # TODO : 범용적으로 모듈화 로직 예정
    #         for children in field['children']:
    #             print(children)
    #             # # Header Text
    #             # if children['id'] == 101:	
    #             # 	name_uid = 'name_' + children['values']['uid']
    #             # 	self.labelDict[name_uid] = {'id': children['id'], 'label': children['values']['label']}

    #             # # Line Break (pass)
    #             # elif children['id'] == 102:
    #             # 	continue

    #             # # Rich Editor
    #             # elif children['id'] == 103:	
    #             # 	name_uid = 'name_' + children['values']['uid']
    #             # 	self.labelDict[name_uid] = {'id': children['id']}

    #             # # Import Data
    #             # elif children['id'] == 104:	
                # 	continue

                # # Long Text
                # elif children['id'] == 202:
                # 	name_uid = children['values']['name']
                # 	self.labelDict[name_uid] = {'id': children['id'], 'label': children['values']['label'], 'description' : children['values']['description']}

                # # Short Text
                # elif children['id'] == 201:
                # 	name_uid = children['values']['name']
                # 	self.labelDict[name_uid] = {'id': children['id'], 'label': children['values']['label'], 'description' : children['values']['description']}

                # # Radio Button
                # elif children['id'] == 203:
                # 	name_uid = children['values']['name']
                # 	options = {}
                # 	for option in children['values']['children']:
                # 		options[option['value']] = option['label']
                # 	self.labelDict[name_uid] = {'id': children['id'], 'label':children['values']['label'], 'options': options, 'description' : children['values']['description']}

                # # CheckBox
                # elif children['id'] == 204:
                # 	name_uid = children['values']['name']
                # 	options = {}
                # 	for option in children['values']['children']:
                # 		options[str(option['value'])] = str(option['label'])
                # 	self.labelDict[name_uid] = {'id': children['id'], 'label':children['values']['label'], 'options': options, 'description' : children['values']['description']}

                # # Multi Select
                # elif children['id'] == 205:
                # 	name_uid = children['values']['name']
                # 	options = {}
                # 	for option in children['values']['children']:
                # 		options[str(option['value'])] = str(option['label'])
                # 	self.labelDict[name_uid] = {'id': children['id'], 'label':children['values']['label'], 'options': options, 'description' : children['values']['description']}

                # # Drop Down
                # elif children['id'] == 206:
                # 	name_uid = children['values']['name']
                # 	options = {}
                # 	for option in children['values']['children']:
                # 		options[str(option['value'])] = str(option['label'])
                # 	self.labelDict[name_uid] = {'id': children['id'], 'label':children['values']['label'], 'options': options, 'description' : children['values']['description']}

                # # File Upload
                # elif children['id'] == 207:
                # 	name_uid = children['values']['name']
                # 	self.labelDict[name_uid] = {'id': children['id'], 'label': children['values']['label'], 'description' : children['values']['description']}

                # # lookup
                # elif children['id'] == 208:
                # 	name_uid = children['values']['name']
                # 	self.labelDict[name_uid] = {'id': children['id'], 'label': children['values']['label'], 'description' : children['values']['description']}
                # # Take Picture
                # elif children['id'] == 301:
                # 	name_uid = children['values']['name']
                # 	self.labelDict[name_uid] = {'id': children['id'], 'label': children['values']['label'], 'description' : children['values']['description']}

                # # Image Bounding
                # elif children['id'] == 302:
                # 	name_uid = children['values']['name']
                # 	self.labelDict[name_uid] = {'id': children['id'], 'label': 'Image Bounding', 'value': children['values']}

                # # Recording
                # elif children['id'] == 304:
                # 	name_uid = children['values']['name']
                # 	self.labelDict[name_uid] = {'id': children['id'], 'label': children['values']['label'], 'description' : children['values']['description']}

                # # 쌔로운거
                # elif children['id'] == 305:
                # 	name_uid = children['values']['name']
                # 	self.labelDict[name_uid] = {'id': children['id'], 'label': ''}
                
                # # Take Video
                # elif children['id'] == 307:
                # 	name_uid = children['values']['name']
                # 	self.labelDict[name_uid] = {'id': children['id'], 'label': children['values']['label'], 'description' : children['values']['description']}

                # # Text Tagging (TODO : 개발 필요)
                # elif children['id'] == 309:
                # 	name_uid = children['values']['name']
                # 	self.labelDict[name_uid] = {'id': children['id'], 'label': 'Text Tagging'}

                # # 처음 보는 케이스 시 중지
                # else:
                # 	print("Result ID Type ERROR :: " + str(children['id']))
                # 	sys.exit()

#             fieldIndex+=1
#         index+=1

# GCS SourceJson 데이터 Dict 대입