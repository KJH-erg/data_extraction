import hashlib
# import datetime
from datetime import date, timedelta
import os
import errno

class PathUtil:

	gcsResultKey = 'cw_result_data'
	gcsSourceKey = 'cw_source_data'
	# gcs_result_prefix = group_id+'/'+project_id+'_result/'

	# 오늘 날짜
	def getTodayYYYYMMDD(self):	# YYYY-MM-DD
		day = date.today()
		return str(day.strftime('%Y-%m-%d'))

	# 어제 날짜
	def getYesterdayYYYYMMDD(self): # YYYY-MM-DD
		yesterday = date.today() - timedelta(1)
		return str(yesterday.strftime('%Y-%m-%d'))
		# return '2020-08-31'

	# 디렉토리 생성
	def mkdir_p(self, path):
	    try:
	        os.makedirs(path)
	    except OSError as exc:  # Python >2.5
	        if exc.errno == errno.EEXIST and os.path.isdir(path):
	            pass
	        else:
	            raise

	# GCS에 업로드 되어 있는 결과 JSON 파일 기본 경로 가져오기
	def getGcsResultFilePath(self, groupId, projectId):
		return groupId+'/'+projectId+'_result/'
	def getSeqnum(self,json):
		return json["lastSeqNum"]
		
	# GCS에 업로드 되어 있는 결과 JSON 파일명 가져오기
	def getGCSResultFileName(self, dataIdx, projectId):
		return str(dataIdx)+'_'+hashlib.md5(str(projectId+'/'+str(dataIdx)+'/'+self.gcsResultKey).encode()).hexdigest()[0:10]

	# GCS에 업로드 되어 있는 결과 파일명 가져오기
	def getGCSResultSeqNumFileName(self, dataIdx, projectId, seqNum):
		return str(dataIdx)+'_'+hashlib.md5(str(projectId+'/'+str(dataIdx)+'/'+self.gcsResultKey).encode()).hexdigest()[0:10]+'_'+str(seqNum)

	# GCS에 업로드 되어 있는 소스 JSON 파일 기본 경로 가져오기
	def getGcsSourceFilePath(self, groupId, projectId):
		return groupId+'/'+projectId+'_source/_source_data/'

	# GCS에 업로드 되어 있는 소스 JSON 파일명 가져오기
	def getGcsSourceFileName(self, sourceIdx, projectId):
		return str(sourceIdx)+'_'+hashlib.md5(str(projectId+'/'+str(sourceIdx)+'/'+self.gcsSourceKey).encode()).hexdigest()[0:10]

	# GCS에 업로드 되어 있는 컨텐츠 파일 기본 경로 가져오기
	def getGcsContentFilePath(self, groupId, projectId):
		return groupId+'/'+projectId+'_content/_content_data/'

	# 어제 날짜
	def getYesterdayYYYYMMDD(self): # YYYY-MM-DD
			yesterday = date.today() - timedelta(1)
			return str(yesterday.strftime('%Y-%m-%d'))
			# return '2020-08-31'
	# 이전 주 날짜
	def getPreviousWeekYYYYMMDD(self): # YYYY-MM-DD
			yesterday = date.today() - timedelta(7)
			return str(yesterday.strftime('%Y-%m-%d'))
			# return '2020-08-31'