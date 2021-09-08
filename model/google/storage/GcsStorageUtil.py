import io

import json
from PIL import Image
from google.cloud import storage

from model.path.PathUtil import PathUtil

class GcsStorageUtil:
	PathUtil = None

	def __init__(self):
		self.PathUtil = PathUtil()

	def json_loads_with_prefix(self, bucket_name, prefix, file_name):
		storage_client = storage.Client()
		bucket = storage_client.get_bucket(bucket_name) #bucket정보 저장
		blob = bucket.blob(prefix+file_name)
		data = json.loads(blob.download_as_string(client=None))
		return data

	def json_loads_with_prefix_openfile(self, bucket, bucket_name, prefix, file_name):

		# storage_client = storage.Client()
		# bucket = storage_client.get_bucket(bucket_name)
		blob = bucket.blob(prefix+file_name)
		data = json.loads(blob.download_as_string(client=None))
		return data

	def download_blob(self, bucket_name, source_blob_name, destination_file_name):
	    """Downloads a blob from the bucket."""
	    # bucket_name = "your-bucket-name"
	    # source_blob_name = "storage-object-name"
	    # destination_file_name = "local/path/to/file"

	    storage_client = storage.Client()
	    bucket = storage_client.bucket(bucket_name)
	    blob = bucket.blob(source_blob_name)
	    blob.download_to_filename(destination_file_name)

	    # print(
	    #     "Blob {} downloaded to {}.".format(
	    #         source_blob_name, destination_file_name
	    #     )
	    # )

	def download_blob_openfile(self, bucket, bucket_name, source_blob_name, destination_file_name):
		"""Downloads a blob from the bucket."""
		# bucket_name = "your-bucket-name"
		# source_blob_name = "storage-object-name"
		# destination_file_name = "local/path/to/file"

		# storage_client = storage.Client()
		# bucket = storage_client.bucket(bucket_name)

		blob = bucket.blob(source_blob_name)
		blob.download_to_filename(destination_file_name)

		# print(
		#     "Blob {} downloaded to {}".format(
		#         source_blob_name, destination_file_name
		#     )
		# )

	def copy_blob(self, bucket, destination_bucket, source_blob_name, destination_blob_name):
		"""Copies a blob from one bucket to another with a new name."""
		# bucket_name = "your-bucket-name" storage_client.get_bucket(self.gcsDownloadBucketCode)
		# blob_name = "your-object-name"
		# destination_bucket_name = "destination-bucket-name"
		# destination_blob_name = "destination-object-name"

		source_blob = bucket.blob(source_blob_name)
		blob_copy = bucket.copy_blob(
			source_blob, destination_bucket, destination_blob_name
		)

		# print(
		# 	"Blob {} in bucket {} copied to blob {} in bucket {}.".format(
		# 		source_blob.name,
		# 		bucket.name,
		# 		blob_copy.name,
		# 		destination_bucket.name,
		# 	)
		# )

	def get_blob_image_width_height(self, bucket_name, source_blob_name):
		"""Downloads a blob from the bucket."""
		# bucket_name = "your-bucket-name"
		# source_blob_name = "storage-object-name"
		# destination_file_name = "local/path/to/file"

		storage_client = storage.Client()

		bucket = storage_client.bucket(bucket_name)
		blob = bucket.blob(source_blob_name)
		# blob.download_to_filename(destination_file_name)

		blob_in_bytes = io.BytesIO(blob.download_as_string())

		im = Image.open(blob_in_bytes)
		im_width, im_height = map(int, im.size)

		# print(str(im_width) + " | " + str(im_height))

		im.close()

		return im_width, im_height

	def upload_blob(self, bucket_name, source_file_name, destination_blob_name, user_email=None):
		"""Uploads a file to the bucket."""
		# bucket_name = "your-bucket-name"
		# source_file_name = "local/path/to/file"
		# destination_blob_name = "storage-object-name"
		storage_client = storage.Client()
		bucket = storage_client.bucket(bucket_name)
		blob = bucket.blob(destination_blob_name)

		blob.upload_from_filename(source_file_name)

		#인터넷 유저 읽기권한 부여한다.
		# TODO : 추후 고객사 또는 PM 구글 계정 정보를 받게 된다면 타겟유저로 권한을 부여하는게 좋은 방안이다.
		acl = blob.acl
		if user_email is None:
			acl.all().grant_read() # all user
		else:
			if isinstance(user_email, list):
				for mail in user_email:
					acl.user(mail).grant_read()  # user grant
			else:
				acl.user(user_email).grant_read() # user grant
		acl.save()
		print(
			"File {} uploaded to {}.".format(
				source_file_name, destination_blob_name
			)
		)