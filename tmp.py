from google.cloud import storage
import os


"""Lists all the blobs in the bucket."""
bucket_name = 'cw_platform'

storage_client = storage.Client()
import re
blobs = storage_client.list_blobs(bucket_name, prefix='538/9498_content/_content_data', delimiter=None)
blobs2 = storage_client.list_blobs(bucket_name, prefix='538/9315_content/_content_data', delimiter=None)
print("Blobs:")
lst = []
lst2 =[]
for blob in blobs:
    lst.append(os.path.basename(blob.name))
print(len(lst))
for blob in blobs2:
    lst2.append(os.path.basename(blob.name))
print(len(lst2))
s1 = set(lst)
print(len(s1))
s2 = set(lst2)
s3 = s1 - s2
print(s3)