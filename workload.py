import boto3 

import os

test_cases = "test_cases/"

aws_access_key_id = "xxxxxxxxxxxxxxxxxxxxxxxxx"
aws_secret_access_key = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"


rados_url='http://127.0.0.1:8000'
s3 = boto3.resource('s3',endpoint_url=rados_url,aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)

def clear_input_bucket():
	input_bucket = s3.Bucket('inputbucket')
	try:
		for item in input_bucket.objects.all():
			item.delete()
	except:
		print("Nothing to clear in input bucket")
	
def clear_output_bucket():
	output_bucket = s3.Bucket('outputbucket')
	try:
		for item in output_bucket.objects.all():
			item.delete()
	except:
		print("Nothing to clear in output bucket")

def upload_to_input_bucket_s3(path, name):
	
	s3.Bucket('inputbucket').upload_file(path + name, name)
	
	
def upload_files(test_case):	

	global test_cases
	
	
	# Directory of test case
	test_dir = test_cases + test_case + "/"
	
	# Iterate over each video
	# Upload to S3 input bucket
	for filename in os.listdir(test_dir):
		if filename.endswith(".mp4") or filename.endswith(".MP4"):
			print("Uploading to input bucket..  name: " + str(filename)) 
			upload_to_input_bucket_s3(test_dir, filename)
			
	
def workload_generator():
	
	#print("Running Test Case 1")
	#upload_files("test_case_1")

	print("Running Test Case 2")
	upload_files("test_case_2")
	

clear_input_bucket()
clear_output_bucket()	
workload_generator()	

	
