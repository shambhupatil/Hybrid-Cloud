import requests
import boto3
import time
import csv
aws_access_key_id = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
aws_secret_access_key = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"


rados_url='http://127.0.0.1:8000'
s3 = boto3.resource('s3',endpoint_url=rados_url,aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)

input_bucket=s3.Bucket('inputbucket')
output_bucket=s3.Bucket('outputbucket')

images=set()
while True:
	for obj in input_bucket.objects.all():
		if obj.key not in images:
			images.add(obj.key)
			
			
			url = "http://192.168.49.2:31112/function/ccproject"
			headers = {'Content-Type': 'application/json'}
			data = '{"Records": [{"s3": {"bucket": {"name": "inputbucket"}, "object": {"key":"'+obj.key+'"}}}]}'

			response = requests.post(url, headers=headers, data=data)

			
			out_key=obj.key.replace('.mp4','.csv')
			output_bucket.download_file(out_key,out_key)
			with open(out_key,'r') as f1:
				csv_reader=csv.reader(f1)
				for row in csv_reader:
					curr=row
			
			
			print("Response Code:", response.status_code," for ", obj.key, "Result:",curr)	
			
			

