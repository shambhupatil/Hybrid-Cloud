import boto3
import face_recognition
import pickle
import numpy as np
import os
import csv
import logging
import json

input_bucket = "inputbucket"
output_bucket = "outputbucket"
def open_encoding(filename):
    file = open(filename, "rb")
    data = pickle.load(file)
    file.close()
    return data

enc_old = open_encoding('/home/app/function/encoding')
# enc_old = open_encoding('encoding')
arr_list_enco = enc_old['encoding']
name_list_enco = enc_old['name']
new_encoding_dict = {}

for i in range(len(arr_list_enco)):
    new_encoding_dict[tuple(arr_list_enco[i])] = name_list_enco[i]


# Set region and credentials
region = 'us-east-1'
aws_access_key_id = "AKIAXF3TLYDM2OOXG3VR"
aws_secret_access_key = "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"

# Create a DynamoDB client with region and credentials
dynamodb = boto3.client('dynamodb', region_name=region, aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)
rados_url='http://10.0.2.15:8000'
s3 = boto3.resource('s3',endpoint_url=rados_url,aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)

def handle(event, context):
	
    data=json.loads(event.body)
    s3_bucket = data['Records'][0]['s3']['bucket']['name']
    s3_key = data['Records'][0]['s3']['object']['key']
    path = "/tmp/"
    video_path = os.path.join(path, s3_key.split("/")[-1])
    path = "/tmp/"
    video_path = os.path.join(path, s3_key.split("/")[-1])
    s3.Bucket('inputbucket').download_file(s3_key,video_path)
    os.system("ffmpeg -i " + video_path + " -r 1 " + path + "image-%3d.jpeg")
    
    
    
    for filename in os.listdir(path):
        if filename.startswith('image-') and filename.endswith('.jpeg'):
            image = face_recognition.load_image_file(f"/tmp/{filename}")
			
            face_encoding = face_recognition.face_encodings(image)
            print(face_encoding)
            if face_encoding:
             # Assuming you want the first encoding
             	
                break
    
	
    matches = face_recognition.compare_faces(list(new_encoding_dict.keys()), np.array(face_encoding))
    for i in matches:
        if i == True:
            name_of_face = new_encoding_dict[list(new_encoding_dict.keys())[matches.index(True)]]
            break
    
    expression_attribute_names = {'#n': 'name'}
    key_condition_expression = '#n = :val'
    expression_attribute_values = {':val': {'S': name_of_face}}
    
    db_response = dynamodb.query(
        TableName='project2-table',
        IndexName='name-index',
        KeyConditionExpression=key_condition_expression,
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values
    )
    
    with open('/tmp/dynamodb_query_results.csv', mode='w', newline='') as csv_file:
    
    	csv_writer = csv.writer(csv_file)
    
    	for item in db_response['Items']:
    	    name = item['name']['S']
    	    major = item['major']['S']
    	    year = item['year']['S']
    	    csv_writer.writerow([name, major, year])
    	object_key=s3_key.replace('.mp4','.csv')    
	
	#with open('/tmp/dynamodb_query_results.csv', 'rb') as f:
    s3.Bucket('outputbucket').upload_file('/tmp/dynamodb_query_results.csv', object_key)
    
    for filename in os.listdir(path):
        if filename.endswith('.jpeg') or filename.endswith('.csv'):
            os.remove(os.path.join(path, filename))
    
    
    return {
        "statusCode": 200,
        "body": "hello 50"
    }
