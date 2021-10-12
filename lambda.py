from urllib.request import urlopen
import boto3
import uuid
import json
import urllib
import os
import requests

# This file converts a file (image or video) from AWS S3, to text. 
# Splits docName with "." and then checks what the file format is and based on that, either calls
# textract (for converting image to text) or AWS Transcribe (for converting video to text)
# It uses Python File system (read/write operations) to read the data from the file
# and output it to a text file.
# This data form the text file is again parsed and sent to Node.js instance which is hosted
# on AWS EC2 (link given in url variable - line84)
# This file is present in AWS Lambda (Python 3.7 runtime). 
# We need to Test this file to see the output, so that whenever AWS S3 gets any file in its bucket,
# that file automatically goes through this program and generates the output accordingly.
# Before we Test the file, for every change, we need to save and hit the Deploy Changes button. 

# This file will create a jobName (variable below) which is a Transcription Job in AWS Transcribe. 
# Transcribe will analyze this file in realtime and generate the transcript.


def lambda_handler(event, context):

    s3bucket = event['Records'][0]['s3']['bucket']['name']
    s3object = event['Records'][0]['s3']['object']['key']
    
    s3Path = "s3://" + s3bucket + "/" + s3object
    jobName = s3object + '-' + str(uuid.uuid4())
    
    docName = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    fileName = docName.split(".")[1]
    
    if fileName == 'jpg' or fileName == 'png' or fileName =='jpeg':
        f = open("/tmp/subs.txt", "w")
        f.truncate(0)
        textract = boto3.client('textract')
         # Call Amazon Textract
        response = textract.detect_document_text(
            Document={
                'S3Object': {
                    'Bucket': s3bucket,
                    'Name': docName
                }
            })
        for item in response["Blocks"]:
            if item["BlockType"] == "LINE":
                #print(item["Text"])
                f.write(item["Text"])
        f.close()        
        with open("/tmp/subs.txt", "r") as g:    
            #print(g.read())
            output_file = g.read()
            print("Entered")        
        
    else:
        f = open("/tmp/subs.txt", "w")
        f.truncate(0)
        client = boto3.client('transcribe')

        response = client.start_transcription_job(
            TranscriptionJobName=jobName,
            LanguageCode='en-US',
            MediaFormat=fileName,
            Media={
                'MediaFileUri': s3Path
            },
            #OutputBucketName = "transcribe-output79"
        )
        
        while True:
            status = client.get_transcription_job(TranscriptionJobName=jobName)
            if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
                print("Completed!")
                break
            print("Not ready yet...")
        uri = status['TranscriptionJob']['Transcript']['TranscriptFileUri']
        res = urlopen(uri)
        data_json = json.loads(res.read())
        f.write(data_json['results']['transcripts'][0]['transcript'])
        f.close()
        with open("/tmp/subs.txt", "r") as g:
            #print(g.read());
            output_file = g.read()
        
        return {
        'TranscriptionJobName': response['TranscriptionJob']['TranscriptionJobName']
        }
        
    url = "http://ec2-18-117-151-130.us-east-2.compute.amazonaws.com:3000/upload"
 
    r = requests.post(url,{'filename': output_file})
    print(r.text)