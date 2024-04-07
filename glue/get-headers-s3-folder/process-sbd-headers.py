# ATTENTION: Add Pandas as a Lambda layer!

# Import required libraries
import json
import boto3
import csv
import time
import os
import io
import gzip
import pandas as pd # ATTENTION: Add Pandas as a Lambda layer!
from urllib.parse import unquote_plus

# Create AWS clients for S3 and SQS
s3_client           = boto3.client("s3")
sqs_client          = boto3.client("sqs")

DESTINATION_BUCKET  = '<BUCKET_NAME>'
SQS_URL             = '<SQS_URL>'

# Main function. Do not change the name or parameters!
def lambda_handler(event, context):
    print("===> Event")
    print(event)
    print("===> Context")
    print(context)
    
    try:        
        # The event parameter comes with an array of elements inside "Records"
        for record in event["Records"]:
            # Save the receipt handle to later delete the SQS message
            incoming_sqs_message_receipt_handle = record["receiptHandle"]
            
            # Decode the body of the message and retrieve the names of the bucket and key
            jsonmaybe = (record["body"])
            jsonmaybe = json.loads(jsonmaybe)
            bucket_name = jsonmaybe["Records"][0]["s3"]["bucket"]["name"]
            object_key = jsonmaybe["Records"][0]["s3"]["object"]["key"]
            
            obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)        
            data = pd.read_csv(obj['Body'], nrows=0, compression='gzip')            
            
            # Set the name of the new file with the csv extension
            csv_filename = "PRODHEADERS/" + object_key + "-headers.csv"            
            
            print("===> New file: "+csv_filename)
            print("===> Columns found: "+str(len(data.columns)))
            
            # Write the new file in the destination bucket with the defined new filename. The file contents will be the headers and the contents of the source file
            write_file(data, object_key, DESTINATION_BUCKET, csv_filename)

            # Delete the SQS message that triggered this execution
            sqs_client.delete_message(
                QueueUrl=SQS_URL,
                ReceiptHandle=incoming_sqs_message_receipt_handle
            )
            
        return {
            'statusCode': 200,
            'body': json.dumps('Headers extracted')
        }
        
    except Exception as e:
        print(str(e))
        return {
            'statusCode': 500,
            'body': str(e)
        }



def write_file(data, filename, s3_bucket, csv_filename):    
    data_array = '-'.join(data.columns.to_numpy())
    header = ['filename', 'columnnames', 'columncount']
    record = [filename, data_array, str(len(data.columns))]
    
    with open('/tmp/temp_file.csv', 'w', newline='') as f:
        w = csv.writer(f)    
        w.writerow(i for i in header)
        w.writerow(record)
    
    s3_client.upload_file('/tmp/temp_file.csv', s3_bucket, csv_filename)
    print("===> FILE WRITTEN")
    

    