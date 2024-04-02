'''
    Telma Frege (tfrege@gmail.com)
    
    This script takes a list of patterns to look for in the table names and deletes them.
    The max number of tables to be retrieved for each pattern is 100 (AWS API's get_tables() hard limit). To work around this, you can add the same pattern multiple times on the TABLE_NAME_PATTERNS list.
    Requisites:
    1. If running this from AWS Lambda: verify the function has a good timeout (not the default 3 secs)
    2. The Lambda role should have the following permissions: 
    3. Have logging enabled (i.e. a CloudWatch log if running this as a Lambda function)
'''

import json
import boto3
import botocore

glue_client  = boto3.client('glue')

GLUE_DATA_CATALOG = 'AwsDataCatalog'
GLUE_DB_NAME = 'mygluedb'
TABLE_NAME_PATTERNS = ['pattern1_', '_pattern2_']

def verify_tables(db_name):
    response = []
    try:
        for pattern in TABLE_NAME_PATTERNS:
            print("SEARCHING TABLES WITH PREFIX: " + pattern)
            response = glue_client.get_tables(DatabaseName=db_name, MaxResults=100, Expression='*['+pattern+']*')
            print("FOUND A TOTAL OF " + str(len(response["TableList"])))
    except botocore.exceptions.ClientError as err:
        print(
            "Couldn't get tables %s. Here's why: %s: %s",
            db_name,
            err.response["Error"]["Code"],
            err.response["Error"]["Message"],
        )
        raise

    return response
    
def delete_tables(db_name):
    response = []
    try:
        for pattern in TABLE_NAME_PATTERNS:
            print("SEARCHING TABLES WITH PREFIX: " + pattern)
            response = glue_client.get_tables(DatabaseName=db_name, MaxResults=100, Expression='*['+pattern+']*')
            print("FOUND A TOTAL OF " + str(len(response["TableList"])))
            count = 0
            for table in tables:
                delete_table(table['Name'])
    except botocore.exceptions.ClientError as err:
        print(
            "Couldn't get tables %s. Here's why: %s: %s",
            db_name,
            err.response["Error"]["Code"],
            err.response["Error"]["Message"],
        )
        raise

    return response

def delete_table(table_name):
    print("DELETING... " + table_name)
    glue_client.delete_table(
        DatabaseName=GLUE_DB_NAME,
        Name=table_name
    )
    
def lambda_handler(event, context):
  # Recommended: first run verify_tables to make sure the code is selecting the tables you want to delete, then uncomment delete_tables()
  verify_tables(GLUE_DB_NAME)
   # delete_tables(GLUE_DB_NAME)
    
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Done!')
    }
