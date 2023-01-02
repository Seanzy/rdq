import json
import logging
import boto3
import os
import traceback
import urllib3
import requests
import pprint as pp
from requests.adapters import HTTPAdapter, Retry

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context): 
    
    print(event)
    logger.info(event)
    
    http = urllib3.PoolManager()
    s3_client = boto3.client('s3')
    BASE_URL = os.getenv("BASE_URL")
    REARC_BUCKET = os.getenv("REARC_BUCKET")
    REARC_TABLE = os.getenv("REARC_TABLE")
    folders = ['pr']
    html_elements = []
    site_files = []
    urls = []

    
    try:
        requests_session = requests.Session()
        response = requests_session.get(BASE_URL + folders[0] +'/')
        if response.status_code == 200:
            # print(response.text) 
            
            for html_element in response.text.split(' <A'):
                html_elements.append(html_element)
                if html_element.startswith(" HREF"):
                    site_file = html_element[html_element.find("time.series/") + len("time.series/") : html_element.find('">')]
                    site_files.append(site_file)
                    urls.append(BASE_URL + site_file)
                    
            # print("html_elements")        
            # pp.pprint(html_elements)
            # print("site_files:")
            # pp.pprint(site_files)
            # print("\n")
            # print("urls:")
            # pp.pprint(urls)
            # print("\n")
            # print("Number of site_files:", len(site_files))
            
        else:  
            return "Error when requesting page source"
            
    except Exception as e:
        traceback.print_exc()
        logger.error(e)
        return e
         
        
        """
         Iterate over each pr file:
          if it doesn't exist in the s3 bucket, then download to memory, upload to s3, update dynamodb with metadata. 
          if it exists in s3 bucket, then check metadata in dynamodb table, if different, then replace s3 file and update metadata 
         Then iterate over each s3 file:
          if it doesn't exist on server, then remove it from s3 and delete the metadata.
        """
        
    # if 2-letter folder does not exist, then create it
    if "Contents" not in s3_client.list_objects_v2(Bucket=REARC_BUCKET, Prefix=folders[0]).keys():
        try:
            s3_client.put_object(Bucket=REARC_BUCKET, Key=(folders[0] + '/'))
            logger.warning("Folder %s does not exist in bucket %s, created it", folders[0], REARC_BUCKET)
        except: 
            logger.error("Unable to create folder %s in bucket", folders[0])
    
    # else, if 2-letter folder exists, then list its files
    else: 
        s3_files = s3_client.list_objects_v2(Bucket=REARC_BUCKET, Prefix=folders[0])['Contents']
        s3_file_keys = []

        for s3_file in s3_files:
            s3_file_keys.append(s3_file['Key'])

        logger.info("List files in bucket %s:", REARC_BUCKET)
        # logger.info(s3_file_keys, "\n")
    

    # check which files from the site exist in the bucket
    if len(s3_files) >= 1: 
        for i in range(len(site_files)):
            
            if site_files[i] not in s3_files:
            # response = s3_client.head_object(Bucket=REARC_BUCKET, Key=file_key)
            # if s3_client.head_object(Bucket=REARC_BUCKET, Key=file_key).status >= 400:
                # logger.info("Upload %s", site_files[i])
            # print(file_key, response)

            # Create DynamoDB metadata for files that don't exist in S3 bucket (size, date, time)
                dyn_client = boto3.client('dynamodb')
                rearc_table = boto3.resource("dynamodb").Table(os.getenv("REARC_TABLE"))
                item = {'dir': "pr", 'file': "pr.contactss"}
                item_resonse = rearc_table.put_item(Item=item)
                
                # dyn_response = dyn_client.update_item(TableName='REARC_TABLE', Key={"dir": "pr", "file": "pr.contacts"}, UpdateExpression="set info.date=:r, info.size(bytes)=:p",ExpressionAttributeValues={':r': "9/13/2022", ':p': 562}, ReturnValues="UPDATED_NEW"),
                    
                # download to /tmp
                # http = urllib3.PoolManager()
                # download_response = http.request("GET", urls[i], decode_content=True)
                # logger.info("upload %s to bucket", urls[i])
                
                # if download_response.status == 200:
                #     # print("data", download_response.data)
                #     logger.info("status: %s", download_response.status)
                #     # open("/tmp/" + file_key[file_key.find("/"):] , "wb").write(download_response.data)
                #     open("/tmp/" + site_files[i][site_files[i].find("/"):], "wb").write(download_response.data)
                #     lst = os.listdir("/tmp")
                # else: 
                #     logger.warning("Problem downloading %s, status code: %s", download_response.status)
                
        # logger.info("Files downloaded to /tmp: %s", lst)
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
