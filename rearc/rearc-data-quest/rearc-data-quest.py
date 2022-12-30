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
    BLS_BASE_URL = os.getenv("BLS_BASE_URL")
    rearc_bucket = os.getenv("REARC_BUCKET")
    html_elements = []
    bls_dirs = ['pr']
    file_paths = []
    urls = []
    s3_client = boto3.client('s3')
    
    try:
        requests_session = requests.Session()
        # response = requests_session.get(BLS_BASE_URL + bls_dirs[0] +'/')
        # response = requests_session.get(BLS_BASE_URL + "bd" +'/')
        response = requests_session.get(BLS_BASE_URL + "pr" +'/')
        if response.status_code == 200:
            print(response.text) 
            print("")
            
            for html_element in response.text.split(' <A'):
                # print(html_element)
                html_elements.append(html_element)
                if html_element.startswith(" HREF"):
                    file_path = html_element[html_element.find("time.series/") + len("time.series/") : html_element.find('">')]
                    file_paths.append(file_path)
                    # print(file_path) 
                    urls.append(BLS_BASE_URL + file_path)
                    #  HREF="/pub/time.series/pr/pr.class">pr.class</A><br>
                    
            print("html_elements")        
            pp.pprint(html_elements)
            print("file_paths:")
            pp.pprint(file_paths)
            print("urls:")
            pp.pprint(urls) 
            print("len of file_paths:", len(file_paths))
            
        else:  
            return "Error when requesting page source"
            
    except Exception as e:
        traceback.print_exc()
        # exception_message = "Error"
        logger.error(e)
        return e
         
        
        """
         Iterate over each pr file:
          if it doesn't exist in the s3 bucket, then download to memory, upload to s3, update dynamodb with metadata. 
          if it exists in s3 bucket, then check metadata in dynamodb table, if different, then replace s3 file and update metadata 
         Then iterate over each s3 file:
          if it doesn't exist on server, then remove it from s3 and delete the metadata.
        """
    try:
        for i in range(len(file_paths)):
            file_key = file_paths[i]
            file_url = urls[i]
            response = s3_client.head_object(Bucket=rearc_bucket, Key=file_key)
            print(file_key, response)
            
    except Exception as e: #file doesn't exist
        logger.error("file_key does not exist in bucket:")
        
        # download to memory
        http = urllib3.PoolManager()
        download_response = http.request("GET", file_url, decode_content=True)
        print(file_key, file_url)
        
        if download_response.status == 200:
            print("status:", download_response.status)
            open("/tmp/" + file_key[file_key.find("/"):] , "wb").write(download_response.data)
            lst = os.listdir("/tmp")
            logger.info(f"Download file to /tmp: {lst}")
        else: return "Something went wrong when downloading file to /tmp, response status: " + response.status
        
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
