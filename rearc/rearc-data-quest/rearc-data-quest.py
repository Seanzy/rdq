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
    
    # print(event)
    # logger.info(event)
    
    http = urllib3.PoolManager()
    
    s3_client = boto3.client('s3')
    dynamodb_client = boto3.client('dynamodb')
    rearc_table = boto3.resource("dynamodb").Table(os.getenv("REARC_TABLE"))
    
    BASE_URL = os.getenv("BASE_URL")
    REARC_BUCKET = os.getenv("REARC_BUCKET")
    REARC_TABLE = os.getenv("REARC_TABLE")
    
    folders = ['eb']
    site_files = []
    urls = [] 
    file_metadata = []
    s3_file_keys = []
    
    try:
        # Parse page source for filenames and their metadata 
        requests_session = requests.Session()
        response = requests_session.get(BASE_URL + folders[0] +'/')
        if response.status_code == 200:
            
            # Add date and time metadata to file_metadata, first element is slightly out of place so check for </A><br><br> instead of </A><br>
            for html_element in response.text.split(' <A'):
                # logger.info(html_element)
                if "</A><br><br>" in html_element:
                    file_metadata.append(html_element[html_element.find("</A><br><br>") + len("</A><br><br>") : ])
                else:
                    file_metadata.append(html_element[html_element.find("</A><br>") + len("</A><br>") : ])
                    
                if html_element.startswith(" HREF"):
                    site_file = html_element[html_element.find("time.series/") + len("time.series/") : html_element.find('">')]
                    site_files.append(site_file)
                    urls.append(BASE_URL + site_file)
        else:  
            return "Error when requesting page source"

        # print("site_files:")
        # pp.pprint(site_files)
        logger.info("site_files: %s", site_files)
        logger.info("Number of site_files: %s", len(site_files))

        # print("urls:")
        # pp.pprint(urls)
        logger.info("urls: %s", urls)
        
        # print("file_metadata:")
        # pp.pprint(file_metadata)
        logger.info("file_metadata: %s", file_metadata)
        logger.info("Length of file_metadata: %s", len(file_metadata)) 
        # print(response.text)
            
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
        
        
    # client.list_objects_vs() return value will not contain "Contents" key if 2-letter folder does not exist, so we create it in that case and log a warning
    if "Contents" not in s3_client.list_objects_v2(Bucket=REARC_BUCKET, Prefix=folders[0]).keys():
        try:
            s3_client.put_object(Bucket=REARC_BUCKET, Key=(folders[0] + '/'))
            logger.warning("Folder %s does not exist in bucket %s, created it", folders[0], REARC_BUCKET)
            s3_files = []
            # s3_file_keys = []
        except: 
            logger.error("Unable to create folder %s in bucket", folders[0])
    
    # else, if 2-letter folder exists, then list its files
    else: 
        s3_files = s3_client.list_objects_v2(Bucket=REARC_BUCKET, Prefix=folders[0])['Contents']
        # s3_file_keys = []

        for s3_file in s3_files:
            s3_file_keys.append(s3_file['Key'])

        logger.info("List files in S3 bucket %s, folder %s: %s", REARC_BUCKET, folders[0], s3_file_keys)
        # logger.info("s3_files: %s", s3_files)
        # logger.info("s3_file_keys: %s", s3_file_keys)
    

    # check which files from the site exist in the bucket, if it doesn't, then download it to /tmp
    if len(s3_file_keys) >= 1:  
        for i in range(len(site_files)):
            
            if site_files[i] not in s3_file_keys:

            # Create DynamoDB item with website file metadata (dir, file, date, size(bytes), time) for files that don't exist in S3 bucket 
                item = {
                    'dir':folders[0], 
                    'file':site_files[i], 
                    'date':file_metadata[i].split()[0], 
                    'time':file_metadata[i].split()[1] + " " + file_metadata[i].split()[2],
                    'size(bytes)': file_metadata[i].split()[3]
                }
                item_response = rearc_table.put_item(Item=item)
                

                # download to /tmp
                http = urllib3.PoolManager()
                download_response = http.request("GET", urls[i], decode_content=True)
                logger.info("Upload %s to bucket", urls[i])
                
                if download_response.status == 200:
                    # print("data", download_response.data)
                    # logger.info("Status: %s", download_response.status)
                    open("/tmp/" + site_files[i][site_files[i].find("/"):], "wb").write(download_response.data)
                    lst = os.listdir("/tmp")
                    logger.info("Files downloaded to /tmp: %s", lst)
                    
                    # upload from /tmp to s3 and update dynamodb metadata 
                    with open("/tmp/" + site_files[i][site_files[i].find("/"):], "rb") as f:
                        s3_client.upload_fileobj(f, REARC_BUCKET, folders[0] + site_files[i][site_files[i].find("/"):] )
                        # logger.info("File uploaded to bucket")
                        rearc_table.put_item(Item=item)
                    # else: 
                    #     logger.warning("Problem downloading %s, status code: %s", download_response.status)
                
        
        # write to s3 from /tmp 
    
        # Else if website file (source) exists in S3, then check if source metadata differs from S3 file metadata in DynamoDB and reupload source to S3 if it does
            else: 
                uploaded_files = []
                
                source = {
                    'dir':folders[0], 
                    'file':site_files[i], 
                    'date':file_metadata[i].split()[0], 
                    'time':file_metadata[i].split()[1] + " " + file_metadata[i].split()[2],
                    'size(bytes)': file_metadata[i].split()[3]
                }
                
                item_response = rearc_table.get_item(TableName=REARC_TABLE, Key={'dir': folders[0], 'file': site_files[i]})
                s3_metadata = item_response['Item']
                
                # logger.info(s3_metadata)
                # # logger.info("Compare source and S3 file metadata")
                
                if source['date'] != s3_metadata['date'] or source['time'] != s3_metadata['time'] or source['size(bytes)'] != s3_metadata['size(bytes)']:
                    
                    # download to source to /tmp then upload to S3
                    http = urllib3.PoolManager()
                    download_response = http.request("GET", urls[i], decode_content=True)
                    logger.info("%s source metadata differs from destination S3 metadata", site_files[i])
                    logger.info("Upload %s to bucket and update its S3 metadata in DynamoDB", urls[i])
                    
                    if download_response.status == 200:
                        # print("data", download_response.data)
                        # logger.info("status: %s", download_response.status)
                        # print(site_files[i][site_files[i].find("/") + 1 :])
                        open("/tmp/" + site_files[i][site_files[i].find("/") :], "wb").write(download_response.data)
                        lst = os.listdir("/tmp")
                        logger.info("Files downloaded to /tmp: %s", lst)    
                        
                        with open("/tmp" + site_files[i][site_files[i].find("/") :], "rb") as f:
                            s3_client.upload_fileobj(f, REARC_BUCKET, folders[0] + site_files[i][site_files[i].find("/"):] )
                            # logger.info("%s uploaded to bucket", site_files[i][site_files[i].find("/") + 1 :])
                            rearc_table.put_item(Item=source)
                    else: 
                        logger.warning("Problem downloading %s, status code: %s", download_response.status)
                        lst = os.listdir("/tmp")
                        logger.info("Files downloaded to /tmp: %s", lst)    
                
                # upload from /tmp to s3 and update dynamodb metadata 
                
        # Check for files in S3 not on the website and remove them from S3. 
                
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
