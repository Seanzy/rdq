import json
import logging
import boto3
import os
import traceback
import urllib3
import math 
from io import StringIO
import pprint as pp
from botocore.exceptions import ClientError
from datetime import datetime, timezone

# LAMBDA LAYER LIBRARIES:
import requests
import pandas as pd
# from pyspark.sql.functions import trim



logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s')

def lambda_handler(event, context): 
    
    print(event)
    logger.info(event)
    
    # PART 1
    http = urllib3.PoolManager()
    
    BASE_URL = os.getenv('BASE_URL')
    CSV_URL = os.getenv('CSV_URL')
    API_URL = os.getenv('API_URL')
    REARC_BUCKET = os.getenv('REARC_BUCKET')
    REARC_TABLE = os.getenv('REARC_TABLE')
    AWS_REGION = os.getenv('AWS_REGION')
    QUEUE_URL = os.getenv('QUEUE_URL')

    s3_client = boto3.client('s3')
    sqs_client = boto3.client('sqs', region_name=AWS_REGION)
    dynamodb_client = boto3.client('dynamodb')
    rearc_table = boto3.resource('dynamodb').Table(os.getenv('REARC_TABLE'))
    
    folders = ['pr', 'api']
    site_files = []
    urls = [] 
    file_metadata = []
    s3_file_keys = []
    uploaded_to_s3 = []
    
    msg_attributes = {}
    msg_body = ''


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
        # logger.info("site_files: %s", site_files)
        logger.info("Number of site_files: %s", len(site_files))

        # print("urls:")
        # pp.pprint(urls)
        logger.info("urls: %s", urls)
        
        # print("file_metadata:")
        # pp.pprint(file_metadata)
        
        # logger.info("Remove extraneous list item from file_metadata: %s", file_metadata.pop())
        file_metadata.pop()
        # logger.info("file_metadata: %s", file_metadata)
        # logger.info("Length of file_metadata: %s", len(file_metadata)) 
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
        
    # If 2-letter folder (folders[0]) does not exist in S3, then create it
    if "Contents" not in s3_client.list_objects_v2(Bucket=REARC_BUCKET, Prefix=folders[0]).keys():
        try:
            s3_client.put_object(Bucket=REARC_BUCKET, Key=(folders[0] + '/'))
            logger.warning("Folder %s does not exist in bucket %s, created it", folders[0], REARC_BUCKET)
            s3_files = s3_client.list_objects_v2(Bucket=REARC_BUCKET, Prefix=folders[0])['Contents']
            
            for s3_file in s3_files:
                s3_file_keys.append(s3_file['Key'])
        except: 
            logger.error("Unable to create folder %s in bucket", folders[0])
    
    # Else, if 2-letter folder exists, then define a list of its files
    else: 
        s3_files = s3_client.list_objects_v2(Bucket=REARC_BUCKET, Prefix=folders[0])['Contents']

        for s3_file in s3_files:
            s3_file_keys.append(s3_file['Key'])

        logger.info("LIST files in S3 bucket %s, folder %s: %s", REARC_BUCKET, folders[0], s3_file_keys)


    # Check which files from source site do not exist in S3 bucket and download them to /tmp
    if len(s3_file_keys) >= 1:  
        for i in range(len(site_files)):
            site_file_key = site_files[i][site_files[i].find("/"):]
            
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
                
                # Download to /tmp
                http = urllib3.PoolManager()
                download_response = http.request("GET", urls[i], decode_content=True)
                
                if download_response.status == 200:
                    # logger.info("Status: %s", download_response.status)
                    open("/tmp/" + site_file_key, "wb").write(download_response.data)
                    lst = os.listdir("/tmp")
                    
                    # Upload from /tmp to S3 and update dynamodb metadata 
                    with open("/tmp/" + site_file_key, "rb") as f:
                        s3_client.upload_fileobj(f, REARC_BUCKET, folders[0] + site_file_key )
                        uploaded_to_s3.append(site_file_key)
                        logger.info("Upload %s to bucket", urls[i])
                        rearc_table.put_item(Item=item)
                        # send_queue_message(QUEUE_URL, msg_attributes, msg_body)
                        # logger.info("SQS message: %s", receive_queue_message(QUEUE_URL))
                
                else: 
                    logger.warning("Problem downloading %s, status code: %s", site_file_key, download_response.status)
                
        
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
                
                # Compare soure metadata to S3 metadata 
                if source['date'] != s3_metadata['date'] or source['time'] != s3_metadata['time'] or source['size(bytes)'] != s3_metadata['size(bytes)']:
                    
                    # download from source to /tmp then upload to S3
                    http = urllib3.PoolManager()
                    download_response = http.request("GET", urls[i], decode_content=True)
                    logger.info("%s source metadata differs from destination S3 metadata", site_files[i])
                    logger.info("Upload %s to bucket and update its S3 metadata in DynamoDB", urls[i])
                    
                    if download_response.status == 200:
                        open("/tmp/" + site_files[i][site_files[i].find("/") :], "wb").write(download_response.data)
                        lst = os.listdir("/tmp")
                        logger.info("Files downloaded to /tmp: %s", lst)    
                        
                        with open("/tmp" + site_files[i][site_files[i].find("/") :], "rb") as f:
                            s3_client.upload_fileobj(f, REARC_BUCKET, folders[0] + site_file_key )
                            rearc_table.put_item(Item=source)
                            uploaded_to_s3.append(site_file_key)
                        
                    else: 
                        logger.warning("Problem downloading %s, status code: %s", download_response.status)
                        lst = os.listdir("/tmp")
                        logger.info("Files downloaded to /tmp: %s", lst)    
                 
        # this logger.infodoesn't show all the files uploaded  
        # logger.info("Files uploaded to S3: %s", uploaded_to_s3)
                        
    # Check for files in S3 not on the website and remove them from S3
    s3_files_to_be_synced = s3_client.list_objects_v2(Bucket=REARC_BUCKET, Prefix=folders[0])['Contents']
    
    # try:
    for file_to_be_synced in s3_files_to_be_synced:
        if file_to_be_synced['Key'] == folders[0] + "/":
            continue
        
        if file_to_be_synced['Key'] not in site_files:
            logger.info("Remove %s from S3 bucket to sync with source", file_to_be_synced['Key'])
            logger.info("Removed file details: %s", file_to_be_synced)
            delete_response = s3_client.delete_object(Bucket=REARC_BUCKET, Key=file_to_be_synced['Key'])
            
        else:
            logger.error("Exiting loop")
            break
    
    # PART 2
    http_api = urllib3.PoolManager()
    s3_client_api = boto3.client('s3')
    API_URL = os.getenv("API_URL")
    api_file_key = "api_data.json"

    
    logger.info("FETCH API data from %s", API_URL)
    try:
        api_response = http_api.request("GET", API_URL, decode_content=True)
        logger.info("REPORT api_response.status: %s", api_response.status)
    except Exception as e:
        traceback.print_exc()
        logger.error(e)
        return e
    
    if api_response.status == 200:
        try:
            logger.info("WRITE %s to /tmp", api_file_key)
            open("/tmp/" + api_file_key, "wb").write(api_response.data)
            lst = os.listdir("/tmp")
        except Exception as e:
            traceback.print_exc()
            logger.error(e)
            return e

    if len(lst):
        logger.info("LIST files in /tmp: %s", lst)
        try:         
            with open("/tmp/" + api_file_key, "rb") as f:
                s3_client.upload_fileobj(f, REARC_BUCKET, folders[1] + "/" + api_file_key)
                logger.info("UPLOAD %s/api/%s", REARC_BUCKET, api_file_key)
        except Exception as e:
            traceback.print_exc(e)
            logger.error(e)
            return e

        logger.info("LIST %s bucket contents: %s", REARC_BUCKET, s3_client.list_objects_v2(Bucket=REARC_BUCKET, Prefix=folders[1])['Contents'][1])
    
    
    # PART 3
    # 3.0 LOAD PART 1 AND PART 2 AS DATAFRAMES (df_1 and df_2)
    # df_1: 
    csv_file_key = "pr.data.0.Current"
    object_key = folders[0] + "/" + csv_file_key

    csv_response = http.request('GET', CSV_URL, decode_content=True)
    open("/tmp/" + csv_file_key, "wb").write(csv_response.data)
    
    lst = os.listdir("/tmp")
    logger.info("LIST files in /tmp: %s", lst)

    # remove tabs
    file_headers = [ "series_id", "year", "period", "value", "footnote_codes" ]
    df_1 = pd.read_csv("/tmp/" + csv_file_key, sep='\t', skiprows=(1), names=file_headers)
    
    # print(df_1)
    
    
    # df_2:
    dict_2 = json.loads(api_response.data)
    df_2 = pd.DataFrame(dict_2['data'])
    # print(df_2)
    
    
    # 3.1 GENERATE POPULATION STATS mean and stdev.s
    df_2.loc[2013 <= df_2['ID Year'], 'Population'].mean()
    mean = df_2.loc[2:7, 'Population'].mean()
    stdev = df_2.loc[2:7, 'Population'].std()
    
    logger.info("GENERATE STATS: mean: %s, stdev.s: %s", mean, stdev)
    

    # 3.2 FIND BEST YEAR
    # Convert df_1 into pivot table:
    df_1_pivot = df_1.pivot_table(index = ["year", "series_id", "period"], values=["value"])
    # logger.info(df_1_pivot)
    
    # Remove Q05 rows from pivot table
    df_1_clean = df_1.loc[df_1["period"] != "Q05"]
    # print(df_1_clean)
    
    # Validate number of removed rows with Excel
    # Note: first column of df_1_clean is not the correct row number because rows were removed from the original dataframe df_1
    # print(f"df_1.shape: {df_1.shape}, df_1_clean.shape: {df_1_clean.shape}, confirmed via Excel 7569 rows deleted")
    
    # Reformat pivot table
    df_1_clean_pivot = df_1_clean.pivot_table(index=["year", "series_id", "period"], values=["value"])
    # print(df_1_clean_pivot)

    # Sum values for all quarters, groupby series_id, year
    df_1_final = df_1_clean_pivot.pivot_table(index=["year", "series_id", "period"], values=["value"], aggfunc=sum)
    groups = df_1_final.groupby(['series_id', 'year'])
    period_sums = groups.sum()
    # print(period_sums)
    
    # Find max sum of values per series_id
    max_values = groups.sum().pivot_table(index=["series_id"], values=["value"], aggfunc=[max])
    # print(max_values)
    
    # Find best years
    series_ids, years, max_vals, best_years = [], [], [], []

    for row in max_values.itertuples():
        ser_id = row[0]
        val = row[1]
        
        for per_row in period_sums.itertuples():
    #         display(ser_id)
    #         display(val)
            per_ser_id = per_row[0][0]
            year = per_row[0][1]
            max_val = per_row[1]
            if ser_id == per_ser_id and max_val == val:
                series_ids.append(ser_id)
                years.append(year)
                max_vals.append(val)     
    #             print(ser_id, year, max_val)
    
    report_series_ids = pd.Series(series_ids)
    report_years = pd.Series(years)
    report_max_vals = pd.Series(max_vals)
    dict = {"series_id": report_series_ids, "year": report_years, "value": report_max_vals}
    df_best_years = pd.DataFrame(dict)
    # print(df_best_years)
    
    
    # 3.3 FIND VALUE FOR SERIES_ID (NO POPULATION DATA AVAILABLE FOR 2022)
    def trim_all_columns(dfb):
        """
        Trim whitespace from ends of each value across all series in dataframe
        """
        trim_strings = lambda x: x.strip() if isinstance(x, str) else x
        return dfb.applymap(trim_strings)

    # df = trim_all_columns(df)
    df3 = trim_all_columns(df_1_clean)
    df3.columns = df3.columns.str.strip()
    df3_clean = df3[(df3['series_id']=="PRS30006032") & (df3['period']=='Q01')  ]
    df_left = df3_clean.drop('footnote_codes', axis=1)
    df_left_sorted = df_left.sort_values('value', ascending=False).head(1)
    report_year = df_left.sort_values('value', ascending=False).head(5)
    
    sqs_resource = boto3.resource('sqs')
    queue = sqs_resource.get_queue_by_name(QueueName='RearcDataQuestW43Queue')

    # Get the approximate number of messages in the queue
    number_of_messages = queue.attributes.get('ApproximateNumberOfMessages')

    logger.info("Approximate number of messages in SQS queue: %s", number_of_messages)
    
    for i in range(int(number_of_messages)): 
        logger.info("REPORT: ")
        # df_final = df_left_sorted.style.hide_index()
        # print(df_final)
        logger.info("No population data available for year %s", df_left_sorted['year'].iloc[:1].to_string(index=False)) 
        # logger.info("Estimated 2022 population based on available df_2 data: 330,996,211")
        # logger.info("Estimated 2021 population based on available df_2 data: 328,775,310")
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
