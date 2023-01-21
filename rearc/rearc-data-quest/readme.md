#### 1/21/23

- [x] 3.1 Generate population stats

#### 1/17/23

- [x] Load Part 1 and Part 2 as dataframes.

#### 1/16/23

- [X] List S3 bucket contents after upload. 

- [X] PASS PART 2.2

- [X] PASS PART 2.1

- [ ] Validate API data on underlying census site 

- [x] Add uploaded_to_s3 variable. 

Test cases: 

- [x] File missing from S3 bucket that is on the site. File should be downloaded to bucket. 

- [x] Extra file in S3 bucket that is not on the site. File should be removed from bucket. 

- [x] File in S3 bucket with metadata different than on site, metadata should be updated and file downloaded to bucket and old file removed. 

#### 1/7/23

- [x] Modify GetAccountSettings and other IAM policies for Rearc.

- [x] REMOVE EXTRANEOUS S3 FILES.

#### 1/5/23

- [ ] Fix logging issues. 

- [ ] Improve the line if site_files[i] not in s3_file_keys: by sorting?

- [x] Debug indent error when uncommenting download section.

- [x] Replace site_files[i][site_files[i].find with variable site_file_key. 

- [x] Fix # upload from /tmp to s3 and update dynamodb metadata line placement so it downloads 
appropriately. DONE. 


#### 1/1/23

- [x] Create DynamoDB metadata for files that don't exist in S3 bucket. 

- [x] Add DynamoDB permissions. 


#### 12/31/22 

- [x] Upload files to S3 bucket. 


#### 12/30/22 

- [x] Download files to /tmp. 


#### 12/28/22 

- [x] Replace print with logger.info 

- [x] implement sync