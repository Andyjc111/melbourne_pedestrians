import os
import requests
import json
import pandas as pd
import datetime
import logging
import os
import csv
import boto3
from botocore.exceptions import ClientError
import io
import keyring
from logging_module import startlogging
import belong_data_test_custom_functions as cf


if __name__ == '__main__':

	start_script = datetime.datetime.now().strftime('%Y%m%d_%H%M')
	log          = logging.getLogger(__name__)
	startlogging()
	
	
	with open('melb_pedestrian_data_extraction_config.json', 'r') as cnfg:
		script_config =json.load(cnfg)
		
	
	for table in script_config:
		
		# WARNING CODE WILL NEED TO BE UPDATED HERE TO FIND YOUR OWN SECRETS DEPENDING ON HOW YOU STORE THEM #
		aws_catalogue_id  =  'insert function or code to get your secrets'
		access_key_id     =  'insert function or code to get your secrets'
		secret_access_key =  'insert function or code to get your secrets'
		
		# ALL OTHER PARAMETERS STORED IN CONFIG FILE
		glue_database_name = script_config[table]['glue_database_name']
		glue_table_name    = script_config[table]['glue_table_name']
		s3_path            = script_config[table]['s3_path']
		s3_bucket          = script_config[table]['s3_bucket']
		aws_region_name    = script_config[table]['aws_region_name']
		api_location         = script_config[table]['api_location']
		dataset              = script_config[table]['dataset']
		order_by             = script_config[table]['order_by']
		records_per_api_call = script_config[table]['records_per_api_call']
		offset               = 0
		master_data_list     = []
		continue_extraction  = True
		
		
		# LOOP THROUGH THE DATA AVAILABLE FROM API DOWNLOADING 2000 RESULTS AT A TIME UNTIL COMPLETE
		while continue_extraction:
			try:
				url         = f"{api_location}{dataset}?$limit={records_per_api_call}&$offset={str(offset)}&$order={order_by}"
				response    = cf.get_request(url)
				data        = json.loads(response.content.decode("utf-8"))
				data_length = len(data)
				if data_length >0:
					for item in data:
						master_data_list.append(item)
					offset += data_length
					log.info(f'records extracted {str(offset)}')
				else:
					log.info('NO DATA RETURNED FROM API - EXTRACTION COMPLETE')
					continue_extraction = False
			except Exception as e:
				log.error(f'warning errror occurred during data extraction: {str(e)}')
				#begin error handling process
				exit(1)
				break    
		
		
		# WRITE THE DATA TO AWS (OR WRITE TO LOCAL CSV FILE IF PREFERRED)
		# WARNING FILE SIZE IS APPROX 450MB
		cf.write_json_to_csv_in_s3(dataset, 
								master_data_list, 
								start_script, 
								s3_path, 
								s3_bucket, 
								access_key_id, 
								secret_access_key, 
								aws_region_name
								)        
		#cf.write_json_to_csv(dataset, master_data_list, start_script)
		
		# INITIATE A GLUE CLIENT. NEEDED TO CREATE A DATABASE AND TABLES WHERE DATA WILL BE STORED FOR LATER QUERYING
		glue_client = boto3.client('glue',
									aws_access_key_id     = access_key_id, 
									aws_secret_access_key = secret_access_key,
									region_name           = aws_region_name
								)    
		
		
		# CREATE DATABASE (EXCEPT ERRORS IF IT ALREADY EXISTS)
		try:
			response = glue_client.create_database(
				CatalogId= aws_catalogue_id,
				DatabaseInput={'Name': glue_database_name}
			)
		except ClientError as e:
			if e.response['Error']['Code'] == 'AlreadyExistsException ':
				pass
		
			
	# DELETE TABLE (EXCEPT ERRORS IF IT ALREADY EXISTS)       
		try:
			response = glue_client.delete_table(
			CatalogId=aws_catalogue_id,
			DatabaseName=glue_database_name,
			Name=glue_table_name
			)
		except ClientError as e:
			if e.response['Error']['Code'] == 'EntityNotFoundException ':
				pass
		
		
		# CREATE GLUE TABLE #
		column_names = list(set().union(*(d.keys() for d in master_data_list)))        
		columns_formatted = [{'Name':col, 'Type':'string'} for col in column_names ]
		try:
				response = glue_client.create_table(
				CatalogId    = aws_catalogue_id,
				DatabaseName = glue_database_name,
				TableInput   ={
					'Name': glue_table_name,    
					'Description': 'Table created for storing melbourne pedestrain sensor data (part of Belong interview test)',
					'StorageDescriptor': {    
						'Columns': columns_formatted,
						'Location': f's3://{s3_bucket}/{s3_path}/',
						'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
						'SerdeInfo': {
							'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
						},
					},
					'Parameters': {
						'classification': 'csv',
						'typeOfData': 'file',
					}
				}
			)
		except ClientError as e:
			if e.response['Error']['Code'] == 'AlreadyExistsException ':
				pass    
		
		
