import logging
import requests
import io
import boto3
import json
import csv
import datetime

log = logging.getLogger(__name__)


def get_request(url):
    attempts = 0 
    while attempts <3:
        log.info(f'attempt api call for url {url}')
        response = requests.get(url)
        if response.status_code != 200:
            log.error(f'warning, requests module failed with reason :{response.text}')
            attempts +=1
        else:
            log.info('request completed successfully')
            return response
    

def write_json_to_csv(dataset_name, data, start_script_date):
    with open(f'{dataset_name.replace("json","csv")}', 'a', newline='') as out_file: 
        writer = csv.writer(out_file, quoting=csv.QUOTE_NONNUMERIC)
        column_names = list(set().union(*(d.keys() for d in data)))
        for row in data:
            row_complete = []
            for col in column_names:
                if col in row.keys():
                    row_complete.insert(len(row_complete),row[col])
                else:
                    row_complete.insert(len(row_complete), '')
            row_complete.insert(len(row_complete), str(start_script_date))        
            writer.writerow(row_complete)


def write_json_to_csv_in_s3(dataset_name, data, start_script_date, s3_path, s3_bucket, aws_access_key_id, aws_secret_access_key, aws_region_name):
    log.info('authenticating s3 client')
    s3_resource = boto3.resource('s3',
                                 aws_access_key_id     = aws_access_key_id, 
                                 aws_secret_access_key = aws_secret_access_key,
                                 region_name           = aws_region_name
                                )

    log.info('converting json to csv')
    output = io.StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC)
    column_names = list(set().union(*(d.keys() for d in data)))
    for row in data:
        row_complete = []
        for col in column_names:
            if col in row.keys():
                row_complete.insert(len(row_complete),row[col])
            else:
                row_complete.insert(len(row_complete), '')
        row_complete.insert(len(row_complete), str(start_script_date))
        writer.writerow(row_complete)
    log.info('begin writing file')
    data_file_name = f'{s3_path}{dataset_name.replace("json","")}_{start_script_date}.csv'
    s3_resource.Object(s3_bucket, data_file_name).put(Body=output.getvalue())