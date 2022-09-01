# melbourne_pedestrians

The code in this repository is designed to carry out the following tasks:
  
  1. extract (ALL) data from each of the following APIs:
  
      https://data.melbourne.vic.gov.au/Transport/Pedestrian-Counting-System-Sensor-Locations/h57g-5234
      https://data.melbourne.vic.gov.au/Transport/Pedestrian-Counting-System-Monthly-counts-per-hour/b2ak-trbp
      
  2. Convert the data from JSON to CSV and save in AWS s3 bucket and path according to json config file.
  
  3. Create a new AWS Glue database and table with appropriate column names and formatting so that data can be queried via Athena
  
  4. Extract key statistics regarding the busiest pedistrian locations in Melbourne's CBD and how Covid lockdowns impacted them.
  
Components 

  melbourne_pedestrian_data_extraction - python module consisting of: 
    
    main.py, 
    
    logging_module.py, 
    
    melb_peds_custom_functions.py, 
    
    melb_pedestrian_data_extraction_config

  Athena_queries - contains text file of queries which can be executed directly in AWS Athena console
    
    athena_queries_for_answer_calculation.txt
      changes data formats,
      cleans a little problematic data and calculates outputs below.
      (see comments in file for more details)
      
  key_outputs - folder contains excel file of statistics and summary data
    statistics_and_summary_data
      
      1. Top 10 Pedistrian Traffic Locations (based on daily pedesitran count averages 2018 - 2022)  
      
      2. Top 10 Pedistrian Traffic Locations (based on monthly pedesitran count averages 2018 - 2022)  
      
      3. Most reduced pedestrian counts during covid years
      
      4. Most Growth in daily pedestrians 2022 vs 2021
    

To Execute melbourne_pedestrian_data_extraction:
  
  save contents to local drive of machine with public internet access and python 3.7 or greater installed.
  
  open main.py script and edit accordingly to provide values for:
  
    aws_catalogue_id 
    access_key_id    
    secret_access_key
 
 Be careful to ensure the actual values for these items are properly encoded.
 
 Once this is complete simply execute the main.py file.
 
 Note that you can also edit the main.py file to save a local csv file instead if AWS access is not easy.
 
 In this case you will likely need to delete or comment out the code relating to AWS Glue and execute manually via AWS console.
 
 
 
 
 
    
