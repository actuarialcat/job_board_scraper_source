# -*- coding: utf-8 -*-
"""
Created on Sat Jul 18 13:38:00 2020

This modele perform data cleaning and anaylsis on the downlaoded data

@author: Jackson
"""

import yaml 
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import datetime
import textract
import math


###################################################
# Global Param

# Load YAML config
CONFIG = yaml.load(open('../private_config/config_with_password.yaml', 'r'), Loader = yaml.Loader)

MAX_NUM_OF_FILES = CONFIG["max_num_of_files"]
OUTPUT_PATH = CONFIG["output_path"]
OUTPUT_PDF_PATH = CONFIG["pdf_output_path"]
OUTPUT_FILENAME = CONFIG["output_file_name"]


#%% Functons 
###################################################
#  Data Structure Functions

def read_data():
    """read main data table file"""
    
    df = pd.read_csv(OUTPUT_PATH + OUTPUT_FILENAME, index_col = 0)
    return df



###################################################
#  Data cleaning Functions

def validate_date(date_text):
    """validate date format"""
    
    valid_date = True
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        valid_date = False
        
    return valid_date



def valid_record(row):
    """check if the record is valid"""
    
    if row["job_number"] == -1:
        return False
    if not(validate_date(row["post_dt"]) and validate_date(row["close_dt"])):
        return False        
    
    return True



def valid_df(df):
    """Summary statistics"""
    
    total = len(df)
    valid = sum(df.apply(valid_record, axis=1))
    perc = valid / total * 100 
    
    print("Loaded rows: " + str(total))
    print("Valid rows:  " + str(valid))
    print("Valid:       " + "{:.2f}".format(perc) + "%")



###################################################
#  Premlim Analysis Functions




###################################################
#  Text Extraction Functions

def init_file_dataframe():
    """Template for extract details dataframe"""
    
    row_names_text = ["NA"] * MAX_NUM_OF_FILES
    
    for i in range(MAX_NUM_OF_FILES):
        row_names_text[i] = "text_" + str(i + 1)

    return row_names_text



def extract_text(file_name):
    """Extract text in single file"""
    
    file_path = OUTPUT_PDF_PATH + file_name
    text = textract.process(file_path)
    
    return str(text)
    
    

def extract_text_record(row):
    """Extract text in 1 record"""
    
    num_file = row["num_of_file"]
    job_number = row["job_number"]
    
    file_text = ["NA"] * MAX_NUM_OF_FILES
    row_names_text = init_file_dataframe()
    
    exit_code = 0
    
    if math.isnan(num_file):
        exit_code = -1
        df_text = pd.Series()
        
    else:
        for i in range(int(num_file)):
            try:
                file_text[i] = extract_text(row["down_" + str(i + 1)])
            except:
                file_text[i] = "Error"
                exit_code = -1
            
        df_text = np.transpose(pd.DataFrame(file_text, row_names_text))
    
    
    df_index = pd.DataFrame(data = {"job_number": [job_number]})
    df_details = pd.concat([df_index.reset_index(drop=True), df_text], axis = 1, sort = False)
    
    return df_details, exit_code
        
    
    
def extract_text_loop(df):
    """Extract text control for whole dataframe"""
    
    start = 0
    end = len(df)
    
    df_details = pd.DataFrame()
    
    for i in range(start, end):
        try:
            row = df.iloc[i]
            
            row_result = extract_text_record(row)
            df_new = row_result[0]
            df_details = df_details.append(df_new, ignore_index = True, sort = False)
            
            if row_result[1] != 0:
                print("Error on item: " + str(i) + " !!!!!!!!!!")
        
        except:
            df_details = df_details.append(pd.Series(), ignore_index = True, sort=False)
            print("Error on item: " + str(i) + " !!!!!!!!!!")
            
        if(i % 50 == 0):
            print("Competed item: " + str(i))
        
    print("Competed item all")
    return df_details
    
    
    
#%% Initilize
###################################################
    
df = read_data()


#%% Data cleaning

valid_df(df)


#%% Read PDF

df_details = extract_text_loop(df)




