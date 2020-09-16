# -*- coding: utf-8 -*-
"""
Created on Sat Jul 18 13:38:00 2020

The module scrape the HKU SAAS job board and download the data to the output folders

@author: Jackson
"""

import yaml 
import requests
from lxml import html
import pandas as pd 
import numpy as np
from pathlib import Path

    

###################################################
# Global Param

# Load YAML config
CONFIG = yaml.load(open('../private_config/config_with_password.yaml', 'r'), Loader = yaml.Loader)

MAX_NUM_OF_FILES = CONFIG["max_num_of_files"]
OUTPUT_PATH = CONFIG["output_path"]
OUTPUT_PDF_PATH = CONFIG["pdf_output_path"]
OUTPUT_FILENAME = CONFIG["output_file_name"]


###################################################
# Web-interface functions

def login_website(session_requests):
    """Log in SAAS website"""
    
    LOGIN_URL = CONFIG["login_url"]
    USERNAME = CONFIG["username"]
    PASSWORD = CONFIG["password"]
    
    # Create payload
    payload = {
        "login": USERNAME, 
        "password": PASSWORD
    }

    # Perform login
    result = session_requests.post(LOGIN_URL, data = payload, headers = dict(referer = LOGIN_URL))

    # print status
    print("Login status: ")
    print(result.ok)
    print(result.status_code)
    print()



def initiate_session():
    """Start a web session and login"""
    
    session_requests = requests.session()
    login_website(session_requests)
    print("Login Completed")
    
    return session_requests



def scrape_html( session_requests, url ):
    """Input session and URL, return scaped html data"""
    
    result = session_requests.get(url, headers = dict(referer = url))
    tree = html.fromstring(result.content)
    
    return tree



def download_file(session_requests, file_url, job_num, file_num, ext):
    """Download job pdf files"""
    
    filename = "job_" + str(job_num) + "_file_" + str(file_num) + ext
    pathname = Path(OUTPUT_PDF_PATH + filename) 
    response = session_requests.get(file_url)
    pathname.write_bytes(response.content)
    
    return filename



###################################################
# Data Structure Functions

def init_file_dataframe():
    """Template for extract details dataframe"""
    
    row_names_link = ["NA"] * MAX_NUM_OF_FILES
    row_names_name = ["NA"] * MAX_NUM_OF_FILES
    row_names_down = ["NA"] * MAX_NUM_OF_FILES
    
    for i in range(MAX_NUM_OF_FILES):
        row_names_link[i] = "link_" + str(i + 1)
        row_names_name[i] = "name_" + str(i + 1)
        row_names_down[i] = "down_" + str(i + 1)
    
    df = pd.DataFrame(columns = row_names_link + row_names_name + row_names_down)
         
    return df, row_names_link, row_names_name, row_names_down



def find_file_extention(file_name):
    """Find the file extention"""
    
    index = file_name.rfind(".")
    ext = file_name[index:].lower()
    
    return ext
    


###################################################
# Data Extraction Functions

def extract_details( session_requests, job_id ):
    """Extract data inside each job add pop-up window"""
    
    url_prefix = CONFIG["url_prefix"]
    
    #Extract html from web
    url = CONFIG["url_jobno"] + str(job_id)
    tree = scrape_html(session_requests, url)
    
    #Extact description
    title = "; ".join(tree.xpath("//p[@class='listheader']/text()"))
    description = "; ".join(tree.xpath("//p//text()")) #more than one element
    
    #Extract files
    num_file = int(tree.xpath("count(//p[contains(text(),'Job Description Document :')]//a)"))
    loop_range = min(num_file, (MAX_NUM_OF_FILES - 1))
    
    file_link = ["NA"] * MAX_NUM_OF_FILES
    file_name = ["NA"] * MAX_NUM_OF_FILES
    down_file_name = ["NA"] * MAX_NUM_OF_FILES
    
    if (num_file > (MAX_NUM_OF_FILES - 1)):
        file_link[(MAX_NUM_OF_FILES - 1)] = "More than 9 files"
        file_name[(MAX_NUM_OF_FILES - 1)] = "More than 9 files"
    
    for i in range(loop_range):
        file_link[i] = url_prefix + tree.xpath("//p[contains(text(),'Job Description Document :')]//a/@href")[i]
        file_name[i] = tree.xpath("//p[contains(text(),'Job Description Document :')]//a/text()")[i]
        
        ext = find_file_extention(file_name[i])
        down_file_name[i] = download_file(session_requests, file_link[i], job_id, i, ext)
        
    # dataframe
    row_names_link = init_file_dataframe()[1]
    row_names_name = init_file_dataframe()[2]
    row_names_down = init_file_dataframe()[3]
    
    df_link = np.transpose(pd.DataFrame(file_link, row_names_link))
    df_name = np.transpose(pd.DataFrame(file_name, row_names_name))
    df_down = np.transpose(pd.DataFrame(down_file_name, row_names_down))
    
    df_file = pd.DataFrame(data = {"job_title": [title], "description": [description], "num_of_file": [loop_range]})
    df_file = pd.concat([df_file.reset_index(drop=True), df_link], axis=1, sort=False)
    df_file = pd.concat([df_file.reset_index(drop=True), df_name], axis=1, sort=False)
    df_file = pd.concat([df_file.reset_index(drop=True), df_down], axis=1, sort=False)
    
    return df_file



def extract_table( session_requests, tree ):
    """Extract data in the table in each webpage"""
    
    num_row = int(tree.xpath("count(//table[@class='tbl']//tr)")) - 1
    
    name = ["NA"] * num_row
    jobtype = ["NA"] * num_row
    post_dt = ["NA"] * num_row
    close_dt = ["NA"] * num_row
    job_number = [-1] * num_row
    
    df_file = pd.DataFrame()
    
    for i in range(num_row):
        #table data
        name[i] = "; ".join(tree.xpath("//table[@class='tbl']//tr[" + str(i + 2) + "]/td[1]/text()"))
        jobtype[i] = "; ".join(tree.xpath("//table[@class='tbl']//tr[" + str(i + 2) + "]/td[last()-2]/text()"))
        post_dt[i] = "; ".join(tree.xpath("//table[@class='tbl']//tr[" + str(i + 2) + "]/td[last()-1]/text()"))
        close_dt[i] = "; ".join(tree.xpath("//table[@class='tbl']//tr[" + str(i + 2) + "]/td[last()]/text()"))
        
        #description
        try:
            job_title = tree.xpath("//table[@class='tbl']//tr[" + str(i + 2) + "]/td[2]/a[1]/@onclick")[0]
            #print(i)
            
            index_start = job_title.find("jobno")
            index_end = job_title.find("'",index_start)
            job_number[i] = int(job_title[index_start+6:index_end])
            
            df_new = extract_details(session_requests, job_number[i])
            df_file = df_file.append(df_new, ignore_index = True, sort=False)
        
        except IndexError:
            job_number[i] = -1
            df_file = df_file.append(pd.Series(), ignore_index = True, sort=False)
            print("Error on item: " + str(i + 1) + " !!!!!!!!!!")
        
    
    df_description = pd.DataFrame(data = {
        "name": name, "jobtype": jobtype, "post_dt": post_dt, "close_dt": close_dt,
        "job_number": job_number})
    
    df = pd.concat([df_description.reset_index(drop=True), df_file], axis=1, sort=False)
        
    return df



###################################################
# Control Functions

def extract_all_pages( session_requests, i):
    url = CONFIG["url_page"] + str(i)
    tree = scrape_html(session_requests, url)
    
    df = extract_table(session_requests, tree)
    print("Competed page: " + str(i))
    
    return df



def extract_all_pages_loop( session_requests, start, max_page ):
    
    df = pd.DataFrame()
    
    for i in range(start, max_page + 1):
        df_new = extract_all_pages(session_requests, i)
        df = df.append(df_new, ignore_index = True, sort=False)
        
    return df



def output_cvs( df ):
    filename = Path(OUTPUT_PATH + OUTPUT_FILENAME) 
    df.to_csv(filename)



###################################################
# Main

if ("session_requests" not in globals()):
    global session_requests
    session_requests = initiate_session()

start_page = CONFIG["start_page"]
end_page = CONFIG["end_page"]
df = extract_all_pages_loop(session_requests, start_page, end_page) # 1 - 61

output_cvs(df)


        
    










































