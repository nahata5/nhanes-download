import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import os
import json
from time import gmtime, strftime, localtime
import glob
from sqlalchemy import create_engine
import mysql.connector

base_cdc_url = 'https://wwwn.cdc.gov'


def get_table_links(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.text)
    table = soup.find(lambda tag: tag.has_attr('id') and tag['id']=="GridView1")
    
# Lambda expression for all links that end with XPT
    link_list = table.findAll(lambda tag: tag.name=='a' and tag['href'].endswith(".XPT"))
    links_only = [link.get('href') for link in link_list]
    
    return links_only

# This gets all of the links for the multiple years of data listed in year_list in order to batch download files
def get_multi_year(data_type, base_url):
    datatype_dict = {'demographics':'Demographics', 'dietary':'Dietary',
                     'examination':'Examination', 'laboratory':'Laboratory', 
                     'questionnaire':'Questionnaire'}
    # Can add years as future years are added
    year_list = [1999, 2001, 2003, 2005, 2007, 2009, 2011, 2013, 2015]
    data_links = []
    for year in year_list:
        url = f"{base_url}/nchs/nhanes/search/datapage.aspx?Component={datatype_dict[data_type]}&CycleBeginYear={year}"
        temp_data_links = get_table_links(url)
        for data in temp_data_links:
            if data not in data_links:
                data_links.append(data)
                print(f"Added {data} from {year}")
        time.sleep(1)

    return data_links

# Can use the link of filename.htm on top of base_cdc_url for access to the codebook links. This will produce a dictionary of column names can replace
def get_column_labels(xpt_link_name, base_url):
    htm_filename = f'{xpt_link_name[:-3]}htm'
    r = requests.get(f'{base_url}{htm_filename}')
    soup = BeautifulSoup(r.text)
    # Codebook section of documentation
    # TODO -- take section or pdf htm pages
    codebook_links = soup.findAll('div', id='CodebookLinks')[0].findAll('a')
    
    dictionary = {link.string.split('-')[0].strip() : link.string.split('-')[1].strip() for link in codebook_links}
    return dictionary

# Batch download function based off of data_type ['demographics', 'examination', 'dietary', 'laboratory', 'questionnaire']
def download_data(data_type, link_list, base_url):
    cwd = os.getcwd()
    try:
        os.mkdir(data_type)
        print(f'Created {data_type} folder')
    except:
        print(f'{data_type} folder exists')
    for link in link_list:
        item_name = link.split('/')[-1]
        exists = os.path.isfile(f'{cwd}/{data_type}/{item_name}')
        if exists:
            print(f'{item_name} already exists')
        else:
            current_time = time.time()
            print(f'Downloading {item_name} at {strftime("%a, %d %b %Y %H:%M:%S", localtime())}')
            r = requests.get(base_url + link, allow_redirects=True)
            open(f'{cwd}/{data_type}/{item_name}', 'wb').write(r.content)
            time_elapsed = time.time() - current_time
            print(f'Downloaded {item_name} at {time_elapsed}s')

# Create a dictionary of filenames for database
def create_xpt_dict(data_type):
    original_file_names = {}
    group_file_names = []
    for file in glob.glob(f'{data_type}/*'):
        xpt_file = file.split('/')[1]
        if len(xpt_file.split('_'))== 1:
            original_file_names[xpt_file.split('.')[0]] = [xpt_file]
    for file in glob.glob(f'{data_type}/*'):
        xpt_file = file.split('/')[1]
        if len(xpt_file.split('_'))> 1:
            try:
                xpt_name = xpt_file.split('_')[0]
                original_file_names[f'{xpt_name}'].append(xpt_file)
            except KeyError as e:
                xpt_name = xpt_file.split('_')[0]
                original_file_names[f'{xpt_name}'] = [xpt_file]               
    return original_file_names
        
# Concat tables based on file names
def combine_tables(data_type, xpt_dict):
    temp_df_list = []
    cwd = os.getcwd()
    for keys, values in xpt_dict[data_type].items():
        for value in values:
            print(f'Trying {cwd}/{data_type}/{value}')
            temp_df_list.append(pd.read_sas(f'{cwd}/{data_type}/{value}'))
            print(f'{cwd}/{data_type}/{value} appended')
    return pd.concat(temp_df_list)
                  
# Error handling if downloading empty files
def grab_empty_files(data_type, base_url):
    empty_list = []
    cwd = os.getcwd()
    for file in glob.glob(f'{cwd}/{data_type}/*'):
        if os.stat(file).st_size == 0:
            empty_list.append(file)
            os.remove(file)
    if len(empty_list) == 0:
        print("There are no empty files in this folder")
    else:
        print(f"Now re-downloading {len(empty_list)} files")
        download_data(data_type, empty_list, base_url)

# demographic_links = get_multi_year('demographics', base_cdc_url)
# dietary_links = get_multi_year('dietary', base_cdc_url)
# examination_links = get_multi_year('examination', base_cdc_url)
# laboratory_links = get_multi_year('laboratory', base_cdc_url)
# questionnaire_links = get_multi_year('questionnaire', base_cdc_url)

# link_dictionary = {'demographics':demographic_links, 'dietary':dietary_links, 
#                    'examination':examination_links, 'laboratory':laboratory_links,
#                   'questionnaire':questionnaire_links}
# with open('xpt_link_dict.json', 'w') as f:
#     json.dump(link_dictionary, f)
    
# Create the xpt_file_dict json for individual table creation and anticipation of merged tabes
# xpt_file_dict = {}
# for keys in link_dictionary:
#     xpt_file_dict[keys] = create_xpt_dict(keys)
    
# with open('xpt_file_dict.json', 'w') as f:
#     json.dump(xpt_file_dict, f)
    

    
# # Download data - 
# download_data('demographics', xpt_link_dictionary['demographics'], base_cdc_url)
# download_data('dietary', xpt_link_dictionary['dietary'], base_cdc_url)
# download_data('examination', xpt_link_dictionary['examination'], base_cdc_url)
# download_data('laboratory', xpt_link_dictionary['laboratory'], base_cdc_url)
# download_data('questionnaire', xpt_link_dictionary['questionnaire'], base_cdc_url)


#Ensure DB max_allowed_packet is set to 1G, this funciton will send to a mysql database
def send_to_db(user,password,host,port,database, data_type, link_dict, file_dict):
    engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}', 
                           echo=False)
    
    cwd = os.getcwd()
    counter = 0
    db_name_dict = create_db_names(data_type, link_dict, file_dict)
    for file in glob.glob(f'{cwd}/{data_type}/*'):
        file_name = file.split('/')[-1]
        print(f"Creating dataframe from {file}")
        temp_df = pd.read_sas(file, encoding='ISO-8859-1')
        print(f'Sending to MySQL Server as {db_name_dict[file_name][1]}')
        try:
            temp_df.to_sql(name=f'{db_name_dict[file_name][1]}', con=engine, if_exists='fail', index=False)
            counter += 1
        except ValueError as e:
            print(file_name + "is present")
            print(e)
            
        print('Now cleaning up db')
        del temp_df
    print(f'Added {counter} databases')



#   This will create file names that append the start year last 2 digits ie. 99 for 1999 and prefix DIET, DEMO, LAB, EXAM, QUEST for the respective filename. It will use the base file name ie. DEMO from DEMO_H.XPT as the filename
def create_db_names(data_type, link_dict, file_dict):
    
#   Exludes a DEMO preview because single tables do not need DEMO_DEMO
    prefix_dict = {'demographics': '', 'dietary': 'DIET_', 'examination': 'EXAM_', 
                   'laboratory': 'LAB_', 'questionnaire': 'QUEST_'}
    
    temp_dict = {}
    
#   Create temp_dict[filename:['2digit year']]
    for link in xpt_link_dictionary[data_type]:
            temp_dict[link.split('/')[-1]] = [link.split('/')[-2][2:4]]
            
#   Add prefix and DB name to temp_dict[xpt_filename: ['2digit year', 'DB Name example DIET_DSBI_99']]
    for key, values in xpt_file_dictionary[data_type].items():
        for value in values:
            if len(value.split('_')) > 2:
                #If there are multiple for same  year in sequence for instance lipids second value
                temp_dict[value].append(f'{prefix_dict[data_type]}'+ value[:-6] + "_" + temp_dict[value][0])
            else:
                temp_dict[value].append(f'{prefix_dict[data_type]}'+ key + "_" + temp_dict[value][0])
    
    return temp_dict

#Setting UTF8 and latin1 encoding errors 
#DSII does not play nice with UTF and encoding errors row '\xC2\x92S MU...' for column 'DSDSUPP' at row 74590
#DSPI Incorrect string value: '\xC2\x92S MU...' for column 'DSDSUPP' at row 6913

# Send folder of files and links to feather dataframes
def send_to_feather(data_type, link_dict, file_dict):    
    cwd = os.getcwd()
    counter = 0
    feather_name_dict = create_db_names(data_type, link_dict, file_dict)
    for file in glob.glob(f'{cwd}/{data_type}/*'):
        file_name = file.split('/')[-1]
        print(f"Creating dataframe from {file}")
        temp_df = pd.read_sas(file)
        print(f'Sending to Feather as {feather_name_dict[file_name][1]}')
        try:
            temp_df.to_feather(f'{cwd}/{data_type}_feather/{feather_name_dict[file_name][1]}.feather')
            counter += 1
        except ValueError as e:
            print(file_name + "is present")
            print(e)    
        print('Now cleaning up dataframe')
        del temp_df
    print(f'Added {counter} feather dataframes')






# First to get the links of all of the database
demographic_xpt_links = get_multi_year('demographics', base_cdc_url)
dietary_xpt_links = get_multi_year('dietary', base_cdc_url)
examination_xpt_links = get_multi_year('examination', base_cdc_url)
laboratory_xpt_links = get_multi_year('laboratory', base_cdc_url)
questionnaire_xpt_links = get_multi_year('questionnaire', base_cdc_url)


# Setup the xpt_link_dictionary to run download
xpt_link_dictionary = {'demographics':demographic_links, 'dietary':dietary_links, 
                   'examination':examination_links, 'laboratory':laboratory_links,
                  'questionnaire':questionnaire_links}

# Create JSON file of current link dict for future use
with open('xpt_link_dict.json', 'w') as f:
    json.dump(link_dictionary, f)
    
# Download individual groupings of xpt files
download_data('demographics', link_dictionary['demographics'], base_cdc_url)
download_data('dietary', link_dictionary['dietary'], base_cdc_url)
download_data('examination', link_dictionary['examination'], base_cdc_url)
download_data('laboratory', link_dictionary['laboratory'], base_cdc_url)
download_data('questionnaire', link_dictionary['questionnaire'], base_cdc_url)
    

# Create the xpt_file_dict json for individual table creation and anticipation of merged tabes
xpt_file_dict = {}
for keys in link_dictionary:
    xpt_file_dict[keys] = create_xpt_dict(keys)
    
with open('xpt_file_dict.json', 'w') as f:
    json.dump(xpt_file_dict, f)