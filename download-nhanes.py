import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from time import gmtime, strftime
import json
import os

base_cdc_url = 'https://wwwn.cdc.gov'


def get_table_links(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.text)
    table = soup.find(lambda tag: tag.has_attr('id') and tag['id']=="GridView1")
    
# Lambda expression for all links that end with XPT
    link_list = table.findAll(lambda tag: tag.name=='a' and tag['href'].endswith(".XPT"))
    links_only = [link.get('href') for link in link_list]
    
    return links_only


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

def get_column_labels(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.text)
    
    # Codebook section of documentation
    # TODO -- take section or pdf htm pages
    codebook_links = soup.findAll('div', id='CodebookLinks')[0].findAll('a')
    
    dictionary = {link.string.split('-')[0].strip() : link.string.split('-')[1].strip() for link in codebook_links}
    return dictionary


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