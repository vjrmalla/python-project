#!/usr/bin/env python3
import os, shutil, sys
import csv
import requests
import datetime, time
import numbers, decimal
import boto3, botocore
import traceback, logging
import concurrent
import argparse
import glob
import re
import json
from pathlib import Path
from datatest import validate
from urllib.parse import urlparse, parse_qs, parse_qsl, urlencode, urlunparse
from multiprocessing.pool import ThreadPool

environment = sys.argv[1] if len(sys.argv) > 1 else 'development'
quarters = int(sys.argv[2]) if len(sys.argv) > 2 else 12  # 12 means 3 years. Min value: 1 (quarter)

# TO RUN on dev's machine:
#     source ~/dev/aws/bin/activate   and   cd <folder_with_script>
#     pip3 install boto3 requests  (to make sure modules 'boto3' and 'requests' are installed)
# python3 nomis_etl_ec2.py

is_dev_machine = True
upload_landing_to_bucket = True        # True on Lambda     # False on Batch
download_landing_from_bucket = False    # False on Lambda    # True on Batch
process_landing = False                  # False on Lambda    # True on Batch
upload_valid_to_bucket = False          # False on Lambda    # True on Batch
verify_geocodes = False

bucket = f'ers-development-ops-mi-1'
region = 'eu-west-2'

# API BASE URL
nomis_api_base_url = 'http://www.nomisweb.co.uk/api/v01/dataset'

# TIME
interval = f'latestMINUS{quarters - 1}-latest' # RELATIVE  (ABSOLUTE: '2018-6-2021-3', MIXED: '2018-6-latest')
interval_claims = f'latestMINUS{3 * quarters - 1}-latest' # adjusting for monthly KPI

# GEOGRAPHY
nation_codes = {'UK':   '2092957697', 'GB':   '2092957698', 'ENGL': '2092957699',
                'WALES':'2092957700', 'SCOTL':'2092957701', 'NI':   '2092957702'}

region_codes = '2092957697...2092957699,2013265921...2013265932'
county_codes = '1807745025...1807745028,1807745030...1807745032,1807745034...1807745083,1807745085,1807745282,1807745283,1807745086...1807745155,1807745157...1807745164,1807745166...1807745170,1807745172...1807745177,1807745179...1807745194,1807745196,1807745197,1807745199,1807745201...1807745218,1807745221,1807745222,1807745224,1807745226...1807745231,1807745233,1807745234,1807745236...1807745244'
district_codes = '1811939329...1811939332,1811939334...1811939336,1811939338...1811939428,1811939436...1811939442,1811939768,1811939769,1811939443...1811939497,1811939499...1811939501,1811939503,1811939505...1811939507,1811939509...1811939517,1811939519,1811939520,1811939524...1811939570,1811939575...1811939599,1811939601...1811939628,1811939630...1811939634,1811939636...1811939647,1811939649,1811939655...1811939664,1811939667...1811939680,1811939682,1811939683,1811939685,1811939687...1811939704,1811939707,1811939708,1811939710,1811939712...1811939717,1811939719,1811939720,1811939722...1811939730'
metrocounty_codes = '1937768449...1937768456'
dstr_mcty_cty_codes = '1807745045,1807745056,1807745067,1807745081...1807745083,1807745085,1807745090,1807745091,1807745099,1807745106...1807745110,1811939329...1811939332,1811939334...1811939336,1811939338...1811939428,1811939436...1811939497,1811939499...1811939501,1811939503,1811939505...1811939507,1811939509...1811939517,1811939519,1811939520,1811939524...1811939570,1811939575...1811939599,1811939601...1811939628,1811939630...1811939634,1811939636...1811939647,1811939649,1811939655...1811939664,1811939667...1811939680,1811939682,1811939683,1811939685,1811939687...1811939704,1811939707,1811939708,1811939710,1811939712...1811939717,1811939719,1811939720,1811939722...1811939730,1811939768,1811939769,1937768449...1937768456,1807745158...1807745163,1807745177,1807745180,1807745181'


def get_query_param(url, param):
    return parse_qs(urlparse(url).query)[param][0].split(',')

def process_occupation_record(in_header, csv_record):
    date_in = csv_record[in_header[0]]
    geocode_in = csv_record[in_header[1]]
    geoname_in = csv_record[in_header[2]]
    value_in = csv_record[in_header[3]]
    variable_in = csv_record[in_header[4]]

    date_out = get_start_date(date_in)
    geocode_out = geocode_in.strip()
    geoname_out = geoname_in.strip()
    value_out = process_val(value_in, True)
    ethnicity_out = get_ethnicity(variable_in)
    occupation_out = get_occupation(variable_in)
    return [date_out, geocode_out, geoname_out, value_out, ethnicity_out, occupation_out]

def process_sector_record(in_header, csv_record):
    date_in = csv_record[in_header[0]]
    geocode_in = csv_record[in_header[1]]
    geoname_in = csv_record[in_header[2]]
    value_in = csv_record[in_header[3]]
    variable_in = csv_record[in_header[4]]

    date_out = get_start_date(date_in)
    geocode_out = geocode_in.strip()
    geoname_out = geoname_in.strip()
    value_out = process_val(value_in, True)
    ethnicity_out = get_ethnicity(variable_in)
    sic_and_sector = get_sic_and_sector(variable_in)
    sic_out = sic_and_sector[0]
    sector_out = sic_and_sector[1]
    return [date_out, geocode_out, geoname_out, value_out, ethnicity_out, sic_out, sector_out]

def process_empl_voml_record(in_header, csv_record):
    date_in = csv_record[in_header[0]]
    geocode_in = csv_record[in_header[1]]
    geoname_in = csv_record[in_header[2]]
    value_in = csv_record[in_header[3]]

    date_out = get_start_date(date_in)
    geocode_out = geocode_in.strip()
    geoname_out = geoname_in.strip()
    value_out = process_val(value_in, False)
    return [date_out, geocode_out, geoname_out, value_out]

def process_empl_rate_record(in_header, csv_record):
    date_in = csv_record[in_header[0]]
    geocode_in = csv_record[in_header[1]]
    geoname_in = csv_record[in_header[2]]
    value_in = csv_record[in_header[3]]

    date_out = get_start_date(date_in)
    geocode_out = geocode_in.strip()
    geoname_out = geoname_in.strip()
    value_out = process_val(value_in, True)
    return [date_out, geocode_out, geoname_out, value_out]

def process_empl_ci_record(in_header, csv_record):
    date_in = csv_record[in_header[0]]
    geocode_in = csv_record[in_header[1]]
    geoname_in = csv_record[in_header[2]]
    value_in = csv_record[in_header[3]]

    date_out = get_start_date(date_in)
    geocode_out = geocode_in.strip()
    geoname_out = geoname_in.strip()
    value_out = process_val(value_in, True)
    return [date_out, geocode_out, geoname_out, value_out]

def process_unempl_voml_record(in_header, csv_record):
    date_in = csv_record[in_header[0]]
    geocode_in = csv_record[in_header[1]]
    geoname_in = csv_record[in_header[2]]
    value_in = csv_record[in_header[3]]

    date_out = get_start_date(date_in)
    geocode_out = geocode_in.strip()
    geoname_out = geoname_in.strip()
    value_out = process_val(value_in, False)
    return [date_out, geocode_out, geoname_out, value_out]

def process_unempl_rate_record(in_header, csv_record):
    date_in = csv_record[in_header[0]]
    geocode_in = csv_record[in_header[1]]
    geoname_in = csv_record[in_header[2]]
    value_in = csv_record[in_header[3]]

    date_out = get_start_date(date_in)
    geocode_out = geocode_in.strip()
    geoname_out = geoname_in.strip()
    value_out = process_val(value_in, True)
    return [date_out, geocode_out, geoname_out, value_out]

def process_unempl_ci_record(in_header, csv_record):
    date_in = csv_record[in_header[0]]
    geocode_in = csv_record[in_header[1]]
    geoname_in = csv_record[in_header[2]]
    value_in = csv_record[in_header[3]]

    date_out = get_start_date(date_in)
    geocode_out = geocode_in.strip()
    geoname_out = geoname_in.strip()
    value_out = process_val(value_in, True)
    return [date_out, geocode_out, geoname_out, value_out]

def process_inact_voml_record(in_header, csv_record):
    date_in = csv_record[in_header[0]]
    geocode_in = csv_record[in_header[1]]
    geoname_in = csv_record[in_header[2]]
    value_in = csv_record[in_header[3]]

    date_out = get_start_date(date_in)
    geocode_out = geocode_in.strip()
    geoname_out = geoname_in.strip()
    value_out = process_val(value_in, False)
    return [date_out, geocode_out, geoname_out, value_out]

def process_inact_rate_record(in_header, csv_record):
    date_in = csv_record[in_header[0]]
    geocode_in = csv_record[in_header[1]]
    geoname_in = csv_record[in_header[2]]
    value_in = csv_record[in_header[3]]

    date_out = get_start_date(date_in)
    geocode_out = geocode_in.strip()
    geoname_out = geoname_in.strip()
    value_out = process_val(value_in, True)
    return [date_out, geocode_out, geoname_out, value_out]

def process_inact_ci_record(in_header, csv_record):
    date_in = csv_record[in_header[0]]
    geocode_in = csv_record[in_header[1]]
    geoname_in = csv_record[in_header[2]]
    value_in = csv_record[in_header[3]]

    date_out = get_start_date(date_in)
    geocode_out = geocode_in.strip()
    geoname_out = geoname_in.strip()
    value_out = process_val(value_in, True)
    return [date_out, geocode_out, geoname_out, value_out]

def process_empl_rate_by_a_g_record(in_header, csv_record):
    date_in = csv_record[in_header[0]]
    geocode_in = csv_record[in_header[1]]
    geoname_in = csv_record[in_header[2]]
    variable_in = csv_record[in_header[3]]
    value_in = csv_record[in_header[4]]

    date_out = get_start_date(date_in)
    geocode_out = geocode_in.strip()
    geoname_out = geoname_in.strip()
    gender_out = get_gender(variable_in)
    age_out = get_age(variable_in)
    value_out = process_val(value_in, True)
    return [date_out, geocode_out, geoname_out, gender_out, age_out, value_out]

def process_empl_ci_by_a_g_record(in_header, csv_record):
    date_in = csv_record[in_header[0]]
    geocode_in = csv_record[in_header[1]]
    geoname_in = csv_record[in_header[2]]
    variable_in = csv_record[in_header[3]]
    value_in = csv_record[in_header[4]]

    date_out = get_start_date(date_in)
    geocode_out = geocode_in.strip()
    geoname_out = geoname_in.strip()
    gender_out = get_gender(variable_in)
    age_out = get_age(variable_in)
    value_out = process_val(value_in, False)
    return [date_out, geocode_out, geoname_out, gender_out, age_out, value_out]

def process_unempl_rate_by_a_g_record(in_header, csv_record):
    date_in = csv_record[in_header[0]]
    geocode_in = csv_record[in_header[1]]
    geoname_in = csv_record[in_header[2]]
    variable_in = csv_record[in_header[3]]
    value_in = csv_record[in_header[4]]

    date_out = get_start_date(date_in)
    geocode_out = geocode_in.strip()
    geoname_out = geoname_in.strip()
    gender_out = get_gender(variable_in)
    age_out = get_age(variable_in)
    value_out = process_val(value_in, True)
    return [date_out, geocode_out, geoname_out, gender_out, age_out, value_out]

def process_unempl_ci_by_a_g_record(in_header, csv_record):
    date_in = csv_record[in_header[0]]
    geocode_in = csv_record[in_header[1]]
    geoname_in = csv_record[in_header[2]]
    variable_in = csv_record[in_header[3]]
    value_in = csv_record[in_header[4]]

    date_out = get_start_date(date_in)
    geocode_out = geocode_in.strip()
    geoname_out = geoname_in.strip()
    gender_out = get_gender(variable_in)
    age_out = get_age(variable_in)
    value_out = process_val(value_in, False)
    return [date_out, geocode_out, geoname_out, gender_out, age_out, value_out]

def process_inact_rate_by_a_g_record(in_header, csv_record):
    date_in = csv_record[in_header[0]]
    geocode_in = csv_record[in_header[1]]
    geoname_in = csv_record[in_header[2]]
    variable_in = csv_record[in_header[3]]
    value_in = csv_record[in_header[4]]

    date_out = get_start_date(date_in)
    geocode_out = geocode_in.strip()
    geoname_out = geoname_in.strip()
    gender_out = get_gender(variable_in)
    age_out = get_age(variable_in)
    value_out = process_val(value_in, True)
    return [date_out, geocode_out, geoname_out, gender_out, age_out, value_out]

def process_inact_ci_by_a_g_record(in_header, csv_record):
    date_in = csv_record[in_header[0]]
    geocode_in = csv_record[in_header[1]]
    geoname_in = csv_record[in_header[2]]
    variable_in = csv_record[in_header[3]]
    value_in = csv_record[in_header[4]]

    date_out = get_start_date(date_in)
    geocode_out = geocode_in.strip()
    geoname_out = geoname_in.strip()
    gender_out = get_gender(variable_in)
    age_out = get_age(variable_in)
    value_out = process_val(value_in, False)
    return [date_out, geocode_out, geoname_out, gender_out, age_out, value_out]

def process_claim_vol_record(in_header, csv_record):
    date_in = csv_record[in_header[0]]
    geocode_in = csv_record[in_header[1]]
    geoname_in = csv_record[in_header[2]]
    value_in = csv_record[in_header[3]]
    gender_in = csv_record[in_header[4]]
    age_in = csv_record[in_header[5]]

    date_out = get_start_date(date_in, date_format = '%B %Y')
    geocode_out = geocode_in.strip()
    geoname_out = geoname_in.strip()
    value_out = process_val(value_in, False)
    gender_out = gender_in.strip()
    age_out = age_in.strip()
    return [date_out, geocode_out, geoname_out, value_out, gender_out, age_out]

def process_claim_rate_record(in_header, csv_record):
    date_in = csv_record[in_header[0]]
    geocode_in = csv_record[in_header[1]]
    geoname_in = csv_record[in_header[2]]
    value_in = csv_record[in_header[3]]
    gender_in = csv_record[in_header[4]]
    age_in = csv_record[in_header[5]]

    date_out = get_start_date(date_in, date_format = '%B %Y')
    geocode_out = geocode_in.strip()
    geoname_out = geoname_in.strip()
    value_out = process_val(value_in, True)
    gender_out = get_gender(gender_in.strip())
    age_out = get_age(age_in.strip())
    return [date_out, geocode_out, geoname_out, value_out, gender_out, age_out]

def get_ethnicity(str):
    lower_str = str.lower()
    if "whites" in lower_str:
        return "Whites"
    elif "mixed ethnic group" in lower_str:
        return "Mixed Ethnic Groups"
    elif "indians" in lower_str:
        return "Indians"
    elif "pakistani/bangladeshis" in lower_str:
        return "Pakistani/Bangladeshi"
    elif "black or black british" in lower_str:
        return "Black"
    elif "other ethnic group" in lower_str or "other ethnic grps" in lower_str:
        return "Other"
    else:
        print(f"Could not map string '{str}' to ethnicity")
        return None

def get_occupation(str):
    lower_str = str.lower()
    if "managers, directors & senior" in lower_str:
        return "Managers, Directors & Senior Officials"
    elif "professional occupations" in lower_str or "prof. occupations" in lower_str \
                 or "prof. occups." in lower_str or "professional occups." in lower_str:
        return "Professional Occupations"
    elif "associate prof. & technical occupations" in lower_str or "associate prof. & tech. occups." in lower_str:
        return "Associate Professional & Technical Occupations"
    elif "administrative & secretarial occupations" in lower_str or "admin. & secretarial occups." in lower_str:
        return "Administrative & Secretarial Occupations"
    elif "skilled trades occupations" in lower_str or "skilled trades occups." in lower_str:
        return "Skilled Trades Occupations"
    elif "caring, leisure and other occ" in lower_str:
        return "Caring, Leisure, Other Occupations"
    elif "sales & consumer service occ" in lower_str:
        return "Sales & Consumer Service Occupations"
    elif "process, plant & mach" in lower_str:
        return "Process, Plant & Machine Operatives"
    elif "elementary occup" in lower_str:
        return "Elementary Occupations"
    else:
        print(f"Could not map string '{str}' to occupation")
        return None

def get_sic_and_sector(str):
    if "A:" in str:
        return ("A", "Agriculture & Fishing")
    elif "B,D,E:" in str:
        return ("B,D,E", "Energy & Water")
    elif "C:" in str:
        return ("C", "Manufacturing")
    elif "F:" in str:
        return ("F", "Construction")
    elif "G,I:" in str:
        return ("G,I", "Distribution, Hotels and Restaurants")
    elif "H,J:" in str:
        return ("H,J", "Transport and Communication")
    elif "K-N:" in str:
        return ("K-N", "Banking, Finance, and Insurance")
    elif "O-Q:" in str:
        return ("O-Q", "Public Admin, Education and Health")
    elif "R-U:" in str:
        return ("R-U", "Other Services")
    elif "G-U:" in str:
        return ("G-U", "Total Services")
    else:
        print(f"Could not map string '{str}' to sector")
        return None

def get_gender(str):
    lower_str = str.lower()
    if "females" in lower_str:
        return "Female"
    elif "males" in lower_str:
        return "Male"
    else:
        return 'All'

def get_age(str):
    lower_str = str.lower()
    if "16+" in lower_str:
        return "16+"
    elif "16-64" in lower_str:
        return "16-64"
    elif "16-19" in lower_str:
        return "16-19"
    elif "20-24" in lower_str:
        return "20-24"
    elif "25-34" in lower_str:
        return "25-34"
    elif "35-49" in lower_str:
        return "35-49"
    elif "50+" in lower_str:
        return "50+"
    elif "50-64" in lower_str:
        return "50-64"
    elif "65+" in lower_str:
        return "65+"
    elif "16-24" in lower_str:
        return "16-24"
    else:
        print(f"Could not map string '{str}' to age")
        return None

kpis = [
    {   'name': 'empl_rate_by_occup',
        'url':  f'{nomis_api_base_url}/NM_17_5.data.csv?geography={nation_codes.get("UK")}&date={interval}&variable=1634...1642,1652...1696&measures=20599&select=date_name,geography_code,geography_name,obs_value,variable_name',
        'file': 'Employment_Rate_Ethnicity_by_Occupation',
        'dir':  'employment_rate_ethnicity_by_occupation',
        'valid_header': ['Date', 'Geocode', 'Geography', 'Employment_Rate_Ethnicity_by_Occupation', 'Ethnicity', 'Occupation'],
        'unique_in_cols': [1, 2, 5],        # used to find duplicates
        'non_null_out_cols': [1, 2, 5, 6],  # used to find invalid
        'process_fn': process_occupation_record,
    },
    {   'name': 'empl_rate_by_sector',
        'url':  f'{nomis_api_base_url}/NM_17_5.data.csv?geography={nation_codes.get("UK")}&date={interval}&variable=1393...1402,1413...1462&measures=20599&select=date_name,geography_code,geography_name,obs_value,variable_name',
        'file': 'Employment_Rate_Ethnicity_by_Sector',
        'dir':  'employment_rate_ethnicity_by_sector',
        'valid_header': ['Date', 'Geocode', 'Geography', 'Employment_Rate_Ethnicity_by_Sector', 'Ethnicity', 'SIC', 'Sector'],
        'unique_in_cols': [1, 2, 5],
        'non_null_out_cols': [1, 2, 5, 6, 7],
        'process_fn': process_sector_record,
    },
    {   'name': 'empl_vol',
        'url':  f'{nomis_api_base_url}/NM_17_5.data.csv?geography={dstr_mcty_cty_codes}&date={interval}&variable=45&measures=21001&select=date_name,geography_code,geography_name,obs_value',
        'file': 'Employment_16plus',
        'dir':  'employment_volume',
        'valid_file': 'valid_CTY_and_LA_Employment_16plus',
        'valid_header': ['Date', 'Geocode', 'Geography', 'Value'],
        'unique_in_cols': [1, 2],
        'non_null_out_cols': [1, 2],
        'process_fn': process_empl_voml_record,
    },
]

max_row_count = 25000 # max number of rows in a data file
threads = 8
local_landing_dir = './landing'
local_valid_dir = './valid'
local_error_dir = './error'
s3_landing_dir  = 'landing'
s3_valid_dir = 'valid'
api_urls = []
file_headers = []
errors = []
geo_master_file = 'geo_master_mapping.csv'
geo_mappings = []
geo_codes = set()


def get_time():
    return f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\t"

def suffix_filename(path, suffix):
    file_name, file_ext = os.path.splitext(path)
    return f'{file_name}{suffix}{file_ext}'

def prefix_filename(path, prefix):
    file_name, file_ext = os.path.splitext(path)
    return f'{prefix}{file_name}{file_ext}'

def append_ext_if_missing(path, extension='.csv'):
    file_name, file_ext = os.path.splitext(path)
    # out =
    return f'{file_name}{extension}' if not file_ext else f'{file_name}{file_ext}'

def is_file_empty(file_path):
    # Check if file exist and it is empty
    return os.path.isfile(file_path) and os.path.getsize(file_path) == 0

def get_number_records(file):
    try:
        with open(file, 'r') as data:
            return len(list(csv.DictReader(data)))
    except Exception as e:
        logging.error(f"Could not get number of records from data file {file}:\n{str(e)}")

def update_query_params(url, params):
    url_parse = urlparse(url)
    query = url_parse.query
    url_dict = {key: value for (key, value) in parse_qsl(query)} # = dict(parse_qsl(query))
    url_dict.update(params)
    new_query = urlencode(url_dict)
    url_parse = url_parse._replace(query=new_query)
    return urlunparse(url_parse)

def get_record_count(url):
    params = { "select":"record_count", "RecordLimit":"1", "ExcludeColumnHeadings":"true" }
    return int(requests.get(update_query_params(url, params), stream=True).text)

def download_file_part_from_api(entry):
    url, file, tot_count, parts, part = entry
    if parts == 1:
        print(f"{get_time()}Downloading {file.split('/')[-1]} [{tot_count} records]...")
    else:
        part_count = max_row_count if part < (parts - 1) else tot_count - (max_row_count * (parts - 1))
        print(f"{get_time()}Downloading {file.split('/')[-1]} [{part_count} records] - part {part+1} of {parts}...")
    Path(os.path.dirname(os.path.abspath(file))).mkdir(parents=True, mode=0o777, exist_ok=True)
    req = requests.get(url, stream=True)
    with open(file, 'wb') as csv:
        for chunk in req.iter_content(chunk_size = 1024):
            if chunk:
                csv.write(chunk)

def concatenate_file_parts(input_file_parts, output_file):
    print(f"{get_time()}Creating {os.path.basename(output_file)} by concatenating the {len(input_file_parts)} parts. Deleting parts")
    with open(output_file, "a") as fout:
        for part_num, file_part in enumerate(input_file_parts):
            with open(file_part) as fin:
                if part_num > 0:
                    next(fin)
                for line in fin:
                    fout.write(line)
            os.remove(file_part)

def download_file_from_api(entry):
    url, file = entry
    try:
        record_count = get_record_count(url)
    except Exception as e:
        logging.error(f"Could not get record count for {os.path.basename(file)} from API:\n{str(e)}")
    else:
        try:
            parts = -(record_count // -max_row_count)
            api_parts = []
            for part in range(parts):
                params = {"RecordOffset": part * max_row_count, "RecordLimit": max_row_count}
                url_part = update_query_params(url, params) # if parts > 1 else url
                file_part = suffix_filename(file, f'_part_{part+1}') if parts > 1 else file
                api_parts.append((url_part, file_part, record_count, parts, part))

            for download in ThreadPool(threads).imap_unordered(download_file_part_from_api, api_parts): pass

            if parts > 1:
                concatenate_file_parts([api_part[1] for api_part in api_parts], file)
        except Exception as e:
            logging.error(f"Could not download file {file.split('/')[-1]} from API:\n{str(e)}")
        # else: print(f"{get_time()}Downloaded {file.split('/')[-1]}")

def read_csv(file, cols=[]):
    with open(file, 'r') as data:
        reader = csv.DictReader(data)
        if cols:
            validate(reader.fieldnames, cols)
        return list(reader)

def validate_file(entry):
    headers, file = entry
    files = filter_files_in_dir_by_name('', file=file)
    for file in files:
        print(f"{get_time()}Validating {file.split('/')[-1]} ...")
        try:
            with open(file, 'r') as data:
                reader = csv.DictReader(data)
                validate(reader.fieldnames, headers)
                rows = len(list(reader))
        except Exception as e:
            logging.error(f"Invalid headers for file {file.split('/')[-1]}:\n{str(e)}")
        else:
            if rows == 0:
                logging.error(f"File {file.split('/')[-1]} has no rows")
            # elif rows >= max_row_count:
            #     logging.error(f"File {file.split('/')[-1]} has reached the max number of rows [{max_row_count}]")
            # else: print(f"{get_time()}Succesfully validated {file.split('/')[-1]}")

def upload_obj_to_s3(bucket, obj_name, local_path):
    try:
        file_name = os.path.split(os.path.abspath(local_path))[1]
        print(f"{get_time()}Uploading {file_name} to S3 {bucket + '/' + obj_name[:obj_name.rfind('/')+1]}")
        s3_res.meta.client.upload_file(local_path, bucket, obj_name)
    except Exception as e:
        logging.error(f"Could not upload object {obj_name} to S3 bucket {bucket}:\n{str(e)}")

def upload_dir_to_s3(*dirs, file='', bucket, bucket_dir):
    local_paths = filter_files_in_dir_by_name(*dirs, file=file)
    for idx, local_path in enumerate(local_paths):
        obj_name = os.path.split(local_path)[1]
        upload_obj_to_s3(bucket, f'{bucket_dir}/{obj_name}', local_path)

def download_obj_from_s3(bucket, obj_name, local_dir, file_name):
    print(f"{get_time()}Downloading {file_name} from S3 bucket {bucket}")
    try:
        s3_cli.download_file(bucket, obj_name, f'{os.path.join(local_dir, file_name)}')
    except Exception as e:
        logging.error(f"Could not download object {obj_name} from S3 bucket {bucket}:\n{str(e)}")

def download_dir_from_s3(bucket, bucket_dir, local_dir = './'):
    try:
        print(f"{get_time()}Downloading folder {bucket_dir} from S3 bucket {bucket}")
        paginator = s3_cli.get_paginator('list_objects')
        for result in paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=bucket_dir):
            if result.get('CommonPrefixes') is not None:
                for subdir in result.get('CommonPrefixes'):
                    download_dir_from_s3(bucket, subdir.get('Prefix'), local_dir)
            for file in result.get('Contents', []):
                dest_pathname = os.path.join(local_dir, file.get('Key'))
                if not os.path.exists(os.path.dirname(dest_pathname)):
                    os.makedirs(os.path.dirname(dest_pathname))
                try:
                    s3_cli.download_file(bucket, file.get('Key'), dest_pathname)
                except Exception as e:
                    logging.error(f"Could not download object {file.get('Key')} from S3 bucket {bucket}:\n{str(e)}")
    except Exception as e:
        logging.error(f"Could not download folder {bucket_dir} from S3 bucket {bucket}:\n{str(e)}")

def delete_if_exists(path):
    if os.path.exists(path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            try:
                os.remove(path)
            except OSError:
                pass

def log_summary(summary):
    valid_file = os.path.basename(summary['valid_file'])
    print(f"\t\t\tGenerated {valid_file}")
    print(f"\t\t\tRecords: {summary['total']} total, {summary['valid']} valid, {summary['duplicate']} duplicate, {summary['invalid']} invalid")
    if min(summary['dates']) == max(summary['dates']):
        print(f"\t\t\tDates: {len(summary['dates'])} value = {list(summary['dates'])[0]}")
    else:
        print(f"\t\t\tDates: {len(summary['dates'])} distinct values between {min(summary['dates'])} and {max(summary['dates'])}")
    print(f"\t\t\tGeo codes: {summary['codes_len']} distinct values")
    if summary['invalid'] > 0:
        logging.error(f"*** ATTENTION - File {valid_file} includes invalid records ***")

def read_geo_codes():
    with open(f'{os.path.join(local_valid_dir, geo_master_file)}', 'r') as data:
        global geo_codes
        print(f"{get_time()}Reading geo codes from master mapping {geo_master_file}")
        rows = csv.DictReader(data)
        for row in rows:
            geo_codes.add(row['LocalAuthority_Code'])
            geo_codes.add(row['County_code'])
            geo_codes.add(row['Region_Code'])
            geo_codes.add(row['National3_Code'])
            geo_codes.add(row['National2_Code'])
            geo_codes.add(row['National1_Code'])
        print(f'{get_time()}{len(geo_codes)} distinct geo codes in {geo_master_file}')

def is_valid_geo_code(code):
    # return get_geo_name_by_code(code)['value'] is not None
    if not geo_codes:
        read_geo_codes()
    return code in geo_codes

def read_geo_master_file():
    with open(f'{os.path.join(local_valid_dir, geo_master_file)}', 'r') as data:
        global geo_mappings
        print(f"{get_time()}Reading geo master mapping {geo_master_file}")
        rows = csv.DictReader(data)
        geo_mappings = list(rows)
        print(f'{get_time()}{len(geo_mappings)} distinct geo mappings in {geo_master_file}')

def get_geo_name_by_code(code):
    output_record = {"level": None, "value": None}
    if not geo_mappings:
        read_geo_master_file()
    for geo_mapping in geo_mappings:
        if geo_mapping['LocalAuthority_Code'] == code:
            output_record = {'level': 'LocalAuthority', 'value': geo_mapping['LocalAuthority']}
            break
        elif geo_mapping['County_code'] == code:
            output_record = {'level': 'County', 'value': geo_mapping['County']}
            break
        elif geo_mapping['Region_Code'] == code:
            output_record = {'level': 'Region', 'value': geo_mapping['Region']}
            break
        elif geo_mapping['National3_Code'] == code:
            output_record = {'level': 'National3', 'value': geo_mapping['National3']}
            break
        elif geo_mapping['National2_Code'] == code:
            output_record = {'level': 'National2', 'value': geo_mapping['National2']}
            break
        elif geo_mapping['National1_Code'] == code:
            output_record = {'level': 'National1', 'value': geo_mapping['National1']}
            break
    return output_record

def get_geocode_set(url, param):
    code_list = []
    for qp in get_query_param(url, param):
        rng = qp.split('...')
        if len(rng) == 1:
            code_list.append(int(rng[0]))
        elif len(rng) == 2:
            code_list.extend(list(range(int(rng[0]), int(rng[1])+1)))
    return set(code_list)

def process_val(value, is_percent):
    try:
        if is_percent:
            return round(float(value)/100, 7)
        else:
            f = float(value)
            if f.is_integer():
                return int(f)
            return round(f, 7)
    except ValueError:
        return None

def get_start_date(date_interv, date_format = '%b %Y', separator = '-'):
    # Defaults work for 'Jan 2019-Dec 2019'
    try:
        x = date_interv.split(separator)
        date_time_obj = datetime.datetime.strptime(x[0], date_format)
        return date_time_obj.date()
    except:
        return None

def try_int(s):
    try:
        return int(s)
    except:
        return s

def alphanum_key(s):
    return [try_int(c) for c in re.split('([0-9]+)', s)]

def filter_files_in_dir_by_name(*dirs, file=''):
    file_name, file_ext = os.path.splitext(file)
    files = glob.glob(os.path.join(os.path.join(*dirs), file_name) + '*' + file_ext)
    files.sort(key=alphanum_key)
    return files


if __name__ == "__main__":
    clearConsole = lambda: os.system('cls' if os.name in ('nt', 'dos') else 'clear')
    clearConsole()
    print(f'{get_time()}Starting ETL - environment = {environment}, bucket = {bucket}, periods: {quarters}')

    # Recreate local landing and error folders
    delete_if_exists(local_landing_dir)
    os.makedirs(local_landing_dir, mode=0o777, exist_ok=True)
    delete_if_exists(local_error_dir)
    os.makedirs(local_error_dir, mode=0o777, exist_ok=True)

    logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    rootLogger = logging.getLogger()
    error_log = os.path.join(local_error_dir, datetime.datetime.now().strftime('%Y_%m_%d__%H_%M_%S') + '.log')

    fileHandler = logging.FileHandler(error_log)
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)
    consoleHandler = logging.StreamHandler()
    # consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)

    for kpi in kpis:
        kpi['landing_header']= [each.upper() for each in get_query_param(kpi['url'], 'select')]
        kpi['landing_file'] = append_ext_if_missing(kpi.get('landing_file') if 'landing_file' in kpi else 'landing_' + kpi['file'])
        kpi['valid_file'] = append_ext_if_missing(kpi.get('valid_file') if 'valid_file' in kpi else 'valid_' + kpi['file'])

        fn = os.path.join(local_landing_dir, kpi['dir'], kpi['landing_file'])
        api_urls.append((kpi['url'], fn))
        file_headers.append((kpi['landing_header'], fn))

    if download_landing_from_bucket or upload_landing_to_bucket or upload_valid_to_bucket or verify_geocodes:
        if is_dev_machine:
            account = {'id': '124329112208', 'name': 'PDP_DEV', 'role': 'role_admin'}
            session_profile = 'pdp-dev-admin'
            role_arn = 'arn:aws:iam::' + str(account.get('id')) + ':role/' + account.get('role')

            session = boto3.Session(profile_name=session_profile, region_name=region,)
            sts = session.client("sts")
            response = sts.assume_role(RoleArn=role_arn, RoleSessionName="nomisEtlSession")
            credentials = response["Credentials"]
            assumed_role_session = boto3.Session(aws_access_key_id = credentials["AccessKeyId"],
                                                 aws_secret_access_key = credentials["SecretAccessKey"],
                                                 aws_session_token = credentials["SessionToken"])
            s3_res = assumed_role_session.resource('s3')
            s3_cli = assumed_role_session.client('s3')
        else:
            s3_res = boto3.resource('s3')
            s3_cli = boto3.client('s3')

    if download_landing_from_bucket:
        # Download files from S3/landing
        for kpi in kpis:
            download_dir_from_s3(bucket, f"{s3_landing_dir}/{kpi['dir']}/")
        print("-"*80)
    else:
        # Download files from API
        for file in ThreadPool(threads).imap_unordered(download_file_from_api, api_urls): pass
        print("-"*80)

        # Validate files: headers and number of records
        for file in ThreadPool(threads).imap_unordered(validate_file, file_headers): pass
        print("-"*80)

        if upload_landing_to_bucket: # Upload files to S3/landing
            for kpi in kpis:
                upload_dir_to_s3(local_landing_dir, kpi['dir'], file=kpi['landing_file'],
                                 bucket=bucket, bucket_dir=f"{s3_landing_dir}/{kpi['dir']}")
            print("-"*80)

    if process_landing:
        # Recreate local valid folder
        delete_if_exists(local_valid_dir)
        os.makedirs(local_valid_dir, mode=0o777, exist_ok=True)

        if verify_geocodes:
            download_obj_from_s3(bucket, f'{s3_valid_dir}/master_mapping/all/all.csv', local_valid_dir, geo_master_file)
            print("-"*80)

        # Process each landing file to create a valid file
        for kpi in kpis:
            landing_files = filter_files_in_dir_by_name(local_landing_dir, kpi['dir']) # file = kpi['landing_file']
            if len(landing_files) > 0:
                found = {}
                valid, duplicate, invalid, total = 0, 0, 0, 0
                dates, codes = set(), set()
                try:
                    for idx, landing_file in enumerate(landing_files):
                        print(f"{get_time()}Processing file {os.path.basename(landing_file)} [{idx+1}]")
                        csv_records = read_csv(landing_file, kpi['landing_header'])

                        if csv_records:
                            valid_file = os.path.abspath(os.path.join(local_valid_dir, kpi['dir'], kpi['valid_file']))
                            Path(os.path.dirname(valid_file)).mkdir(parents=True, mode=0o777, exist_ok=True)

                            with open(valid_file, 'a') as csv_valid_file:
                                writer = csv.writer(csv_valid_file, quoting=csv.QUOTE_NONNUMERIC) # QUOTE_ALL  QUOTE_MINIMAL
                                if idx == 0:
                                    writer.writerow(kpi['valid_header']) # header added only once

                                total += len(csv_records)

                                for in_record in csv_records:
                                    id_key = tuple([in_record[kpi['landing_header'][col-1]] for col in kpi['unique_in_cols']])

                                    if not id_key in found:
                                        found[id_key] = in_record

                                        # call kpi specific processing function
                                        out_record = kpi['process_fn'](kpi['landing_header'], in_record)

                                        if all(tuple([out_record[col-1] for col in kpi['non_null_out_cols']])) \
                                            and (not verify_geocodes or is_valid_geo_code(out_record[1])):

                                            valid += 1
                                            dates.add(out_record[0])
                                            codes.add(out_record[1])
                                            out_record[0] = out_record[0].strftime("%d/%m/%Y")

                                            writer.writerow(out_record)
                                        else:
                                            invalid += 1
                                    else:
                                        duplicate += 1

                    log_summary({'total': total, 'valid': valid, 'duplicate': duplicate, 'invalid': invalid,
                                 'dates': dates, 'codes_len': len(codes), 'landing_file': landing_file, 'valid_file': valid_file})

                    if upload_valid_to_bucket:
                        upload_dir_to_s3(local_valid_dir, kpi['dir'], file=kpi['valid_file'],
                                         bucket=bucket, bucket_dir=f"{s3_valid_dir}/{kpi['dir']}")
                except Exception as e:
                    logging.error(f"Could not process file {os.path.basename(landing_file)}:\n{str(e)}")
                finally:
                    print("-"*80)

    if is_file_empty(error_log):
        print(f"{get_time()}ETL completed successfully")
    else:
        print(f"{get_time()}ETL completed with errors. Details logged in {(error_log)}")
        if upload_landing_to_bucket or upload_valid_to_bucket:
            upload_dir_to_s3(local_error_dir, bucket=bucket, bucket_dir='error')
        # sys.stderr.write('true')
