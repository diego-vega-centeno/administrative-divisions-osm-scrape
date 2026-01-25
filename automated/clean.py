# init
import os
from pathlib import Path
import boto3
import pandas as pd
import re
import sys
import subprocess
import copy
from dotenv import load_dotenv

import toolsGeneral.logger as tgl
import toolsGeneral.main as tgm
import toolsOSM.overpass as too
import toolsSync.main as tsm

#* Initialize setup
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SAVE_DIR = DATA_DIR / 'cleaned'
RAW_DIR = DATA_DIR / 'raw/osm countries queries'
DEV_MODE = False

logger = tgl.initiate_logger('logger', SAVE_DIR / 'cleaned.log')

#* initialize git
subprocess.run(["git", "config", "--global", "--add", "safe.directory", "/app"], check=True)
subprocess.run(["git", "config", "--global", "user.name", "github-actions[bot]"])
subprocess.run(["git", "config", "--global", "user.email", "github-actions[bot]@users.noreply.github.com"])

token = os.environ.get("GITHUB_TOKEN")
if token:
    subprocess.run([
        "git", "remote", "set-url", "origin",
        f"https://x-access-token:{token}@github.com/CopaCabana21/administrative-divisions-osm-scrape.git"
    ])
    subprocess.run(["git", "pull", "--rebase"], check=True)

#* setup b2
bucket_name = os.environ["B2_BUCKET_NAME"]
session = boto3.session.Session()
s3 = session.client(
    service_name="s3",
    aws_access_key_id=os.environ["B2_KEY_ID"],
    aws_secret_access_key=os.environ["B2_APPLICATION_KEY"],
    endpoint_url=os.environ["B2_ENDPOINT"]
)

#* download from b2
process_state_file = DATA_DIR / "process_state.json"
tsm.download_file_from_bucket(bucket_name, process_state_file.relative_to(ROOT), s3, process_state_file, logger)

#* load state and meta data files
process_state = tgm.load(process_state_file)

#* select entities to process
countries_cleaned = [c for c, val in process_state.items() if (val['clean']['status'] == 'ok')]
logger.info(f'countries cleaned: {len(countries_cleaned)}')
countries_to_clean = [c for c, val in process_state.items() if (val['scrape']['status'] == 'ok') and (val['clean']['status'] in ['pending', 'error'])]
logger.info(f'countries to clean: {len(countries_to_clean)}')

# schedule countries
to_scrape = [
    ('France', '2202162', ['4', '6', '8']),
    ('Canada', '1428125', ['4', '6', '8']), 
    ('Peru', '288247', ['4', '6', '8']),
    ('Germany', '51477', ['4', '6', '8'])
]

if len(countries_to_clean) < 1:
    logger.info("No countries to clean, exiting script")
    sys.exit(0)

#* load environment variables
load_dotenv()

#* Use AWS kit to upload files
logger.info(f"* initializing b2 ...")
session = boto3.session.Session()

s3 = session.client(
    service_name="s3",
    aws_access_key_id=os.environ["B2_KEY_ID"],
    aws_secret_access_key=os.environ["B2_APPLICATION_KEY"],
    endpoint_url=os.environ["B2_ENDPOINT"]
)
logger.info(f"* finished b2")

#* Download required data
logger.info(f"* Downloading required raw data to clean")
logger.info(f"  * Downloading country data from B2 in directory: '{RAW_DIR.relative_to(ROOT)}'")

countries_to_clean_in_b2 = tsm.donwload_country_data_from_bucket(countries_to_clean, os.environ["B2_BUCKET_NAME"], RAW_DIR.relative_to(ROOT), RAW_DIR, s3, logger)

if len(countries_to_clean_in_b2) < 1:
    logger.info("No countries to clean found in B2, exiting script")
    sys.exit(0)

#* load data for countries to clean
logger.info(f'* Countries to clean: {len(countries_to_clean)}')
logger.info(f"* Load raw data for countries to clean in B2: {len(countries_to_clean_in_b2)}")
country_raw_dirs = [f for f in (DATA_DIR / 'raw/osm countries queries').glob('*') if f.is_dir()]
logger.info(f"  * Total of raw data directories found: {len(country_raw_dirs)}")

if len(country_raw_dirs) < 1:
    logger.info("No raw data found for countries to clean, exiting script")
    sys.exit(0)


to_clean_by_cntr = {}
# for chunks and non chunk files
for country in countries_to_clean_in_b2:
    country_dir = DATA_DIR / 'raw/osm countries queries' / country
    if not country_dir.exists():
        continue
    files_elements = [tgm.load(f)['elements'] for f in country_dir.glob('*.json')]
    elements = [ele for list in files_elements for ele in list]
    to_clean_by_cntr[country] = elements

logger.info(f"  * Number of countries with raw data loaded: {len(to_clean_by_cntr)}")
logger.info(f"  * Countries to clean without raw data: {tgm.complement(countries_to_clean_in_b2, to_clean_by_cntr.keys())}")



#* START CLEANING STEPS
logger.info(f"* Start cleaning steps")
cleaned_by_cntr = copy.deepcopy(to_clean_by_cntr)

#* Use sovereign countries only
# logger.info(f"* Use sovereign countries only")
# sovereign_countries = tgm.load(DATA_DIR / 'sovereign countries.json')
# logger.info(f"  * Sovereign countries: {len(sovereign_countries)}")

# cleaned_by_cntr = {k:data for k,data in to_clean_by_cntr.items() if k in sovereign_countries}
# logger.info(f"  * Filtered sovereign countries: {len(cleaned_by_cntr)}")

#* clean countries
logger.info(f"* Clean countries")

def clean_country_data(raw_by_cntr):

    cleaned_by_cntr = copy.deepcopy(raw_by_cntr)  

    for country, raw_data in cleaned_by_cntr.items():

        # convert id to string
        for ele in raw_data:
            ele['id'] = str(ele['id'])

        # add parent_id to level 4 entities
        cntr_id = list(filter(lambda ele: ele['tags']['admin_level'] == '2', raw_data))[0]['id']
        for ele in raw_data:
            if ele['tags']['admin_level'] == '4':
                ele['tags']['parent_id'] = str(cntr_id)

        # add country_name and country_id tag
        for ele in raw_data:
            ele['tags']['country_name'] = country
            ele['tags']['country_id'] = cntr_id

    return cleaned_by_cntr

cleaned_by_cntr = clean_country_data(cleaned_by_cntr)
logger.info(f"  * Cleaned countries: {len(cleaned_by_cntr)}")

temp = [ele['tags'].get('parent_id', None) for k,v in cleaned_by_cntr.items() for ele in v]
logger.info(f"  * Tally 'parent_id' for all elements: {tgm.tally([type(ele) for ele in temp])}")

#* convert to dataframe
logger.info(f"* Convert to dataframe")
cleaned_by_cntr = {k:too.normalizeOSM(elems) for k,elems in cleaned_by_cntr.items()}
logger.info(f"  * Cleaned converted to data frame: {len(cleaned_by_cntr)}")

combined = pd.concat(cleaned_by_cntr.values(), ignore_index=True)
logger.info(f"  * Tally all types of values in dataframe: {tgm.tally(list(combined.map(type).stack().values))}")

#* save files
logger.info(f"Finished cleaning. Total of cleaned countries: {len(cleaned_by_cntr)}")

for country,df in cleaned_by_cntr.items():
    tgm.dump(SAVE_DIR / country / f'{country}_cleaned.pkl', df)
logger.info(f"Saved files to directory '{SAVE_DIR}' : {len(cleaned_by_cntr)}")

#* Upload data to backblaze b2 and update process state
logger.info("* Uploading data to backblaze b2")
config = {'root':ROOT, 's3':s3, 'logger':logger}
for country in cleaned_by_cntr.keys():
    country_save_dir = SAVE_DIR / country
    process_status = 'ok'
    process_error = None
    # all data in country directory will  be uploaded
    if not DEV_MODE:
        upload_response = tsm.upload_dir_files_to_backblaze(country_save_dir, config)
        process_status = upload_response['status']
        process_error = upload_response['status_type']
    # override process task state with upload response
    logger.info(f"  * Updating {country} in process state: (clean, ok)")
    tsm.update_process_state(process_state, country, 'clean', process_status=process_status, process_error=process_error)
    tgm.dump(process_state_file, process_state)
    if not DEV_MODE:
        tsm.upload_file_to_backblaze(process_state_file, config)
        # tsm.commit_file(process_state_file, f"Update process state for {country}: (clean, ok)", config['logger'])