#!/usr/bin/env python
# coding: utf-8

# init
import os
import time
from pathlib import Path
import boto3
import subprocess
import sys
from dotenv import load_dotenv

import toolsGeneral.logger as tgl
import toolsGeneral.main as tgm
import toolsOSM.overpass as too
import toolsSync.main as tsm

# Initialize variables
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SAVE_DIR = DATA_DIR / 'raw/osm countries queries'
DEV_MODE = False

# load environment variables for local run
load_dotenv()

token = os.environ.get("GITHUB_TOKEN")
if token:
    # initialize git
    subprocess.run(["git", "config", "--global", "--add", "safe.directory", "/app"], check=True)
    subprocess.run(["git", "config", "--global", "user.name", "github-actions[bot]"])
    subprocess.run(["git", "config", "--global", "user.email", "github-actions[bot]@users.noreply.github.com"])

    subprocess.run([
        "git", "remote", "set-url", "origin",
        f"https://x-access-token:{token}@github.com/CopaCabana21/administrative-divisions-osm-scrape.git"
    ])
    subprocess.run(["git", "pull", "--rebase"], check=True)

# initialize logger
logger = tgl.initiate_logger('logger', DATA_DIR / 'raw/raw_scrape.log')

# setup b2
bucket_name = os.environ["B2_BUCKET_NAME"]
session = boto3.session.Session()
s3 = session.client(
    service_name="s3",
    aws_access_key_id=os.environ["B2_KEY_ID"],
    aws_secret_access_key=os.environ["B2_APPLICATION_KEY"],
    endpoint_url=os.environ["B2_ENDPOINT"]
)

# download from b2
process_state_file = DATA_DIR / "process_state.json"
tsm.download_file_from_bucket(bucket_name, process_state_file.relative_to(ROOT), s3, process_state_file, logger)
country_meta_file = DATA_DIR / "osmMetaCountrDict.json"
tsm.download_file_from_bucket(bucket_name, country_meta_file.relative_to(ROOT), s3, country_meta_file, logger)
# load state and meta data files
osmMetaCountrDict = tgm.load(country_meta_file)
process_state = tgm.load(process_state_file)

# filter countries to scrape
processed_countries = [country for country, country_state in process_state.items() if country_state['scrape']['status'] == 'ok']
failed_countries = [country for country, country_state in process_state.items() if country_state['scrape']['status'] == 'failed']
to_scrape_countries = [country for country, country_state in process_state.items() if country_state['scrape']['status'] in ['pending', 'error']]
to_scrape = [(country, osmMetaCountrDict[country]['id'], osmMetaCountrDict[country]['addLvlsNum']) for country in to_scrape_countries]

if len(to_scrape_countries) < 1:
    logger.info("* No countries to scrape, exiting script")
    sys.exit(0)

# test to scrape
to_scrape = to_scrape[:10]
# to_scrape = [('UnitedStates', '148838', ['4', '6', '8'])]

logger.info(f"* processed_countries: {len(processed_countries)}")
logger.info(f"* failed_countries: {len(failed_countries)}")
logger.info(f"* to process countries: {len(to_scrape)}")

# Use AWS kit to upload files
logger.info(f"* initializing b2 ...")
session = boto3.session.Session()

s3 = session.client(
    service_name="s3",
    aws_access_key_id=os.environ["B2_KEY_ID"],
    aws_secret_access_key=os.environ["B2_APPLICATION_KEY"],
    endpoint_url=os.environ["B2_ENDPOINT"]
)
logger.info(f"* finshed b2")

in_chunks_countries = ['China','Armenia']

def scrape_country_in_chunks(tuple, save_dir, country_save_file, config, process_state, process_state_file):
    logger.info(f"* Scrape in chunks started")
    country, id, lvls = tuple
    process_state[country]['scrape']['type'] = 'chunk'
    process_state[country]['scrape']['chunk_state'] = {}
    chunk_state = process_state.get('chunk_state')

    response = too.getOSMIDAddsStruct_chunks((country, id, lvls), save_dir, chunk_state)
    logger.info(f"* Scrape in chunks finished: {response['status']} - {response['status_type']}")

    state_resume = {k:{k2:(len(v2) if type(v2) == set else v2) for k2,v2 in val.items()} for k,val in response['data'].items()}
    logger.info(f"  - Chunk status: {state_resume}")

    process_status = response['status']
    process_error = response['status_type']

    if response["status"] == "ok":
        # Try to upload data and override process status with upload result from B2
        logger.info("* Upload data to backblaze b2")
        if not DEV_MODE:
            upload_response = tsm.upload_dir_files_to_backblaze(country_save_file.parent, config)
            process_status = upload_response['status']
            process_error = upload_response['status_type']

    logger.info(f"* Update and upload process state: {country} - {(process_status, process_error)}")
    process_state[country]['scrape']['chunk_state'] = response['data']
    tsm.update_process_state(process_state, country, 'scrape', process_status=process_status, process_error=process_error)
    tgm.dump(process_state_file, process_state)
    if not DEV_MODE:
        tsm.upload_file_to_backblaze(process_state_file, config)
        # tsm.commit_file(process_state_file, f"[automated] Update process state: {country}: (scrape, {process_status})", config['logger'])

# fetch admin
for country, id, lvls in to_scrape:

    config = {'root':ROOT, 's3':s3, 'logger':logger}
    country_save_file = SAVE_DIR / country / f'rawOSMRes.json'
    logger.info(f"* processing: {country, id, lvls}")

    # if country not in in_chunks_countries:
        
    #     response = too.getOSMIDAddsStruct(id, lvls)
    #     logger.info(f"  - Scrape for country {country} result: {response['status']}")

    #     # Use chunks if there are too many requests
    #     if '429' in response["status_type"] or 'timeout' in response["status_type"]:
    #         logger.info(f"  - Too many requests/timeout error, using chunks", logger)
    #         scrape_country_in_chunks((country, id, lvls), SAVE_DIR, country_save_file, config, process_state, process_state_file)
    #         continue
        
    #     process_status = response['status']
    #     process_error = response['status_type']
    #     # Try to upload data and override process status with upload result from B2
    #     if response["status"] == "ok":
    #         tgm.dump(country_save_file, response["data"])
    #         logger.info("  - Upload data to backblaze b2")
    #         if not DEV_MODE:
    #             upload_response = tsm.upload_dir_files_to_backblaze(country_save_file.parent, config)
    #             process_status = upload_response['status']
    #             process_error = upload_response['status_type']
        
    #     logger.info(f"  - Update and commit to process state: {country} - ({process_status, process_error})")
    #     tsm.update_process_state(process_state, country, 'scrape', process_status=process_status, process_error=process_error)
    #     tgm.dump(process_state_file, process_state)
    #     if not DEV_MODE:
    #         tsm.commit_file(process_state_file, f"[automated] Update process state: {country}: (scrape, {process_status})", config['logger'])
    # else:
    scrape_country_in_chunks((country, id, lvls), SAVE_DIR, country_save_file, config, process_state, process_state_file)

    time.sleep(3)

processed_countries = [country for country, country_state in process_state.items() if country_state['scrape']['status'] == 'ok']
failed_countries = [country for country, country_state in process_state.items() if country_state['scrape']['status'] == 'failed']

logger.info(f"* new total of processed_countries: {len(processed_countries)}")
logger.info(f"* new total of failed_countries: {len(failed_countries)}")
