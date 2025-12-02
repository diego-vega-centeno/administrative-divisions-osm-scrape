#!/usr/bin/env python
# coding: utf-8

# init
import os
import time
from pathlib import Path
import boto3
import subprocess
import tools.upload_and_commit as tools

import toolsGeneral.logger as tgl
import toolsGeneral.main as tgm
import toolsOSM.overpass as too

# Initialize variables
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SAVE_DIR = DATA_DIR / 'raw/osm countries queries'
DEV_MODE = False

# initialize git
subprocess.run(["git", "config", "--global", "--add", "safe.directory", "/app"], check=True)
subprocess.run(["git", "config", "--global", "user.name", "github-actions[bot]"])
subprocess.run(["git", "config", "--global", "user.email", "github-actions[bot]@users.noreply.github.com"])

if not DEV_MODE:
    subprocess.run([
        "git", "remote", "set-url", "origin",
        f"https://x-access-token:{os.environ['GITHUB_TOKEN']}@github.com/CopaCabana21/automated-add-osm-scrape.git"
    ])
    subprocess.run(["git", "pull", "--rebase"], check=True)

# initialize logger
logger = tgl.initiate_logger('logger', DATA_DIR / 'raw/raw_scrape.log')

# load state and meta data files
osmMetaCountrDict = tgm.load(DATA_DIR / "osmMetaCountrDict.json")
process_state_file = DATA_DIR / "process_state.json"
process_state = tgm.load(process_state_file)

# filter countries to scrape
processed_countries = [country for country, country_state in process_state.items() if country_state['scrape']['status'] == 'ok']
failed_countries = [country for country, country_state in process_state.items() if country_state['scrape']['status'] == 'failed']
to_scrape_countries = [country for country, country_state in process_state.items() if country_state['scrape']['status'] == 'pending']
to_scrape = [(country, osmMetaCountrDict[country]['id'], osmMetaCountrDict[country]['addLvlsNum']) for country in to_scrape_countries]

# test to scrape
# to_scrape = to_scrape[:2]
to_scrape = [('Armenia', '364066', ['4', '6', '8'])]

logger.info(f"* processed_countries: {len(processed_countries)}")
logger.info(f"* failed_countries: {len(failed_countries)}")
logger.info(f"* to process countries: {len(to_scrape)}")

# load environment variables
if DEV_MODE:
    from dotenv import load_dotenv
    load_dotenv()

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
    country, id, lvls = tuple
    process_state[country]['scrape']['type'] = 'chunk'
    process_state[country]['scrape']['chunk_state'] = {}

    response = too.getOSMIDAddsStruct_chunks((country, id, lvls), save_dir)
    logger.info(f"  - Scrape in chunks finished: {response['status']} - {response['status_type']}")

    state_resume = {k:{k2:(len(v2) if type(v2) == set else v2) for k2,v2 in val.items()} for k,val in response['data'].items()}
    logger.info(f"  - Chunk status: {state_resume}")

    tools.update_process_state(process_state, country, 'scrape', response['status'])
    process_state[country]['scrape']['chunk_state'] = response['data']
    process_state[country]['scrape']['error'] = response['status_type']
    tgm.dump(process_state_file, process_state)

    if not DEV_MODE:
        tools.upload_dir_files_to_backblaze(country_save_file.parent, config)
        tools.commit_file(process_state_file, f"Update process state: {country}: (scrape, {response['status']})", config['logger'])

# fetch admin
for country, id, lvls in to_scrape:

    config = {'root':ROOT, 's3':s3, 'logger':logger}
    country_save_file = SAVE_DIR / country / f'rawOSMRes.json'

    if country not in in_chunks_countries:
        logger.info(f"* processing: {country, id, lvls}")
        
        response = too.getOSMIDAddsStruct(id, lvls)
        logger.info(f"  - finished: {response['status']}")

        if response["status"] == "ok":
            tgm.dump(country_save_file, response["data"])
            if not DEV_MODE:
                tools.upload_dir_files_to_backblaze(country_save_file.parent, config)
        elif '429' in response["status_type"] or 'timeout' in response["status_type"]:
            logger.info(f"  - Too many requests/timeout error, using chunks", logger)
            scrape_country_in_chunks((country, id, lvls), SAVE_DIR, country_save_file, config, process_state, process_state_file)
        else:
            logger.info(f"  - Failed, saving to process state")

        tools.update_process_state(process_state, country, 'scrape', response['status'])
        tgm.dump(process_state_file, process_state)
        if not DEV_MODE:
            tools.commit_file(process_state_file, process_state, f"Update process state: {country}: (scrape, {response['status']})", config['logger'])
    else:
        scrape_country_in_chunks((country, id, lvls), SAVE_DIR, country_save_file, config, process_state, process_state_file)

    time.sleep(3)

processed_countries = [country for country, country_state in process_state.items() if country_state['scrape']['status'] == 'ok']
failed_countries = [country for country, country_state in process_state.items() if country_state['scrape']['status'] == 'failed']

logger.info(f"* new total of processed_countries: {len(processed_countries)}")
logger.info(f"* new total of failed_countries: {len(failed_countries)}")
