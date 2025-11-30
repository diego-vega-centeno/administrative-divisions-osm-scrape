#!/usr/bin/env python
# coding: utf-8

# init
import sys
import os
import time
from importlib import reload
from pathlib import Path
import boto3
import subprocess

import toolsGeneral.logger as tgl
import toolsGeneral.main as tgm
import toolsOSM.overpass as too

def pckgs_reload():
    reload(tgm)
    reload(tgl)
    reload(too)

pckgs_reload()


# Initialize setup
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SAVE_DIR = DATA_DIR / 'raw/osm countries queries'

subprocess.run(["git", "config", "--global", "--add", "safe.directory", "/app"], check=True)
subprocess.run(["git", "config", "--global", "user.name", "github-actions[bot]"])
subprocess.run(["git", "config", "--global", "user.email", "github-actions[bot]@users.noreply.github.com"])

subprocess.run([
    "git", "remote", "set-url", "origin",
    f"https://x-access-token:{os.environ['GITHUB_TOKEN']}@github.com/CopaCabana21/automated-add-osm-scrape.git"
])
subprocess.run(["git", "pull", "--rebase"], check=True)

# load variables
raw_scrape_logger = tgl.initiate_logger('raw_scrape_logger', DATA_DIR / 'raw/raw_scrape.log')

osmMetaCountrDict = tgm.load(DATA_DIR / "osmMetaCountrDict.json")

tuples = sorted(
    [(k, v["id"], v["addLvlsNum"]) for (k, v) in osmMetaCountrDict.items()],
    key=lambda arg: arg[0]
)

# exclude processed countries
processed_countries = tgm.load(SAVE_DIR / "processed_countries.pkl")
# processed_countries = {f.parent.name for f in SAVE_DIR.glob('*/*.json')}

# skip already processed countries
to_scrape = [t for t in tuples if t[0] not in processed_countries]
to_scrape = to_scrape[:2]

# load files
failed_file = SAVE_DIR / 'failed_countries.pkl'
failed_countries = tgm.load(failed_file) if os.path.exists(failed_file) else set()

processed_file = SAVE_DIR / 'processed_countries.pkl'
processed_countries = tgm.load(processed_file) if os.path.exists(processed_file) else set()

raw_scrape_logger.info(f"* processed_countries: {len(processed_countries)}")
raw_scrape_logger.info(f"* failed_countries: {len(failed_countries)}")
raw_scrape_logger.info(f"* to process countries: {len(to_scrape)}")

# Use AWS kit to upload files
session = boto3.session.Session()

s3 = session.client(
    service_name="s3",
    aws_access_key_id=os.environ["B2_KEY_ID"],
    aws_secret_access_key=os.environ["B2_APPLICATION_KEY"],
    endpoint_url=os.environ["B2_ENDPOINT"]
)

def upload_file_to_backblaze(path):
    s3.upload_file(
        str(path), 
        os.environ["B2_BUCKET"], 
        str(path.relative_to(ROOT))
    )

def commit_file(file, commit_msg):
    subprocess.run(["git", "add", file], check=True)
    result = subprocess.run(["git", "diff", "--cached", "--quiet"])
    if result.returncode != 0:
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
        subprocess.run(["git", "push"], check=True)


# fetch admin
for country, id, lvls in to_scrape:
    raw_scrape_logger.info(f"* processing: {country, id, lvls}")
    
    country_save_file = SAVE_DIR / country / f'rawOSMRes.json'
    response = too.getOSMIDAddsStruct(id, lvls)
    raw_scrape_logger.info(f"  - finished: {response['status']}")

    if response["status"] == "ok":
        tgm.dump(country_save_file, response["data"])
        upload_file_to_backblaze(country_save_file)

        processed_countries.add(country)
        tgm.dump(processed_file, processed_countries)
        upload_file_to_backblaze(processed_file)
        commit_file(str(processed_file), f"Update processed_countries: added {country}")

    elif '429' in response["status_type"]:
        raw_scrape_logger.info(f"  - Too many requests error, trying chunks")
        too.getOSMIDAddsStruct_chunks((country, id, lvls), SAVE_DIR)
    else:
        raw_scrape_logger.info(f"  - Failed, saving to failed_countries")
        failed_countries.add(country)
        tgm.dump(failed_file, failed_countries)
        upload_file_to_backblaze(failed_file)
        commit_file(str(failed_file), f"Update failed_countries: added {country}")
    
    time.sleep(3)

raw_scrape_logger.info(f"* new total of processed_countries: {len(processed_countries)}")
raw_scrape_logger.info(f"* new total of failed_countries: {len(failed_countries)}")


# for country, id, lvls in to_scrape:

#     too.getOSMIDAddsStruct_chunks(tuple, SAVE_DIR)
#     time.sleep(3)
