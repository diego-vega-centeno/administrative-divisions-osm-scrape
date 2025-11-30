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

# load files
failed_file = SAVE_DIR / 'failed_countries.pkl'
failed_countries = tgm.load(failed_file) if os.path.exists(failed_file) else set()

processed_file = SAVE_DIR / 'processed_countries.pkl'
processed_countries = tgm.load(processed_file) if os.path.exists(processed_file) else set()

# skip already processed countries
to_scrape = [t for t in tuples if t[0] not in processed_countries]
to_scrape = to_scrape[:2]
to_scrape = [('China', '270056', ['4', '6', '8'])]

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

def upload_dir_files_to_backblaze(dir:Path):
    for file in dir.rglob("*"):
        if file.is_file():
            try:
                s3.upload_file(
                    str(file), 
                    os.environ["B2_BUCKET_NAME"], 
                    str(file.relative_to(ROOT))
                )
                raw_scrape_logger.info(f"Uploaded {file} to Backblaze successfully")
            except Exception as e:
                raw_scrape_logger.error(f"Failed to upload {file}: {e}")


def commit_file(file:Path, commit_msg):
    try:
        subprocess.run(["git", "add", str(file)], check=True)
        result = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if result.returncode != 0:
            subprocess.run(["git", "commit", "-m", commit_msg], check=True)
            subprocess.run(["git", "push"], check=True)

        raw_scrape_logger.info(f"Commit successful: {file.name}")
    except Exception as e:
        raw_scrape_logger.error(f"Failed to commit {file.name}: {e}")

def dump_upload_and_commit_result(file, data, commit_msg):
    tgm.dump(file, data)
    upload_dir_files_to_backblaze(file.parent)
    commit_file(file, commit_msg)

in_chunks_countries = ['China','Armenia']

# fetch admin
for country, id, lvls in to_scrape:

    if country not in in_chunks_countries:
        raw_scrape_logger.info(f"* processing: {country, id, lvls}")
        
        country_save_file = SAVE_DIR / country / f'rawOSMRes.json'
        response = too.getOSMIDAddsStruct(id, lvls)
        raw_scrape_logger.info(f"  - finished: {response['status']}")

        if response["status"] == "ok":
            tgm.dump(country_save_file, response["data"])
            upload_dir_files_to_backblaze(country_save_file.parent)

            processed_countries.add(country)
            dump_upload_and_commit_result(processed_file, processed_countries, f"Update processed_countries: added {country}")

        elif '429' in response["status_type"] or 'timeout' in response["status_type"]:
            raw_scrape_logger.info(f"  - Too many requests/timeout error, using chunks")
            too.getOSMIDAddsStruct_chunks((country, id, lvls), SAVE_DIR)
        else:
            raw_scrape_logger.info(f"  - Failed, saving to failed_countries")
            failed_countries.add(country)
            dump_upload_and_commit_result(failed_file, failed_countries, f"Update failed_countries: added {country}")
    else:
        response = too.getOSMIDAddsStruct_chunks((country, id, lvls), SAVE_DIR)

        # only commit data and upload data on successfull return of chunks
        raw_scrape_logger.info(f"  - finished: {response['status']} - {response['status_type']}")
        if response['status'] == 'ok':
            processed_countries.add(country)
            dump_upload_and_commit_result(processed_file, processed_countries, f"Update processed_countries: added {country}")
        else:
            raw_scrape_logger.info(f"  - A chunk failed, saving to failed_countries")
            failed_countries.add(country)
            dump_upload_and_commit_result(failed_file, failed_countries, f"Update failed_countries: added {country}")          

    time.sleep(3)


raw_scrape_logger.info(f"* new total of processed_countries: {len(processed_countries)}")
raw_scrape_logger.info(f"* new total of failed_countries: {len(failed_countries)}")
