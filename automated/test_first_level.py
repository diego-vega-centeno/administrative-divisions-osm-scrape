# init
import os
from pathlib import Path
import pandas as pd
import boto3
import sys
import subprocess
from dotenv import load_dotenv
import time

import toolsGeneral.main as tgm
import toolsGeneral.logger as tgl
import toolsOSM.overpass as too
import toolsSync.main as tsm

#* initialize variables
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
TESTS_DIR = DATA_DIR / 'tests results'
CLEANED_DIR = DATA_DIR / 'cleaned'
DEV_MODE = False

task = 'test_first_level'
TEST_FIRST_LEVEL_DIR = TESTS_DIR / 'osm first level test'
process_state_file = DATA_DIR / "process_state.json"
first_level_test_state_file = DATA_DIR / "first_level_test_state.json"

#* initialize logger
logger = tgl.initiate_logger('logger', TEST_FIRST_LEVEL_DIR / 'first_level_test.log')

#* initialize B2
session = boto3.session.Session()

s3 = session.client(
    service_name="s3",
    aws_access_key_id=os.environ["B2_KEY_ID"],
    aws_secret_access_key=os.environ["B2_APPLICATION_KEY"],
    endpoint_url=os.environ["B2_ENDPOINT"]
)
config = {'root':ROOT, 's3':s3, 'logger':logger}

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

#* download from b2
tsm.download_file_from_bucket(os.environ["B2_BUCKET_NAME"], process_state_file.relative_to(ROOT), s3, process_state_file, logger)
tsm.download_file_from_bucket(os.environ["B2_BUCKET_NAME"], first_level_test_state_file.relative_to(ROOT), s3, first_level_test_state_file, logger)

#* load state
process_state = tgm.load(process_state_file)
first_level_test_state = tgm.load(first_level_test_state_file)

#* load environment variables
load_dotenv()

#* select countries to test
countries_tested = [c for c, val in process_state.items() if (val[task]['status'] == 'ok')]
logger.info(f"countries tested: {len(countries_tested)}")
countries_to_test = [c for c, val in process_state.items() if 
    (val['clean']['status'] == 'ok') and (val[task]['status'] in ['pending', 'error'])
]

if len(countries_to_test) < 1:
    logger.info("No countries to test, exiting script")
    sys.exit(0)

#* schedule
# countries_to_test = ['Canada','Germany','France','Peru']

logger.info(f"Countries to test: {len(countries_to_test)} \n {countries_to_test}")


#* download required data
logger.info(f"* Downloading required data to test: {len(countries_to_test)} countries")
logger.info(f"  * Downloading country data from B2 in directory: '{CLEANED_DIR.relative_to(ROOT)}'")

countries_downloaded = tsm.donwload_country_data_from_bucket(countries_to_test, os.environ["B2_BUCKET_NAME"], CLEANED_DIR.relative_to(ROOT), CLEANED_DIR, s3, logger)

if len(countries_downloaded) < 1:
    logger.info("No countries to test found in B2, exiting script")
    sys.exit(0)

logger.info(f'* Countries to test: {len(countries_to_test)}')
logger.info(f"* Countries to test with downloaded data from B2: {len(countries_downloaded)}")

#* load data for countries to test
logger.info(f"* Load data from: {CLEANED_DIR.relative_to(ROOT)}")
cleaned_files = [f for f in CLEANED_DIR.glob('*/*')]
logger.info(f"  * Directories found: {len(cleaned_files)}")

countries_to_test_df = {file.parent.name:tgm.load(str(file)) for file in cleaned_files if file.parent.name in countries_to_test}
logger.info(f"  * Countries to test: {len(countries_to_test)}")
logger.info(f"  * Data loaded for countries to test: {len(countries_to_test_df)}")

if len(cleaned_files) < 1:
    logger.info("No cleaned data found for countries to test, exiting script")
    sys.exit(0)

#* select only first level
first_lvl_df = {}
countries_wihout_first_level = []
for country, df in countries_to_test_df.items():
    if not df[df['tags.admin_level'] == '4'].empty:
        first_lvl_df[country] = df[df['tags.admin_level'] == '4']
    else:
        countries_wihout_first_level.append(country)
        tsm.update_process_state(process_state, country, task, process_status='missing')
        tgm.dump(DATA_DIR / "process_state.json", process_state)

if not DEV_MODE and len(countries_wihout_first_level) > 0:
    tsm.upload_file_to_backblaze(process_state_file, config)
    # tsm.commit_file(DATA_DIR / "process_state.json", f"Update process state for {country}: ({task}, ok)", logger)

logger.info(f"countries with first level: {len(first_lvl_df)} \n {list(first_lvl_df.keys())}")
logger.info(f"countries without first level: {countries_wihout_first_level}")

if len(first_lvl_df) < 1:
    logger.info("No first level data found for countries to test, exiting script")
    sys.exit(0)

#* filter already processed relations
logger.info(f"First_level_test_state countries: {len(first_level_test_state)}")
logger.info(f"First_level_test_state triplets processed: {sum([len(t['processed']) for t in first_level_test_state.values()])}")

first_lvl_filtered_df = {}
for country,df in first_lvl_df.items():
    processed = first_level_test_state[country]['processed'] if first_level_test_state.get(country) else set()
    failed = first_level_test_state[country]['failed'] if first_level_test_state.get(country) else set()

    in_processed = df[['id','tags.parent_id','tags.country_id']].apply(tuple, axis=1).isin(processed)
    in_failed = df[['id','tags.parent_id','tags.country_id']].apply(tuple, axis=1).isin(failed)
    filtered_df = df[~in_processed | in_failed]
    if not filtered_df.empty:
        first_lvl_filtered_df[country] = filtered_df

logger.info(f"Countries with first level -> filtered pending to process: {len(first_lvl_filtered_df)} \n {list(first_lvl_filtered_df.keys())}")
logger.info(f"Total of relations to process: {sum([len(lis) for lis in first_lvl_filtered_df.values()])}")

if len(first_lvl_filtered_df) < 1:
    logger.info("No first level data to test, exiting script")
    sys.exit(0)

#* makes tests
logger.info(f"* Making tests")
for country, df in first_lvl_filtered_df.items():
    logger.info(f"* Testing country {country}: {len(df)} relations")

    if not first_level_test_state.get(country):
        first_level_test_state[country] = {"to_process": set(), "processed": set(), "failed": set(), "next_index": 0}
    country_test_state = first_level_test_state[country]

    total = len(df)
    test_res = {}
    
    CHUNK_SIZE = 15
    MAX_SECONDS_WITHOUT_UPLOAD = 300 # 5 minutes
    chunk_count = 0
    last_upload_time = time.time()
    save_path = TEST_FIRST_LEVEL_DIR / country / f"{country}_first_level_test_res_{country_test_state['next_index']}.pkl"

    for i, (idx, row) in enumerate(df.iterrows(), start=1):
        id_triplet = (row["id"], row["tags.parent_id"], row["tags.country_id"])
        logger.info(f" ^ [{i}/{total}]: testing {id_triplet}:")

        res = too.is_child_inside_parent(row["id"], row["tags.parent_id"])

        test_res[id_triplet] = res

        status_list = [v['status'] for k,v in res.items()]
        if 'error' in status_list:
            country_test_state['failed'].add(id_triplet)
        else:
            country_test_state['processed'].add(id_triplet)
            country_test_state['failed'].discard(id_triplet)

        time.sleep(2)
        resume  = {k:v['status'] for k,v in res.items()}
        logger.info(f" $ finished: status: {resume}")

        chunk_count += 1
        #* persist partial results
        if (chunk_count >= CHUNK_SIZE or (time.time() - last_upload_time) >= MAX_SECONDS_WITHOUT_UPLOAD):
            logger.info(f"* Checkpoint upload for {country}")
            # persist current chunk
            tgm.dump(save_path, test_res)
            # advance chunk
            test_res = {}
            country_test_state['next_index'] += 1
            # next chunk file
            save_path = TEST_FIRST_LEVEL_DIR / country / f"{country}_first_level_test_res_{country_test_state['next_index']}.pkl"

            tgm.dump(DATA_DIR / "first_level_test_state.json", first_level_test_state)
            # upload to B2
            if not DEV_MODE:
                logger.info("* Uploading data to backblaze b2")
                tsm.upload_dir_files_to_backblaze(TEST_FIRST_LEVEL_DIR / country, config)
                tsm.upload_file_to_backblaze(first_level_test_state_file, config)
                # tsm.commit_file(DATA_DIR  / "first_level_test_state.json", f"Update {country} first level test state: chunk {country_test_state['next_index'] - 1}", logger)

            chunk_count = 0
            last_upload_time = time.time()


    logger.info(f"* Finished {country}: saving data ...")
    # save test res
    if test_res:
        tgm.dump(save_path, test_res)
        country_test_state['next_index'] += 1

    # save country state
    tgm.dump(DATA_DIR / "first_level_test_state.json", first_level_test_state)

    if len(first_level_test_state[country]['failed']) < 1:
        tsm.update_process_state(process_state, country, task, process_status='ok')
        tgm.dump(DATA_DIR / "process_state.json", process_state)

    # upload and commit after a country finishes
    if not DEV_MODE:
        logger.info("* Uploading data to backblaze b2")
        tsm.upload_dir_files_to_backblaze(TEST_FIRST_LEVEL_DIR / country, config)
        tsm.upload_file_to_backblaze(first_level_test_state_file, config)
        # tsm.commit_file(DATA_DIR  / "first_level_test_state.json", f"Update {country} first level test state", logger)
        tsm.upload_file_to_backblaze(process_state_file, config)
        # tsm.commit_file(DATA_DIR / "process_state.json", f"Update process state for {country}: ({task}, ok)", logger)