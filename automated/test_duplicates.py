# init
import os
from pathlib import Path
import subprocess
import boto3
from dotenv import load_dotenv
import time

import toolsGeneral.main as tgm
import toolsGeneral.logger as tgl
import toolsOSM.overpass as too
import toolsSync.main as tsm

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
TESTS_DIR = DATA_DIR / 'tests results'
CLEANED_DIR = DATA_DIR / 'cleaned'
DEV_MODE = True

task = 'test_duplicates'
TEST_DUPLICATES_DIR = TESTS_DIR / 'osm duplicates test'
process_state = tgm.load(DATA_DIR / "process_state.json")
dups_test_state = tgm.load(DATA_DIR / "dups_test_state.json")

logger = tgl.initiate_logger('logger', TEST_DUPLICATES_DIR / 'dups_test.log')

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

#* load environment variables
load_dotenv()

#* select countries to test
countries_tested = [c for c, val in process_state.items() if (val[task]['status'] == 'ok')]
logger.info(f"countries tested: {len(countries_tested)}")
countries_to_test = [c for c, val in process_state.items() if 
    (val['clean']['status'] == 'ok') and (val[task]['status'] in ['pending', 'error'])
]
logger.info(f"countries to test: {len(countries_to_test)}")

#* initialize B2
session = boto3.session.Session()

s3 = session.client(
    service_name="s3",
    aws_access_key_id=os.environ["B2_KEY_ID"],
    aws_secret_access_key=os.environ["B2_APPLICATION_KEY"],
    endpoint_url=os.environ["B2_ENDPOINT"]
)

config = {'root':ROOT, 's3':s3, 'logger':logger}

#* download required data
logger.info(f"* Downloading required data to test: {len(countries_to_test)} countries")
logger.info(f"  * Downloading data from B2 in directory: '{CLEANED_DIR.relative_to(ROOT)}'")

countries_downloaded = tsm.donwload_country_data_from_bucket(countries_to_test, os.environ["B2_BUCKET_NAME"], CLEANED_DIR.relative_to(ROOT), CLEANED_DIR, s3, logger)

logger.info(f'* Countries to test: {len(countries_to_test)}')
logger.info(f"* Countries to test with downloaded cleaned data from B2: {len(countries_downloaded)}")

#* load data for countries to test
logger.info(f"* Load data from: {CLEANED_DIR.relative_to(ROOT)}")
countries_to_test_df = tgm.load_single_file_dirs(CLEANED_DIR, countries_to_test)
logger.info(f"  * Countries to test {len(countries_to_test)} ; countries with data loaded {len(countries_to_test_df)}")

#* select relations with duplicates ids
# dups_id is computed using all countries in cleaned data, so we need just to filter here
dups_id = tgm.load(TEST_DUPLICATES_DIR  / 'dups_id.pkl')
logger.info(f"Duplicates ids: {len(dups_id)}")

logger.info(f"Countries to test: {len(countries_to_test_df)}")
logger.info(f"Relations to test: {len([row['id'] for df in countries_to_test_df.values() for i, row in df.iterrows()])}")

dups_df = {}
countries_wihout_first_level = []
for country, df in countries_to_test_df.items():
    dups = df[df['id'].isin(dups_id)]
    if not dups.empty:
        dups_df[country] = dups
    else:
        countries_wihout_first_level.append(country)
        tsm.update_process_state(process_state, country, task, process_status='missing')
        tgm.dump(DATA_DIR / "process_state.json", process_state)

logger.info(f"countries with first level: {len(dups_df)} \n {list(dups_df.keys())}")
logger.info(f"relations duplicates to test: {len([row['id'] for df in dups_df.values() for i, row in df.iterrows()])}")
logger.info(f"countries without first level: {len(countries_wihout_first_level)} \n{countries_wihout_first_level}")

#* exclude processed relations
dups_test_state = tgm.load(DATA_DIR / 'dups_test_state.json')
logger.info(f"Countries dups state: {len(dups_test_state)}")

processed_id_triplets = [ids for res in dups_test_state.values() for ids in res['processed']]
logger.info(f"Processed id triplets: {len(processed_id_triplets)}")

logger.info(f"Countries to process: {len(dups_df)}")
logger.info(f"Relations to process: {len([row['id'] for df in dups_df.values() for i, row in df.iterrows()])}")

dups_pending_process_df = {}
for country, df in dups_df.items():
    processed = dups_test_state[country]['processed'] if dups_test_state.get(country) else set()
    failed = dups_test_state[country]['failed'] if dups_test_state.get(country) else set()

    in_processed = df[['id','tags.parent_id','tags.country_id']].apply(tuple, axis=1).isin(processed)
    in_failed = df[['id','tags.parent_id','tags.country_id']].apply(tuple, axis=1).isin(failed)
    filtered_df = df[~in_processed | in_failed]
    if not filtered_df.empty:
        dups_pending_process_df[country] = filtered_df

print(f"Countries to process: {len(dups_pending_process_df)}")
print(f"Relations to process filtered: {len([row['id'] for df in dups_pending_process_df.values() for i, row in df.iterrows()])}")

#* make tests

config = {'root':ROOT, 's3':s3, 'logger':logger}

for country, df in dups_pending_process_df.items():
    logger.info(f"* Testing country {country}: {len(df)} relations")

    if not dups_test_state.get(country):
        dups_test_state[country] = {"to_process": set(), "processed": set(), "failed": set(), "next_index": 0}
    country_test_state = dups_test_state[country]

    total = len(df)
    test_res = {}
    
    CHUNK_SIZE = 15
    MAX_SECONDS_WITHOUT_UPLOAD = 300 # 5 minutes
    chunk_count = 0
    last_upload_time = time.time()
    country_time_start = time.time()   
    save_path = TEST_DUPLICATES_DIR / country / f"{country}_first_level_test_res_{country_test_state['next_index']}.pkl"

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
            save_path = TEST_DUPLICATES_DIR / country / f"{country}_first_level_test_res_{country_test_state['next_index']}.pkl"

            tgm.dump(DATA_DIR / "dups_test_state.json", dups_test_state)
            # upload to B2
            if not DEV_MODE:
                logger.info("* Uploading data to backblaze b2")
                tsm.upload_dir_files_to_backblaze(TEST_DUPLICATES_DIR / country, config)
                tsm.commit_file(DATA_DIR  / "dups_test_state.json", f"Update {country} first level test state: chunk {country_test_state['next_index'] - 1}", logger)

            chunk_count = 0
            last_upload_time = time.time()
            
        #* break after 20 min per country
        if (time.time() - country_time_start) >= 1200:
            break


    logger.info(f"* Finished {country}: saving data ...")
    # save test res
    if test_res:
        tgm.dump(save_path, test_res)
        country_test_state['next_index'] += 1

    # save country state
    tgm.dump(DATA_DIR / "dups_test_state.json", dups_test_state)

    if len(dups_test_state[country]['failed']) < 1:
        tsm.update_process_state(process_state, country, task, process_status='ok')
        tgm.dump(DATA_DIR / "process_state.json", process_state)

    # upload and commit after a country finishes
    if not DEV_MODE:
        logger.info("* Uploading data to backblaze b2")
        tsm.upload_dir_files_to_backblaze(TEST_DUPLICATES_DIR / country, config)
        tsm.commit_file(DATA_DIR  / "dups_test_state.json", f"Update {country} first level test state", logger)
        tsm.commit_file(DATA_DIR / "process_state.json", f"Update process state for {country}: ({task}, ok)", logger)