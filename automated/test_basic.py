# init
import os
from pathlib import Path
import pandas as pd
import boto3
import sys
import subprocess
from dotenv import load_dotenv

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
TEST_BASIC_DIR = TESTS_DIR / 'osm basic test'
process_state_file = DATA_DIR / "process_state.json"
task = 'test_basic'

#* load environment variables
load_dotenv()

#* initialize logger
logger = tgl.initiate_logger('logger', TEST_BASIC_DIR / 'basic_test.log')

#* initalize B2
session = boto3.session.Session()

s3 = session.client(
    service_name="s3",
    aws_access_key_id=os.environ["B2_KEY_ID"],
    aws_secret_access_key=os.environ["B2_APPLICATION_KEY"],
    endpoint_url=os.environ["B2_ENDPOINT"]
)
config = {'root':ROOT, 's3':s3, 'logger':logger}

token = os.environ.get("GITHUB_TOKEN")
if token:
    #* initialize git
    subprocess.run(["git", "config", "--global", "--add", "safe.directory", "/app"], check=True)
    subprocess.run(["git", "config", "--global", "user.name", "github-actions[bot]"])
    subprocess.run(["git", "config", "--global", "user.email", "github-actions[bot]@users.noreply.github.com"])
    subprocess.run([
        "git", "remote", "set-url", "origin",
        f"https://x-access-token:{token}@github.com/CopaCabana21/administrative-divisions-osm-scrape.git"
    ])
    subprocess.run(["git", "pull", "--rebase"], check=True)

#* download from b2
tsm.download_file_from_bucket(os.environ["B2_BUCKET_NAME"], process_state_file.relative_to(ROOT), s3, process_state_file, logger)

#* load state
process_state = tgm.load(process_state_file)

#* select entities to test
countries_tested = [c for c, val in process_state.items() if (val['test_basic']['status'] == 'ok')]
logger.info(f"countries tested: {len(countries_tested)}")
countries_to_test = [c for c, val in process_state.items() if 
    (val['clean']['status'] == 'ok') and (val['test_basic']['status'] in ['pending', 'error'])
]

#* schedule countries
# countries_to_test = ['SahrawiArabDemocraticRepublic']
# countries_to_test = countries_to_test[:2]

if len(countries_to_test) < 1:
    logger.info("No countries to test, exiting script")
    sys.exit(0)

logger.info(f"countries to test: {len(countries_to_test)}")

#* download required data
logger.info(f"* Downloading required data to test: {len(countries_to_test)} countries")
logger.info(f"  * Downloading country data from B2 in directory: '{CLEANED_DIR.relative_to(ROOT)}'")

countries_downloaded = tsm.donwload_country_data_from_bucket(countries_to_test, os.environ["B2_BUCKET_NAME"], CLEANED_DIR.relative_to(ROOT), CLEANED_DIR, s3, logger)

logger.info(f'* Countries to test: {len(countries_to_test)}')
logger.info(f"* Countries to test with downloaded data from B2: {len(countries_downloaded)}")

if len(countries_downloaded) < 1:
    logger.info("No countries to test found in B2, exiting script")
    sys.exit(0)

#* load data for countries to test
logger.info(f"* Load data from: {CLEANED_DIR.relative_to(ROOT)}")
cleaned_files = [f for f in CLEANED_DIR.glob('*/*')]
logger.info(f"  * Files found: {len(cleaned_files)}")

if len(cleaned_files) < 1:
    logger.info("No cleaned data found for countries to test, exiting script")
    sys.exit(0)

# to_test_df = {file.parent.name:tgm.load(str(file)) for file in cleaned_files if file.parent.name}
to_test_df = {file.parent.name:tgm.load(str(file)) for file in cleaned_files if file.parent.name in countries_to_test}
logger.info(f"  * Countries to test: {len(countries_to_test)}")
logger.info(f"  * Data loaded for countries to test: {len(to_test_df)}")

#* make test
logger.info(f"* Make basic test and save")
for country, df in to_test_df.items():
    logger.info(f'* Country: {country}')
    test_res = too.osm_basic_test(df)
    tgm.dump(TEST_BASIC_DIR / country / f'{country}_basic_test_res.pkl', test_res)

#* upload results to B2
tested_dirs = [dir.name for dir in TEST_BASIC_DIR.glob('*/') if dir.name in countries_to_test]

if len(tested_dirs) < 1:
    logger.info("No data to upload, exiting script")
    sys.exit(0)
else:
    logger.info(f"* Test result directories found: {len(tested_dirs)}")

for country in tested_dirs:
    process_status = 'ok'
    process_error = None
    
    # all data in country directory will  be uploaded
    if not DEV_MODE:
        upload_response = tsm.upload_dir_files_to_backblaze(TEST_BASIC_DIR / country, config)
        process_status = upload_response['status']
        process_error = upload_response['status_type']

    # override process task state with upload response
    logger.info(f"  * Updating {country} in process state: ({task}, ok)")
    tsm.update_process_state(process_state, country, task, process_status=process_status, process_error=process_error)

#* upload state to B2
tgm.dump(process_state_file, process_state)
if not DEV_MODE:
    tsm.upload_file_to_backblaze(process_state_file, config)