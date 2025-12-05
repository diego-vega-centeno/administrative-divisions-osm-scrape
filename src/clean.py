# init
import os
from pathlib import Path
import boto3
import pandas as pd
import re
import sys

import toolsGeneral.logger as tgl
import toolsGeneral.main as tgm
import toolsOSM.overpass as too
import toolsSync.main as tsm

#* Initialize setup
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SAVE_DIR = DATA_DIR / 'cleaned'
DEV_MODE = True

logger = tgl.initiate_logger('logger', SAVE_DIR / 'cleaned.log')

process_state = tgm.load(DATA_DIR / 'process_state.json')
logger.info(f"Number of countries in process state: {len(process_state)}")

#* select entities to process
countries_cleaned = [c for c, val in process_state.items() if (val['clean']['status'] == 'ok')]
logger.info(f'countries cleaned: {len(countries_cleaned)}')
countries_to_clean = [c for c, val in process_state.items() if (val['scrape']['status'] == 'ok') and val['clean']['status'] == 'pending']
logger.info(f'countries to clean: {len(countries_to_clean)}')

#* load environment variables
if DEV_MODE:
    from dotenv import load_dotenv
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
logger.info(f"* finshed b2")

#* load state and meta data files
process_state_file = DATA_DIR / "process_state.json"
process_state = tgm.load(process_state_file)

countries_to_clean = ['Armenia']

downloaded_count = 0
to_download_total = 0
raw_data_dir = Path('data/raw/osm countries queries')
list_obj_response = s3.list_objects_v2(Bucket=os.environ["B2_BUCKET_NAME"], Prefix=raw_data_dir.as_posix())
files_list = [(obj['Key']) for obj in list_obj_response['Contents']]
logger.info(f"Total files found for bucket in {raw_data_dir}: {len(files_list)}")

logger.info(f"* Downloading data from backbkaze: {len(countries_to_clean)}")
# load data from b2 bucket for countries to process
for count, country in enumerate(countries_to_clean, start=1):
    country_files = [str(file) for file in files_list if re.match(rf"{raw_data_dir.as_posix()}/{country}/.+\.json", file)]
    to_download_total += len(country_files)
    logger.info(f"  * Country {country} ({count}/{len(countries_to_clean)}) files found: {len(country_files)}")
    for file in country_files:
        save_file = ROOT / raw_data_dir / country / os.path.basename(file)
        if save_file.exists():
            logger.info(f"  * Skip existing file {save_file}")
            continue
        
        os.makedirs(save_file.parent, exist_ok=True)
        try:
            s3.download_file(os.environ["B2_BUCKET_NAME"], file, str(save_file))
            logger.info(f"  * File '{file}' downloaded successfully to '{Path(save_file).relative_to(ROOT)}'")
            downloaded_count += 1
        except Exception as e:
            logger.error(f"  * Error downloading file '{file}': {e}")

logger.info(f"Number of downloaded files: {downloaded_count}/{to_download_total}")

#* load data for countries to clean
country_raw_dirs = [f for f in (DATA_DIR / 'raw/osm countries queries').glob('*') if f.is_dir()]
logger.info(f"Number of all raw directories found: {len(country_raw_dirs)}")

to_clean_by_cntr = {}
logger.info(f"Loading raw data only for countries to clean: {len(countries_to_clean)}")
# for chunks and non chunk files
for country in countries_to_clean:
    country_dir = DATA_DIR / 'raw/osm countries queries' / country
    if not country_dir.exists():
        continue
    files_elements = [tgm.load(f)['elements'] for f in country_dir.glob('*.json')]
    elements = [ele for list in files_elements for ele in list]
    to_clean_by_cntr[country] = elements

logger.info(f"Number of countries with raw data loaded: {len(to_clean_by_cntr)}")
logger.info(f"Number of countries to clean without raw data: {tgm.complement(countries_to_clean, to_clean_by_cntr.keys())}")

if len(countries_to_clean) < 1:
    logger.info("No countries to clean, exiting script")
    sys.exit(0)

#* START CLEANING STEPS
logger.info(f"Start cleaning steps")

#* Use sovereign countries only
logger.info(f"* Use sovereign countries only")
sovereign_countries = tgm.load(DATA_DIR / 'sovereign countries.json')
logger.info(f"  * Sovereign countries: {len(sovereign_countries)}")

cleaned_by_cntr = {k:data for k,data in to_clean_by_cntr.items() if k in sovereign_countries}
logger.info(f"  * Filtered sovereign countries: {len(cleaned_by_cntr)}")

#* clean countries
logger.info(f"* Clean countries")
import copy
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
    tgm.dump(str(SAVE_DIR), df)
logger.info(f"Saved files to cleaned directory: {len(cleaned_by_cntr)}")

#* Upload data to backblaze b2 and update process state
logger.info("* Uploading data to backblaze b2")
config = {'root':ROOT, 's3':s3, 'logger':logger}
for country in cleaned_by_cntr.keys():
    logger.info(f"  * Uploading directory for country: {country}")
    country_save_dir = SAVE_DIR / country
    # all data in country directory will  be uploaded
    if not DEV_MODE:
        res = tsm.upload_dir_files_to_backblaze(country_save_dir, config)
        if res['status'] != 'ok':
            continue
    # add country to process state
    logger.info(f"  * Updating {country} in process state: (clean, ok)")
    tsm.update_process_state(process_state, country, 'clean', 'ok')
    tgm.dump(DATA_DIR / 'process_state.json', process_state)
    # commit process state
    if not DEV_MODE:
        tsm.commit_file(DATA_DIR / 'process_state.json', f"Update process state for {country}: (scrape, ok)", config['logger'])