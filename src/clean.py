# init
import os
from importlib import reload
from pathlib import Path
import boto3
import pandas as pd
import tools.upload_and_commit as tuc

import toolsGeneral.logger as tgl
import toolsGeneral.main as tgm
import toolsOSM.overpass as too

# Initialize setup
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SAVE_DIR = DATA_DIR / 'normalized'
logger = tgl.logger('logger', SAVE_DIR / 'raw_scrape.log')

# Use AWS kit to upload files
session = boto3.session.Session()

s3 = session.client(
    service_name="s3",
    aws_access_key_id=os.environ["B2_KEY_ID"],
    aws_secret_access_key=os.environ["B2_APPLICATION_KEY"],
    endpoint_url=os.environ["B2_ENDPOINT"]
)

# list raw files
files_dirs = [f for f in (DATA_DIR / 'raw/osm countries queries').glob('*') if f.is_dir()]
logger.info(f"* raw files: {len(files_dirs)}")

# load raw data for chunks and non chunk files
raw_by_cntr = {}
for dir in files_dirs:
    files_elements = [tgm.load(str(f))['elements'] for f in dir.glob('*.json')]
    elements = [ele for list in files_elements for ele in list]
    raw_by_cntr[str(dir.name)] = elements

logger.info(f"* countries raw data found: {len(raw_by_cntr)}")

# filter countries to process
processed_file = SAVE_DIR / 'processsed_countries.pkl'
processed_countries = tgm.load(processed_file) if os.path.exists(processed_file) else set()
logger.info(f"* processsed_countries: {len(processed_countries)}")

to_process_by_cntr = {k:v for k,v in raw_by_cntr.items() if k not in processed_countries}
logger.info(f"* to_process_by_cntr: {len(to_process_by_cntr)}")


#* use sovereign countries only
sovereign_countries = tgm.load(DATA_DIR / 'sovereign countries.json')
processsed_by_cntr = {k:df for k,df in to_process_by_cntr.items() if k in sovereign_countries}

#* clean countries
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

processsed_by_cntr = clean_country_data(processsed_by_cntr)

temp = [ele['tags'].get('parent_id',None) for k,v in processsed_by_cntr.items() for ele in v]

logger.info(f"* types of data:")
logger.info(f"{pd.Series(temp).map(type).value_counts()}")

#* convert to dataframe
df_by_cntr = {k:too.normalizeOSM(elems) for k,elems in processsed_by_cntr.items()}
logger.info(f"* df_by_cntr: {len(df_by_cntr)}")

config = {'root':ROOT, 's3':s3, 'logger':logger}
# * save files
for country,df in df_by_cntr.items():
    processed_countries.add(country)
    tuc.dump_upload_and_commit_result(processed_file, processed_countries, f"Update processed_countries: added {country}", config)
