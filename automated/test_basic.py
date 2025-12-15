# init
from importlib import reload
import os
from pathlib import Path
import pandas as pd
from IPython.display import clear_output
import boto3
import sys

import toolsGeneral.main as tgm
import toolsGeneral.logger as tgl
import toolsOSM.overpass as too
import toolsPandas.helpers as tph
import toolsSync.main as tsm

def pckgs_reload():
    reload(tgm)
    reload(too)
    reload(tph)
    reload(tgl)
    reload(tsm)


#* initialize variables
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
TESTS_DIR = DATA_DIR / 'tests results'
CLEANED_DIR = DATA_DIR / 'cleaned'
DEV_MODE = True

process_state_file = DATA_DIR / "process_state.json"
process_state = tgm.load(process_state_file)
TEST_BASIC_DIR = TESTS_DIR / 'osm basic test'
logger = tgl.initiate_logger('logger', TEST_BASIC_DIR / 'basic_test.log')

#* select entities to test
countries_tested = [c for c, val in process_state.items() if (val['test_basic']['status'] == 'ok')]
logger.info(f"countries tested: {len(countries_tested)}")
countries_to_test = [c for c, val in process_state.items() if 
    (val['clean']['status'] == 'ok') and (val['test_basic']['status'] in ['pending', 'error'])
]

logger.info(f"countries to test: {len(countries_to_test)}")

#* initalize B2
session = boto3.session.Session()

s3 = session.client(
    service_name="s3",
    aws_access_key_id=os.environ["B2_KEY_ID"],
    aws_secret_access_key=os.environ["B2_APPLICATION_KEY"],
    endpoint_url=os.environ["B2_ENDPOINT"]
)

#* download required data
logger.info(f"* Downloading required data to test: {len(countries_to_test)} countries")
logger.info(f"  * Downloading country data from B2 in directory: '{CLEANED_DIR.relative_to(ROOT)}'")

countries_downloaded = tsm.donwload_country_data_from_bucket(countries_to_test, os.environ["B2_BUCKET_NAME"], CLEANED_DIR.relative_to(ROOT), CLEANED_DIR, s3, logger)

logger.info(f'* Countries to test: {len(countries_to_test)}')
logger.info(f"* Countries to test with downloaded data from B2: {len(countries_downloaded)}")

#* load data for countries to test
logger.info(f"* Load data from: {CLEANED_DIR.relative_to(ROOT)}")
cleaned_files = [f for f in CLEANED_DIR.glob('*/*')]
logger.info(f"  * Directories found: {len(cleaned_files)}")

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

#* select missing names and leaks from other countries
test_res_by_cntr = {f.parent.name:tgm.load(f) for f in TEST_BASIC_DIR.glob('*/*') if f.parent.name in countries_to_test}
# test_res_by_cntr = {f.parent.name:tgm.load(f) for f in TEST_BASIC_DIR.glob('*/*') if f.parent.name}
logger.info(f'Test results found: {len(test_res_by_cntr)}')

missing_names = set()
leaks = set()
for k,v in test_res_by_cntr.items():
    missing_names.update(v['missing_name'])
for k,v in test_res_by_cntr.items():
    leaks.update(v['test_tags_leak'])

logger.info(f"Missing names relations: {len(missing_names)}")
logger.info(f"Leaks relations: {len(leaks)}")
relations_from_test_to_delete = leaks | missing_names
logger.info(f"To delete parents relations: {len(relations_from_test_to_delete)}")

#* From the relations to delete, select the childs in the country that has them as parent
# get all id triplets: (id, parent_id, country_id)
id_triplets = pd.concat(to_test_df.values(), ignore_index=True)[['id', 'tags.parent_id', 'tags.country_id']].fillna('missing').apply(tuple,axis=1).to_list()
logger.info(f"All dataframes id triplets: {len(id_triplets)}")

parents_to_delete = relations_from_test_to_delete
relations_childs_to_delete = set()
while len(parents_to_delete) > 0:
    parents_id_and_countryid = {(ele[0],ele[2]) for ele in parents_to_delete}
    childs_to_delete = {ele for ele in id_triplets if (ele[1], ele[2]) in parents_id_and_countryid}
    relations_childs_to_delete.update(childs_to_delete)
    parents_to_delete = childs_to_delete
logger.info(f"Childs to delete: {len(relations_childs_to_delete)}")

#* Add to saved basic_test_to_delete.json and save
basic_test_to_delete = relations_from_test_to_delete | relations_childs_to_delete
logger.info(f"Basic test to delete relations: {len(basic_test_to_delete)}")

basic_test_to_delete_old = tgm.load(DATA_DIR / "basic_test_to_delete.json")
basic_test_to_delete_new = basic_test_to_delete_old | basic_test_to_delete
logger.info(f"Current total of relations to delete from basic test: {len(basic_test_to_delete_new)}")

tgm.dump(DATA_DIR / "basic_test_to_delete.json", basic_test_to_delete_new)

#* upload results to B2
logger.info("* Uploading data to backblaze b2")
config = {'root':ROOT, 's3':s3, 'logger':logger}
task = 'test_basic'
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
    tgm.dump(process_state_file, process_state)
    if not DEV_MODE:
        tsm.commit_file(process_state_file, f"Update process state for {country}: ({task}, ok)", config['logger'])