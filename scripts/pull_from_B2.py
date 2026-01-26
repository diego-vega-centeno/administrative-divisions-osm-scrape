import toolsSync.main as tsm
from pathlib import Path
import toolsGeneral.logger as tgl
import toolsGeneral.main as tgm
import os
import boto3
from dotenv import load_dotenv


#* variables
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
logger = tgl.initiate_logger('logger')
load_dotenv()
process_state_file = Path(DATA_DIR / "process_state.json")
dups_test_state_file = Path(DATA_DIR / "dups_test_state.json")
first_level_test_state_file = Path(DATA_DIR / "first_level_test_state.json")

#* setup b2
session = boto3.session.Session()
s3 = session.client(
    service_name="s3",
    aws_access_key_id=os.environ["B2_KEY_ID"],
    aws_secret_access_key=os.environ["B2_APPLICATION_KEY"],
    endpoint_url=os.environ["B2_ENDPOINT"]
)

#* download files
tsm.download_file_from_bucket(os.environ["B2_BUCKET_NAME"], process_state_file.relative_to(ROOT), s3, process_state_file.with_name("process_state_new.json"), logger)
tsm.download_file_from_bucket(os.environ["B2_BUCKET_NAME"], dups_test_state_file.relative_to(ROOT), s3, dups_test_state_file.with_name("dups_test_state_new.json"), logger)
tsm.download_file_from_bucket(os.environ["B2_BUCKET_NAME"], first_level_test_state_file.relative_to(ROOT), s3, first_level_test_state_file.with_name("first_level_test_state_new.json"), logger)