import toolsSync.main as tsm
from pathlib import Path
import toolsGeneral.logger as tgl
import os
import boto3

ROOT = Path.cwd().parents[0]
DATA_DIR = ROOT / "data"
logger = tgl.initiate_logger('logger')

session = boto3.session.Session()
s3 = session.client(
    service_name="s3",
    aws_access_key_id=os.environ["B2_KEY_ID"],
    aws_secret_access_key=os.environ["B2_APPLICATION_KEY"],
    endpoint_url=os.environ["B2_ENDPOINT"]
)

process_state_file = DATA_DIR / "process_state.json"
tsm.download_file_from_bucket(os.environ["B2_BUCKET_NAME"], process_state_file.relative_to(ROOT), s3, process_state_file, logger)