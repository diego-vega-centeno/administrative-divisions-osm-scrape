import subprocess
import toolsGeneral.main as tgm
from pathlib import Path
import os
import datetime
from botocore.exceptions import ClientError

def upload_dir_files_to_backblaze(dir:Path, config):

    s3 = config['s3']
    logger = config['logger']
    root = config['root']

    for file in dir.rglob("*"):
        if file.is_file():
            # delete file if exists
            try:
                s3.delete_object(Bucket=os.environ["B2_BUCKET_NAME"], Key=str(file.relative_to(root)))
            except ClientError as e:
                # Ignore error if file doesn't exist
                if e.response['Error']['Code'] != "NoSuchKey":
                    logger.error(f"Failed delete {file}: {e}")
                    raise # raise same exception
            # upload file
            try:
                s3.upload_file(
                    str(file), 
                    os.environ["B2_BUCKET_NAME"], 
                    str(file.relative_to(root))
                )
                logger.info(f"Uploaded {file} to Backblaze successfully")
            except Exception as e:
                logger.error(f"Failed to upload {file}: {e}")


def commit_file(file:Path, commit_msg, logger):
    try:
        subprocess.run(["git", "add", str(file)], check=True)
        result = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if result.returncode != 0:
            subprocess.run(["git", "commit", "-m", commit_msg], check=True)
            subprocess.run(["git", "push"], check=True)

        logger.info(f"Commit successful: {file.name}")
    except Exception as e:
        logger.error(f"Failed to commit {file.name}: {e}")

def update_process_state(process_state, country, process_type, process_status):
    process_state.setdefault(country, {
        key: {"status": "pending", "last_run": None, "error": None} for key in 
        ["scrape", "clean", "test_basic", "test_first_level", "test_duplicates", "fix"]
    })
    process_state[country][process_type]['status'] = process_status
    process_state[country][process_type]['last_run'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")