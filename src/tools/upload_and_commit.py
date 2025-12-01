import subprocess
import toolsGeneral.main as tgm
from pathlib import Path
import os
import datetime

def upload_dir_files_to_backblaze(dir:Path, config):

    s3 = config['s3']
    logger = config['logger']
    root = config['root']

    for file in dir.rglob("*"):
        if file.is_file():
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

def update_process_state(process_state, country, process_type, process_result):
    process_state.setdefault(country, {
        key: {"status": "pending", "last_run": None, "error": None} for key in 
        ["scrape", "clean", "test_basic", "test_first_level", "test_duplicates", "fix"]
    })
    process_state[country][process_type] = process_result
    process_state[country][process_type] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")