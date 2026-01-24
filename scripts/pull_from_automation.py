import subprocess

subprocess.run(["git", "checkout", "automation", "--", "data/process_state.json"])
subprocess.run(["git", "restore", "--staged", "data/process_state.json"])