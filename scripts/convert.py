import subprocess
from pathlib import Path
import sys

SCRAPE_REPO = Path(r"C:/Users/gonta/D/study/full stack/projects/administrative divisions osm scrape")

ROOT = Path(__file__).resolve().parents[1]
source_notebook = SCRAPE_REPO / "notebooks/scrape.ipynb"
to_convert_notebook = ROOT / "notebooks" / "scrape.ipynb"
converted_file = ROOT / "src" / "scrape"

def convert_notebook():
    # convert
    subprocess.run([
        sys.executable,
        "-m",
        "nbconvert",
        "--to",
        "script",
        str(to_convert_notebook),
        "--output",
        converted_file
    ], check=True)

    print("Converted and moved:", converted_file)

if __name__ == "__main__":
    convert_notebook()