import shutil
from pathlib import Path

SCRAPE_REPO = Path(r"C:/Users/gonta/D/study/full stack/projects/administrative divisions osm scrape")

ROOT = Path(__file__).resolve().parents[1]
notebooks = ["notebooks/scrape.ipynb"]
files = ["data/osmMetaCountrDict.json"]
to_convert_notebook = ROOT / "to_convert_notebooks" / "scrape.ipynb"
converted_file = ROOT / "src" / "scrape"

def copy_files():
    for nb in notebooks:
        shutil.copy2(SCRAPE_REPO / nb, ROOT / nb)
        print(f"Notebook copied to {ROOT / nb}")
    for file in files:
        shutil.copy2(SCRAPE_REPO / file, ROOT / file)
        print(f"File copied to {ROOT / file}")

if __name__ == "__main__":
    copy_files()