#!/usr/bin/env python
# coding: utf-8

# init
import sys
import os
import time
from importlib import reload
from pathlib import Path

import toolsGeneral.logger as tgl
import toolsGeneral.main as tgm
import toolsOSM.overpass as too

def pckgs_reload():
    reload(tgm)
    reload(tgl)
    reload(too)

pckgs_reload()


# Initialize variables
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SAVE_DIR = DATA_DIR / 'raw/osm countries queries'

raw_scrape_logger = tgl.initiate_logger('raw_scrape_logger', DATA_DIR / 'raw/raw_scrape.log')

osmMetaCountrDict = tgm.load(DATA_DIR / "osmMetaCountrDict.json")

tuples = sorted(
    [(k, v["id"], v["addLvlsNum"]) for (k, v) in osmMetaCountrDict.items()],
    key=lambda arg: arg[0]
)

# exclude processed countries
processed_countries = tgm.load(SAVE_DIR / "processed_countries.pkl")
# processed_countries = {f.parent.name for f in SAVE_DIR.glob('*/*.json')}

# skip already processed countries
to_scrape = [t for t in tuples if t[0] not in processed_countries]
to_scrape = to_scrape[:5]

to_scrape = [ ('Peru', '288247', ['4', '6', '8'])]

# load files
failed_file = SAVE_DIR / 'failed_countries.pkl'
failed_countries = tgm.load(failed_file) if os.path.exists(failed_file) else set()

processed_file = SAVE_DIR / 'processed_countries.pkl'
processed_countries = tgm.load(processed_file) if os.path.exists(processed_file) else set()

# fetch admin
for country, id, lvls in to_scrape:
    raw_scrape_logger.info(f"* processing: {country, id, lvls}")
    
    country_save_file = SAVE_DIR / country / f'rawOSMRes.json'
    response = too.getOSMIDAddsStruct(id, lvls)
    raw_scrape_logger.info(f"  - finished: {response['status']}")

    if response["status"] == "ok":
        tgm.dump(country_save_file, response["data"])
        processed_countries.add(country)
        tgm.dump(processed_file, processed_countries)
    elif '429' in response["status_type"]:
        raw_scrape_logger.info(f"  - Too many requests error, trying chunks")
        too.getOSMIDAddsStruct_chunks((country, id, lvls), SAVE_DIR)
    else:
        raw_scrape_logger.info(f"  - Failed, saving to failed_countries")
        failed_countries.add(country)
        tgm.dump(failed_file, failed_countries)
    
    time.sleep(3)

raw_scrape_logger.info(f"* processed_countries: {len(processed_countries)}")
raw_scrape_logger.info(f"* failed_countries: {len(failed_countries)}")


# for country, id, lvls in to_scrape:

#     too.getOSMIDAddsStruct_chunks(tuple, SAVE_DIR)
#     time.sleep(3)
