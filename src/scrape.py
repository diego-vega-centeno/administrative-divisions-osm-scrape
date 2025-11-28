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
raw_path = DATA_DIR / 'raw/osm countries queries'
countries_processed = {f.parent.name for f in raw_path.glob('*/*.json')}

# skip already processed countries
to_scrape = [t for t in tuples if t[0] not in countries_processed]
to_scrape = to_scrape[:2]
to_scrape = [ ('Peru', '288247', ['4', '6', '8'])]

# fetch admin
for country, id, lvls in to_scrape:
    too.fetch_admin_osm_structure((country, id, lvls), SAVE_DIR, method='chunks')
