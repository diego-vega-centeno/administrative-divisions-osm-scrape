import re
from toolsOSM.overpass import is_child_inside_parent
import pandas as pd
from IPython.display import clear_output
import os
import toolsGeneral.main as tgm
import toolsGeneral.logger as tgl
import logging
from pathlib import Path
import time
# this will create a logger on module import
# later we can add its configuration
logger = tgl.initiate_logger('logger')


def osm_basic_test(df_input, save_logger_path = None):

    if save_logger_path:
        tgl.add_file_handler(logger, save_logger_path)

    #* sort the dataframe
    df = df_input.copy()
    df = df.sort_values('tags.admin_level', key=lambda col: col.astype(int)) 

    cntrRow = df["tags.ISO3166-1"].notna()
    cntrISO = df[cntrRow].iloc[0]["tags.ISO3166-1"]
    cntrName = df[cntrRow].iloc[0]["tags.country_name"]
    cntr_id = df[cntrRow].iloc[0]["id"]

    #* some elements have missing name
    miss = df[df["tags.name"].isna()]['id'].to_list()
    logger.info(f" * missing names: {len(miss)}")

    #* relations from other countries
    #* only discard the ones we are sure are not from the country
    #* pass the test otherwise

    checkTags = [
        "tags.is_in:country",
        "tags.ISO3166-2",
        "tags.ref:nuts",
        "tags.ref:nuts:2",
        "tags.ref:nuts:3",
        "tags.addr:country"
    ]
    leak = []
    in_country = []
    isInCountry = {}
    NA_result = []

    for idx, row in df.iterrows():
        true_count = 0
        false_count = 0
        osmID = str(row.get("id"))
        if str(row.get("tags.admin_level")) == '2':
            isInCountry[osmID] = True
            in_country.append((osmID, pd.NA, cntr_id))
            continue
        parentID = row.get("tags.parent_id")

        for tag in checkTags:
            # we'll check all tags to have some confidence meassure of the test
            val = row.get(tag)
            if pd.isna(val):
                continue

            # Handle is_in:country tag
            if tag == "tags.is_in:country":
                if val.strip().lower() == cntrName.strip().lower():
                    true_count += 1
                else:
                    false_count += 1
                continue

            # Handle ISO-style tags
            checkISO_res = checkISO(val, cntrISO)
            if checkISO_res is True:
                true_count += 1
            elif checkISO_res is False:
                false_count += 1
            # else NA, ignore


        # meassure confidence of results

        if true_count >= 2:
            in_country.append((osmID, parentID, cntr_id))
            isInCountry[osmID] = True
        elif false_count > 0:
            leak.append((osmID, parentID, cntr_id))
            isInCountry[osmID] = False
        elif true_count <= 1:
            # single weak signal, fallback to parent
            # except for first level (4)
            if parentID and isInCountry.get(parentID) is True and row.get('tags.admin_level') != '4':
                in_country.append((osmID, parentID, cntr_id))
                isInCountry[osmID] = True
            else:
                NA_result.append((osmID, parentID, cntr_id))
                isInCountry[osmID] = pd.NA

    logger.info(f" * relations from other countries: {len(leak)}")

    return {    
        "missing.name": miss,
        "leak": leak,
        "in_country": in_country,
        'NA_result': NA_result,
    }

def checkISO(code, cntrCode):
    if code == "": return pd.NA
    iso1 = re.search(r"^([A-Z]{2})", code)

    if not iso1:
        return pd.NA
    else:
        iso1 = iso1.group(0)

    return iso1 == cntrCode

def osm_test_center(rows, save_temp=False, save_path=''):

    total = len(rows)
    if save_temp:
        test_res = tgm.load(save_path) if os.path.exists(save_path) else {}

    for i, (idx, row) in enumerate(rows.iterrows(), start=1):
        clear_output(wait=True)
        logger.info(f'* country: {Path(save_path).parent.name}')
        tuple_id = (row["id"], row["tags.parent_id"], row["tags.country_id"])
        logger.info(f" ^ [{i}/{total}]: testing {tuple_id}:")

        res = is_child_inside_parent(row["id"], row["tags.parent_id"])
        test_res[tuple_id] = res

        resume  = {k:v['status'] for k,v in res.items()}
        status_list = [v['status'] for k,v in res.items()]

        if save_temp and 'error' not in status_list:
            logger.info(f"  * saving ...")
            tgm.dump(save_path, test_res)

        logger.info(f" $ finished: status: {resume}")
        
        time.sleep(3)
    return test_res