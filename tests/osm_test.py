import re
from toolsOSM.overpass import getCenterNodeInsideParent
import pandas as pd

import logging
# this will create a logger on module import
# later we can add its configuration
logger = logging.getLogger('dup_test_logger')

def osm_basic_test(df):

    cntrRow = df["tags.ISO3166-1"].notna()
    cntrISO = df[cntrRow].iloc[0]["tags.ISO3166-1"]
    cntrName = df[cntrRow].iloc[0]["tags.name"]
    columns = df.columns

    #* some elements have missing name
    miss = df[df["tags.name"].isna()]
    if miss.empty:
        miss = pd.DataFrame(columns=columns)
    print(" * missing names: ", len(miss))


    #* relations from other countries
    checkTags = [
        "tags.is_in:country",
        "tags.ISO3166-2",
        "tags.ref:nuts",
        "tags.ref:nuts:2",
        "tags.ref:nuts:3",
    ]
    leakRows = []
    isInCountry = {}

    for idx, row in df.iterrows():
        osmID = str(row.get("id"))
        parentID = row.get("tags.parent_id")
        foundTag = False
        for tag in checkTags:
            val = row.get(tag)
            if pd.isna(val):
                continue

            foundTag = True

            # Handle explicit country tag
            if tag == "tags.is_in:country":
                if val.strip().lower() == cntrName.strip().lower():
                    isInCountry[osmID] = True
                else:
                    # not the same country -> verify with parent
                    if pd.notna(parentID) and isInCountry.get(parentID):
                        isInCountry[osmID] = True
                    else:
                        leakRows.append(row)
                break  # no need to check other tags
            # Handle ISO-style tags
            elif not checkISO(val, cntrISO):
                leakRows.append(row)
                break
            else:
                isInCountry[osmID] = True
                break
        if not foundTag and pd.notna(parentID) and isInCountry.get(parentID):
            isInCountry[osmID] = True

    leakRowsDF = pd.DataFrame(leakRows, columns=columns)
    print(" * relations from other countries: ", len(leakRowsDF))

    return {    
        "missing.name": miss,
        "leak": leakRowsDF,
    }

def checkISO(code, cntrCode):
    if code == "":
        return True
    iso1 = re.search(r"^([A-Z]{2})", code)
    iso1 = iso1.group(0) if iso1 else ""
    return iso1 == cntrCode

def osm_duplicates_test_center(df, cache = {}):

    keep_elems = []
    delete_elems = []
    center_res = {}
    test_res = {}
    #* duplicates elements happens when a polygon intersect other areas due
    #* to incorrect boundaries
    dup = df[df.duplicated("id", keep=False)]
    logger.debug(f" - duplicate elements by id: {len(dup)}")

    if len(dup) > 0:
        logger.debug(f"  - testing osm center: {len(dup)}")
        for _, row in dup.iterrows():
            logger.debug(f"   - {[row['id'], row['tags.parent_id']]}:")
            if (row['id'], row['tags.parent_id']) in [ele[1:3] for ele in cache.keys()]:
                logger.debug("   - already tested: continue")
                continue
            res = getCenterNodeInsideParent(row["id"], row["tags.parent_id"])
            center_res[(row["id"], row["tags.parent_id"])] = res
            logger.debug(f"  - result: {res['status'], res.get('error_type','')}")

    # normalize test result
    for k,v in center_res.items():
        if v['status'] == 'ok' and len(v['data']['elements']) == 0:
            test_res[k] = {'status':'ok', 'result': False, 'data':v}
            delete_elems.append(k)
        elif v['status'] == 'ok':
            parent = v['data']['elements'][0]
            if str(parent['id']) == k[1]:
                test_res[k] = {'status':'ok', 'result': True, 'data':v}
                keep_elems.append(k)
            else:
                test_res[k] = {'status':'ok', 'result': False, 'data':v}
                delete_elems.append(k)
        else:
            test_res[k] = v
        

    return {
        'test_res': test_res,
        'duplicate.keep_elems': keep_elems,
        'duplicate.delete_elems': delete_elems
    }