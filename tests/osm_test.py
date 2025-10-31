import re
from toolsOSM.overpass import getCenterNodeInsideParent
import pandas as pd
from IPython.display import clear_output
import os
import toolsGeneral.main as tgm
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
    miss = df[df["tags.name"].isna()]['id'].to_list()
    # if miss.empty:
    #     miss = pd.DataFrame(columns=columns)
    print(" * missing names: ", len(miss))


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
    leakRows = []
    isInCountry = {}

    for idx, row in df.iterrows():

        osmID = str(row.get("id"))
        if str(row.get("tags.admin_level")) == '2':
            isInCountry[osmID] = True
            continue
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

    leakRows = [row['id'] for row in leakRows]
    print(" * relations from other countries: ", len(leakRows))

    return {    
        "missing.name": miss,
        "leak": leakRows,
    }

def checkISO(code, cntrCode):
    if code == "":
        return True
    iso1 = re.search(r"^([A-Z]{2})", code)
    iso1 = iso1.group(0) if iso1 else ""
    return iso1 == cntrCode

def osm_duplicates_test_center(df, processed_info, test_all_df):

    keep_elems = []
    delete_elems = []
    center_res = {}
    test_res = {}
    #* duplicates elements happens when a polygon intersect other areas due
    #* to incorrect boundaries
    if not test_all_df:
        dup = df[df.duplicated("id", keep=False)]
    else:
        dup = df
    logger.info(f" - duplicate elements by id: {len(dup)}")

    total = len(dup)
    if len(dup) > 0:
        logger.info(f" - testing osm center: {len(dup)}")
        for i, (idx, row) in enumerate(df.iterrows(), start=1):
            logger.info(f"  * [{i}/{total}]: testing {[row['id'], row['tags.parent_id']]}:")
            if (row['id'], row['tags.parent_id']) in [ele[1:3] for ele in processed_info.keys()]:
                logger.info("  * already tested: pass")
                continue
            res = getCenterNodeInsideParent(row["id"], row["tags.parent_id"], logger)
            center_res[(row["id"], row["tags.parent_id"])] = res

            k, v = (row["id"], row["tags.parent_id"]), res

            # normalize test results
            if v['status'] == 'ok' and len(v['data']['elements']) == 0:
                test_res[k] = {'status':'ok', 'result': False, 'data':v['data']}
                delete_elems.append(k)
            elif v['status'] == 'ok':
                parent = v['data']['elements'][0]
                if str(parent['id']) == k[1]:
                    test_res[k] = {'status':'ok', 'result': True, 'data':v['data']}
                    keep_elems.append(k)
                else:
                    test_res[k] = {'status':'ok', 'result': False, 'data':v['data']}
                    delete_elems.append(k)
            else:
                test_res[k] = v
            logger.info(f"  * finished: status: {test_res[k]['status'], test_res[k].get('error_type','')}; result: {test_res[k].get('result','')}")
        
    return {
        'test_res': test_res,
        'duplicate.keep_elems': keep_elems,
        'duplicate.delete_elems': delete_elems
    }

def countries_run_test(
    df,
    test,
    logger,
    save=True,
    save_dir=None,
    processed_info=None,
    test_all_df=False
):
    test_res = {}
    to_test = list(df.items())
    total = len(to_test)
    for i, (cntr, df) in enumerate(to_test, start=1):
        chunk_size = 2
        acumulated_res = {}
        chunks_index = range(00, len(df), chunk_size)
        for j,chunk_start in enumerate(chunks_index, start=1):
            clear_output(wait=True)
            logger.info(f"[{i}/{total}] Processing {cntr}")
            logger.info(f"  * chunk size {chunk_size}: current [{j}/{len(chunks_index)}]")
            chunk_df = df[chunk_start:chunk_start + chunk_size] 
            chunk_res = test(
                df=chunk_df,
                processed_info=processed_info,
                test_all_df=test_all_df
            )
            acumulated_res.get(cntr, []).append(chunk_res)
            tgm.dump(os.path.join(save_dir, f"chunk_test_res.pkl"), acumulated_res)

        # join chunks results
        key_test_res = {}
        Key_duplicate_keep_elems = []
        Key_duplicate_delete_elems = []
        for res in acumulated_res:
            key_test_res.update(res['test_res'])
            Key_duplicate_keep_elems.append(res['duplicate.keep_elems']) 
            Key_duplicate_delete_elems.append(res['duplicate.delete_elems'])
        
        # make test res from chunks
        test_res_curr = {
            'test_res': key_test_res,
            'duplicate.keep_elems': Key_duplicate_keep_elems,
            'duplicate.delete_elems': Key_duplicate_delete_elems
        }

        if save:
            data_path = os.path.join(save_dir, f"{cntr}/{cntr}_test_res.pkl")
            if not os.path.exists(data_path):
                tgm.dump(data_path, test_res_curr)
        test_res[cntr] = test_res_curr

    return test_res