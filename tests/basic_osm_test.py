import re
from toolsOSM.overpass import getCenterNodeInsideParent
import pandas as pd

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

def osm_duplicates_test(df):

    #* duplicates elements happens with are method where polygon intersect other countries areas
    dup = df[df.duplicated("id", keep=False)]
    print(" * duplicate elements by id: ", len(dup))

    return {
        'duplicates.id': dup
    }


def testFunc(type):
    match type:
        case "missingName":
            return lambda x: x["tags"].get("name", "") == ""


def checkISO(code, cntrCode):
    if code == "":
        return True
    iso1 = re.search(r"^([A-Z]{2})", code)
    iso1 = iso1.group(0) if iso1 else ""
    return iso1 == cntrCode

def makeFixes(testRes, fixed = {}):

    keepElems = []
    deleteElems = []

    missingName = testRes["missing.name"]
    print("\tmissing name: ", len(missingName))
    for index, row in missingName.iterrows():
        deleteElems.append(row['id'])

    dup = testRes["duplicate.id"]
    print("\tduplicate: ", len(dup))

    dupTestRes = []
    if len(dup) > 0:
        print("\t\ttesting osm center: ", len(dup))
        for groupId, group in dup.groupby("id"):
            for _, row in group.iterrows():
                res = [
                    row["id"],
                    row["tags.parent_id"],
                    getCenterNodeInsideParent(row["id"], row["tags.parent_id"]),
                ]
                dupTestRes.append(res)
                print(f"\t\t* {[row['id'], row['tags.parent_id']]}: {res[2]['status'], res[2].get('error_type','')}")

    dupCorrectElems = list(filter(
        lambda x: x[2]["status"] == "ok"
        and x[1] == str(x[2]["data"]["elements"][0]["id"]),
        dupTestRes,
    ))
    
    deleteElemsGrouped = []
    for ele in dupCorrectElems:
        keepElems.append({'id':ele[0], 'parent_id':ele[1]})
        if dup[0] == ele[0] and dup[1] != ele[1]:
            deleteElemsGrouped.append({'id':dup[0], 'parent_id':dup[1]})
        # deleteElemsGrouped.append([
        #     {'id':dup[0], 'parent_id':dup[1]} 
        #     for dup in dupTestRes 
        #     if dup[0] == ele[0] and dup[1] != ele[1]
        # ])

    leaks = testRes["leak"]
    print("\tleaks: ", len(leaks))
    for index, row in leaks.iterrows():
        deleteElems.append(row['id'])

    return {
        'deleteElems': deleteElems,
        'duplicate.keepElems': keepElems,
        'duplicate.deleteElems': deleteElemsGrouped
    }