import re
from toolsGeneral.main import findDuplicates
from toolsOSM.overpass import getCenterNodeInsideParent
import pandas as pd
import time

def test(type):

    match type:
        case 'leaksFromOtherCountries':
            return leaksFromOtherCountries
        
def testOSMElements(df):
    
    cntrRow = df['tags.ISO3166-1'].notna()
    cntrISO = df[cntrRow].iloc[0]['tags.ISO3166-1']
    cntrName = df[cntrRow].iloc[0]['tags.name']
    print(cntrName, '-', df[cntrRow].iloc[0]['id'])

    # some elements have missing name
    miss = df[df['tags.name'].isna()]
    print('\tmissing names: ', len(miss))

    # duplicates elements happens with are method where polygon intersect other countries areas
    dup = df[df.duplicated('id', keep=False)]
    dupTestRes = []
    print('\tduplicate elements by id: ', len(dup))
    if(len(dup) > 0):
      print('\t\ttesting osm center: ', len(dup))
      # dupTestRes = [getCenterNodeInsideParent(c,p) for c,p in zip(dupToTest['id'],dupToTest['tags.parent_id'])]
      for groupId, group in dup.groupby('id'):
        for _, row in group.iterrows():
          res = [
            row['id'],
            row['tags.parent_id'],
            getCenterNodeInsideParent(row['id'],row['tags.parent_id'])
          ]
          dupTestRes.append(res)
          print(f'\t\t* {[row['id'], row['tags.parent_id']]}: {res[2]['status']}')
      # dupTestRes = list(filter(lambda x: x[2]['status']=='ok' and len(x[2]['elements'])>0, dupTestRes))


    # relations from other countries
    checkTags = ["tags.is_in:country", "tags.ISO3166-2", "tags.ref:nuts", 'tags.ref:nuts:2', 'tags.ref:nuts:3']
    leakRows = []
    isInCountry = {}

    for idx, row in df.iterrows():
      osmID = str(row.get('id'))
      parentID = row.get('tags.parent_id')
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
          break # no need to check other tags
        # Handle ISO-style tags
        elif not checkISO(val, cntrISO):
          leakRows.append(row)
          break
        else:
          isInCountry[osmID] = True
          break
      if not foundTag and pd.notna(parentID) and isInCountry.get(parentID):
        isInCountry[osmID] = True


    print('\trelations from other countries: ', len(leakRows))

    return {'missing.name':miss, 'duplicate.id':dup, 'duplicate.centerTest':dupTestRes, 'leak':leakRows}

def testFunc(type):
    match type:
        case 'missingName': 
            return lambda x: x['tags'].get("name", '') == ''


def checkISO(code, cntrCode):
    if code == '': return True
    iso1 = re.search(r"^([A-Z]{2})", code)
    iso1 = iso1.group(0) if iso1 else ''
    return iso1 == cntrCode

def leaksFromOtherCountries(data, cntrId):
    failed = {k:v
        for k,v 
        in data.items() 
        if not testFunc('matchISO')(
            v["tags"].get("ISO3166-2", ""), 
            data[cntrId]['tags']['ISO3166-1']
        )
    }

    print('Entities that not belong to the country: ', len(failed))

    return failed
