import re
from toolsGeneral.main import findDuplicates

def test(type):

    match type:
        case 'leaksFromOtherCountries':
            return leaksFromOtherCountries
        
def testSanitize(elements):
    
    # some elements have missing name
    miss = list(filter(lambda x: testFunc('missingName')(x), elements))
    print('missing name: ', len(miss))

    # duplicates elements happens with are method where polygon intersect other countries areas
    dup = list(findDuplicates([ele['id'] for ele in elements]))
    print('duplicate elements: ', len(dup))

    # relations from other countries
    cntrIso = [
        ele['tags']['ISO3166-1'] 
        for ele in elements 
        if ele['tags']['admin_level'] == '2'
    ][0]

    relLeaks = [
        ele for ele in elements 
        # if not matchISO(ele["tags"].get("ISO3166-2", ""), cntrIso)
        if not matchISO(next((
            ele['tags'][key] 
            for key 
            in ["ISO3166-2", "ref:nuts", 'ref:nuts:2', 'ref:nuts:3', 'ref'] 
            if key in ele['tags'] 
        ), ''), cntrIso)
    ]

    print('relations from other countries: ', len(relLeaks))

    return [miss, dup, relLeaks]

def testFunc(type):
    match type:
        case 'missingName': 
            return lambda x: x['tags'].get("name", '') == ''
        case 'matchISO': 
            return matchISO  


def matchISO(code, cntrCode):

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
