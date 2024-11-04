import re
from toolsGeneral.main import findDuplicates

def test(type):

    match type:
        case 'leaksFromOtherCountries':
            return leaksFromOtherCountries
def testSanitize(raw):
    # some elements have missing name
    miss = list(filter(lambda x: testFunc('missingName')(x), raw['elements']))
    print('missing name: ', len(miss))

    # duplicates elements happens with are method where polygon intersect other countries areas
    dup = list(findDuplicates([ele['id'] for ele in raw['elements']]))
    print('duplicate elements: ', len(dup))

    return [miss, dup]

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
