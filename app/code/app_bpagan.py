from cmr import CollectionQuery, GranuleQuery, ToolQuery, ServiceQuery, VariableQuery
from cmr.queries import CMR_UAT, CMR_OPS
import json
import urllib3
import certifi
def webSearch(sn, ver):
    ''' Searches the GESDISC Data Services API endpoint with given parameters:
        sn/shortname e.g. 'SNDRSNIML2CCPRET or M2T1NXSLV')
        ver/version (collection version) e.g. '2' or '5.12.4'
        Returns: response from request
    '''
    ## Pool Manager instance to create a request
    http = urllib3.PoolManager(
        cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
    ## GESDISC API endpoint
    url = 'https://disc.gsfc.nasa.gov/service/datasets/jsonwsp'
    sn_ver = sn + '_' + ver
    ## Construct search request
    search_request = {'methodname': 'search',
                      'type': 'jsonwsp/request',
                      'version': '1.0',
                      'args': {'search': sn_ver}
                      }

    ## Submit the search request to the GES DISC server
    try:
        r = http.request('POST', url, body=json.dumps(search_request),
                         headers={'Content-Type': 'application/json',
                                  'Accept': 'application/json'})
        response = json.loads(r.data)
        if response['result']['totalResults'] > 1:
            print('Too many items match your search entry')
            print('Try again with exact collection ShortName and version')

        elif response['result']['totalResults'] == 0:
            print('No items were found')
        ## Return one response from request
        else:
            return response

    except KeyError:
        if response['type'] == 'jsonwsp/fault':
            print('ERROR! Faulty request. Please try again.')







short_name = 'TRPSYL2HDOCRS1FS'
ver_number = '1'
provider = 'GES_DISC'
mode = CMR_OPS #CMR_UAT or CMR_OPS for PROD 

out_response = {}
api_granule = GranuleQuery(mode=mode)
api_granule.parameters(
    short_name= short_name,
    version = ver_number,
    provider = provider
    )
granules = api_granule.get(1)
if granules[0]:
    cmr_collection_found = True
    out_response.update({'cmr collection found':cmr_collection_found})

    collection_concept_id = granules[0]['collection_concept_id']
    api_collection = CollectionQuery()
    collection_found = api_collection.concept_id(collection_concept_id).get_all()
    out_response.update({'cmr service info':collection_found[0]['service_features']})
else:
    cmr_collection_found = False
    out_response.update({'cmr collection found':cmr_collection_found})




on_prem_search = webSearch(short_name,ver_number)

if on_prem_search:
    on_prem_collection_found = True
    out_response.update({'on prem collection found':on_prem_collection_found})
    #out_response.update({'on_prem_services':{}})

    on_prem_search_services = on_prem_search['result']['items'][0]['services']['subset']
    services_out = []
    for service in on_prem_search_services:
        services_out.append({'service_name': service['serviceAgent'],'capability_name': service['capabilities']})

    out_response.update({'on prem service info':services_out})

else:
    on_prem_collection_found = False
    out_response.update({'on prem collection found':on_prem_collection_found})


    

out_response