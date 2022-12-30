import cmr.auth.token as t
import cmr.search.collection as coll
import cmr.search.common as scom
import cmr.util.network as net
import json
import urllib3
import certifi
import pprint
from flask import Flask
import requests

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

def mySearch(env, search_type, query, show=False):
    ''' Constructs a CMR search URL based on three inputs: 
        the environment, type of search, and the query syntax.
        Fourth optional argument will print out the search URL if True. 
        Leverages the eo-metadata-tools python package to submit the
        request, download the results, and return them to the calling module.
    '''
    # CMR base URLs
    cmr_root = {'PROD': 'https://cmr.earthdata.nasa.gov',
                'UAT': 'https://cmr.uat.earthdata.nasa.gov',
                'SIT': 'https://cmr.sit.earthdata.nasa.gov'}
    # Construct the search URL using the inputs
    base = cmr_root[env]
    url = '{}/{}?{}'.format(base, search_type, net.expand_query_to_parameters(query))
    if show:
        print(url)
    try:
        result = net.get(url, accept="application/vnd.nasa.cmr.umm_results+json")
        items = result.get("items", [])
        return items
    except:
        return False

def getCollId(coll, daac='GES_DISC'):
    ''' Returns the collection ID, e.g. C1244165121-GES_DISC
        Input is a collection metadata record
    '''
    return coll["meta"]["concept-id"]

def getCollSvcs(coll, env, daac='GES_DISC'):
    ''' Searches for a collection's associated services.
        Inputs are the collection metadata record and the env.
        Returns the concept_ids and names.
    '''
    try:
        svcL = coll['meta']['associations']['services']
        svcRecs = []
        for svc in svcL: 
            sitems = mySearch(env, 'services', {'provider':daac, 'concept_id':svc})
            svcRecs.append(sitems)
    except:
        svcRecs = []

    return svcRecs

def getCollMeta(cname, env, daac='GES_DISC'):
    ''' Returns a collection's metadata record.
        Input cname has syntax: shortname_version 
        Input env is one of these: SIT, UAT, or PROD
        Input daac is 'GES_DISC' by default
    '''
    return mySearch(env, 'collections', {'native-id':cname,'provider':daac})

def getCollVars(cid, env, pageSize=500):
    ''' Returns the UMM-Var records for variables associated with a collection
    '''
    return mySearch(env, 'variables', {'keyword':cid, 'page_size':pageSize})


app = Flask(__name__)

@app.route('/')
def index():
    return 'App Works!'

@app.route('/ummvar/<string:env>/<string:shortname>/<string:version>/')
def ummvar_query(env, shortname, version):
    c_name = shortname + '_' + version

    out_response = {'Env': env}

    cRecs = getCollMeta(c_name, env)

    cMeta = cRecs[0]

    out_response['collection name'] = cMeta["meta"]["native-id"]
    out_response['collection id'] = cMeta["meta"]["concept-id"]

    cSvcs = getCollSvcs(cMeta, env)

    if cSvcs: 
        for svc in cSvcs:
            out_response[svc[0]['umm']['LongName']] = svc[0]['meta']['concept-id']

    cVars = getCollVars(cMeta["meta"]["concept-id"], env)
    vars = {}
    if cVars:
        for v in cVars:
            # show the umm-var id (V*_GES-DISC) and the name
            vars[v["umm"]["Name"]] = v["meta"]["concept-id"]

    out_response['Variables'] = vars


    output_formatted = pprint.pformat(out_response)

    return output_formatted

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)

"""short_name = 'S5P_L2__AER_AI_HiR'
ver_number = '2'
provider = 'GES_DISC'
env = 'UAT' #CMR_UAT or CMR_OPS for PROD"""