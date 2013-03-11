from celery import task
import subprocess
import csv
import simplejson as json
import pymongo
import tempfile
import urlparse, urllib

def csv2json(fname):
    """ Convert CSV file to JSON document for dumping to Mongo """
    reader = csv.DictReader(open(fname))
    rows = [row for row in reader]
    return rows

def json2csv(jsond, outfile=None):
    """ Convert regular structured json document to CSV """
    jsond = json.loads( jsond )
    if not outfile:
        outfile = tempfile.NamedTemporaryFile(mode="wb+", delete=False).name
    f = csv.writer(open(outfile,'wb+'))
    f.writerow( jsond[0].keys() )
    for item in jsond:
        f.writerow( item.values() )
    return outfile

def url_fix(s, charset='utf-8'):
    """ Replace unsafe characters in URLs """ 
    if isinstance(s, unicode):
        s = s.encode(charset, 'ignore')
    scheme, netloc, path, qs, anchor = urlparse.urlsplit(s)
    path = urllib.quote(path,'/%')
    qs = urllib.quote_plus(qs, ':&=')
    return urlparse.urlunsplit((scheme, netloc, path, qs, anchor))

def mongoconnect(db,col):
    """ Connect to Mongo and return connection object, assumes localhost to 
    force installation of mongos on host
    """ 
    return pymongo.Connection()[db][col]

@task
def importTag( uploadloc, tagname, location ):
    """ Import a geologger tag to mongodb """ 
    data = {"tagname":tagname, "location": location }
    data['data'] = csv2json( uploadloc )
    try:
        c = mongoconnect('geologger','lightlogs')
        c.insert( data ) 
        return url_fix('http://test.cybercommons.org/mongo/db_find/geologger/lightlogs/{"spec":{"tagname":"%s"}}' % tagname)
    except:
        return "Error saving to mongodb" 

@task
def getElevation( calibdata, xylocation ):
    """ Wrapper for GeoLight getElevation """ 
    task_id = getElevation.request.id
    
    inputdata = json2csv( calibdata )
    subprocess.call(['RScript', 'geolight.R', inputdata, xylocation])

