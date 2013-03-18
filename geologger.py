from celery import task
import subprocess
import csv
import simplejson as json
import pymongo
import tempfile
import datetime
import urlparse, urllib
import rpy2.robjects as robjects

def csv2json(fname):
    """ Convert CSV file to JSON document for dumping to Mongo """
    reader = csv.DictReader(open(fname,'rU'))
    rows = [row for row in reader]
    return rows

def json2csv(jsond, outfile=None, subkey=None):
    """ Convert regular structured json document to CSV 
        - If outfile is not specified a temporary file is created
        - Subkey will select a subkey of the returned JSON to generate the CSV from:
            Example: 
            jsond = {"data": [ { "date": "2011-15-10T12:00:00Z", "light": "10" } ],
                    "location": [ "a", "b" ], "tagname": "PABU"
                }
            subkey = "data"
            json2csv(jsond,subkey)
            
    """
    jsond = json.loads( jsond )
    if subkey:
        jsond = jsond[subkey]
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
def importTag( uploadloc, tagname, notes, location, user_id="guest" ):
    """ Import a geologger tag to mongodb """ 
    data = {"tagname":tagname, "notes":notes, "release_location": location, "user_id": user_id, 
                "timestamp": datetime.datetime.now().isoformat() 
           }
    data['data'] = csv2json( uploadloc )
    try:
        c = mongoconnect('geologger','lightlogs')
        c.insert( data )
        return url_fix('http://test.cybercommons.org/mongo/db_find/geologger/lightlogs/{"spec":{"tagname":"%s","user_id":"%s"}}' % (tagname,user_id))
    except:
        return "Error saving to mongodb"

@task
def twilightCalc( ligdata, threshold ):
    """ Python wrapper for GeoLight twilightCalc() """
    r = robjects.r
    r.library('GeoLight')
    r.library('RJSONIO')
    r('lig <- read.csv("%s", header=T)' % ligdata)
    r('trans <- twilightCalc(lig$datetime, lig$light, LightThreshold=%s' % threshold)
    return r('toJSON(trans)')

@task
def changeLight( transdata , riseprob, setprob, days ):
    """ Python wrapper for GeoLight changeLight() """
    pass

@task
def distanceFilter( transdata, elevation, distance ):
    """ Python wrapper for GeoLight distanceFilter() """
    pass

@task
def coord( transdata, elevation ):
    """ Python wrapper for GeoLight coord() """
    pass

@task
def sunAngle( transdata, calib_start, calib_stop, release_location ):
    r = robjects.r
    #task_id = sunAngle.request.id
    r.library('GeoLight')
    lat, lon = release_location
    robjects.FloatVector([lat,lon])
    # Place holders for now...will need to accept user upload from web interface instead.
    r('trans <- read.csv( "%s" , header=T)' % transdata )
    r('trans <- twilightCalc(trans$datetime, trans$light, ask=F)')
    r('print(summary(trans))')
    r('calib <- subset(trans, as.numeric(trans$tSecond) < as.numeric(strptime("%s", "%%Y-%%m-%%d %%H:%%M:%%S")))' % (calib_stop))
    r('print(summary(calib))')
    r('elev <- getElevation(calib$tFirst, calib$tSecond, calib$type, known.coord=c(%s,%s), plot=F)' % (lon, lat) )
    elev = r('elev')
    return elev[0]

@task
def getElevation( calibdata, xylocation ):
    """ Wrapper for GeoLight getElevation """ 
    task_id = getElevation.request.id
    inputdata = json2csv( calibdata )
    subprocess.call(['RScript', 'geolight.R', inputdata, xylocation])
    return task_id


