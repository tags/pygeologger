from celery import task
import subprocess
import csv
import simplejson as json
import pymongo
import tempfile
import datetime
import urlparse, urllib
import pandas
import rpy2.robjects as robjects
from util import *

def getTagData(tagname, user_id, db="geologger", col="lightlogs"):
    """ Get light level data for a tag """ 
    url = "http://test.cybercommons.org/mongo/db_find/%s/%s/{'spec':{'tagname':'%s','user_id':'%s'}}" %(db,col,tagname, user_id)
    return json.loads(urllib.urlopen(url_fix(url)).read())[0]

@task
def importTagData( uploadloc, tagname, notes, location, user_id="guest" ):
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
def twilightCalc( tagname, user_id, threshold ):
    """ Python wrapper for GeoLight twilightCalc() """
    r = robjects.r
    r.library('GeoLight')
    r.library('RJSONIO')
    ligdata = dict2csv(getTagData(tagname,user_id),subkey="data")
    r('lig <- read.csv("%s", header=T)' % ligdata)
    r('trans <- twilightCalc(lig$datetime, lig$light, LightThreshold=%s, ask=F)' % threshold)
    try:
        c = mongoconnect('geologger','twilights')
        data = { "data":json.loads(r('toJSON(trans)')[0]), "tagname": tagname, "user_id": user_id, "threshold": threshold }
        c.insert(data)
        return 'http://test.cybercommons.org/mongo/db_find/geologger/twilights/{"spec":{"tagname":"%s","user_id":"%s"}}' % (tagname, user_id)
    except:
        return "Trouble persisting results to mongo...try again later"

@task
def changeLight( tagname, riseprob, setprob, days ):
    """ Python wrapper for GeoLight changeLight() """
    user_id_ = changeLight.request.id
    user_id = "guest"
    r = robjects.r
    r.library('GeoLight')
    r.library('RJSONIO')
    twilight = df2csv(getTagData(tagname, user_id, col="twilights"), subkey="data")
    if len(twilight) < 5:
        return "Twilights have not yet been calculated, please compute twilight events and then try again"
    r('twilight <- read.csv("%s", header=T)' % twilight)
    r('twilight$tFirst <- as.POSIXlt(twilight$tFirst, origin="1970-01-01")') # Convert to R Datetime
    r('twilight$tSecond <- as.POSIXlt(twilight$tFirst, origin="1970-01-01")') # Convert to R Datetime
    r('change <- changeLight(twilight$tFirst, twilight$tSecond, twilight$type, rise.prob=%s, set.prob=%s, days=%s)' % (riseprob,setprob,days))
    c = mongoconnect('geologger','changelight')
    data = { "data": json.loads(r('toJSON(change)')[0]), "user_id_": user_id_ }
    c.insert(data)
    return 'http://test.cybercommons.org/mongo/db_find/geologger/changelight/{"spec":{"tagname":"%s","user_id":"%s"}}' % (tagname, user_id)

foo 


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
    r('calib <- subset(trans, as.numeric(trans$tSecond) < as.numeric(strptime("%s", "%%Y-%%m-%%d %%H:%%M:%%S")))' % (calib_stop))
    r('elev <- getElevation(calib$tFirst, calib$tSecond, calib$type, known.coord=c(%s,%s), plot=F)' % (lon, lat) )
    elev = r('elev')
    return {"sunelevation": elev[0] }

@task
def getElevation( calibdata, xylocation ):
    """ Wrapper for GeoLight getElevation """ 
    task_id = getElevation.request.id
    inputdata = dict2csv( calibdata )
    subprocess.call(['RScript', 'geolight.R', inputdata, xylocation])
    return task_id


