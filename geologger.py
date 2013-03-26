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
import os
from util import *

def getTagData(tagname, user_id="guest", db="geologger", col="lightlogs"):
    """ Get light level data for a tag """ 
    url = "http://test.cybercommons.org/mongo/db_find/%s/%s/{'spec':{'tagname':'%s','user_id':'%s'}}" %(db,col,tagname, user_id)
    url_get = urllib.urlopen(url_fix(url)).read()
    if url_get == "[]":
        return {"error": "Empty result"}
    else:
        return json.loads(url_get)[0]

@task
def importTagData( uploadloc, tagname, notes, location, user_id=None ):
    """ Import a geologger tag to mongodb """ 
    if not user_id:
        user_id = getTaskUser(importTagData.request.id)
    data = {
            "tagname":tagname, 
            "notes":notes, 
            "release_location": location, 
            "user_id": user_id, 
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
def twilightCalc( tagname=None, threshold=None ):
    """ Python wrapper for GeoLight twilightCalc() """
    r = robjects.r
    user_id = getTaskUser(twilightCalc.request.id)
    r.library('GeoLight')
    r.library('RJSONIO')
    tagdata = getTagData(tagname,user_id)
    if tagdata != {"error": "Empty result"}:
        ligdata = dict2csv(tagdata,subkey="data")
        r('lig <- read.csv("%s", header=T)' % ligdata)
        r('trans <- twilightCalc(lig$datetime, lig$light, LightThreshold=%s, ask=F)' % threshold)
        c = mongoconnect('geologger','twilights') 
        data = { 
                "data":json.loads(r('toJSON(trans)')[0]), 
                "tagname": tagname, 
                "user_id": user_id, 
                "threshold": threshold, 
                "timestamp": datetime.datetime.now().isoformat(),
                "format": "RJSONIO"
                }
        c.insert(data)
        cleanup([ligdata])
        return 'http://test.cybercommons.org/mongo/db_find/geologger/twilights/{"spec":{"tagname":"%s","user_id":"%s"}}' % (tagname, user_id)
    else:
        return "Had a problem finding lightlog data"

@task
def twilightInsert(tagname=None, data=None, threshold=None):
    """ Take twilight data from web interface """
    c = mongoconnect('geologger','twilights')
    user_id = getTaskUser(twilightInsert.request.id)
    data = { 
        "data": json.loads( data ), 
        "tagname": tagname, 
        "user_id": user_id, 
        "threshold": threshold, 
        "timestamp": datetime.datetime.now().isoformat(),
        "format": "JSON-list"
        }
    c.save(data)
    return 'http://test.cybercommons.org/mongo/db_find/geologger/twilights/{"spec":{"tagname":"%s","user_id":"%s"}}' % (tagname, user_id)

    

@task
def changeLight( tagname=None, riseprob=None, setprob=None, days=None ):
    """ Python wrapper for GeoLight changeLight() """
    r = robjects.r
    user_id = getTaskUser(changeLight.request.id)
    r.library('GeoLight')
    r.library('RJSONIO')
    twilight = df2csv(getTagData(tagname=tagname, user_id=user_id, col="twilights"), subkey="data")
    if len(twilight) < 5:
        return "Twilights have not yet been calculated, please compute twilight events and then try again"
    r('twilight <- read.csv("%s", header=T)' % twilight)
    r('twilight$tFirst <- as.POSIXlt(twilight$tFirst, origin="1970-01-01")') # Convert to R Datetime
    r('twilight$tSecond <- as.POSIXlt(twilight$tFirst, origin="1970-01-01")') # Convert to R Datetime
    r('change <- changeLight(twilight$tFirst, twilight$tSecond, twilight$type, rise.prob=%s, set.prob=%s, days=%s,plot=F)' % (riseprob,setprob,days))
    # Hack to get "." out of variable names so json can be stored in MongoDB
    #   see: "http://docs.mongodb.org/manual/reference/limits/#Restrictions on Field Names"
    r('names(change)[3] <- "rise_prob"')
    r('names(change)[4] <- "set_prob"')
    r('names(change$setProb)[2] <- "prob_y"')
    r('names(change$riseProb)[2] <- "prob_y"')
    r('names(change$migTable)[5] <- "P_start"')
    r('names(change$migTable)[6] <- "P_end"')
    c = mongoconnect('geologger','changelight')
    data = { 
            "data": json.loads(r('toJSON(change)')[0]), 
            "params": { "riseprob": riseprob, "setprob":setprob,"days":days },
            "user_id": user_id, 
            "tagname": tagname,
            "timestamp": datetime.datetime.now().isoformat()
            }
    c.insert(data)
    cleanup([twilight])
    return 'http://test.cybercommons.org/mongo/db_find/geologger/changelight/{"spec":{"tagname":"%s","user_id":"%s"}}' % (tagname, user_id)

@task
def distanceFilter( transdata, elevation, distance ):
    """ Python wrapper for GeoLight distanceFilter() """
    pass

@task
def coord( tagname=None, sunelevation=None ):
    """ Python wrapper for GeoLight coord() """
    user_id = getTaskUser(coord.request.id)
    r = robjects.r
    r.library('GeoLight')
    r.library('RJSONIO')
    twilight = df2csv(getTagData(tagname, user_id, col="twilights"), subkey="data")
    if len(twilight) < 5:
        return "Twilights have not yet been calculated, please compute twilight events then try again"
    r('twilight <- read.csv("%s", header=T)' % (twilight))
    r('twilight$tFirst <- as.POSIXlt(twilight$tFirst, origin="1970-01-01")') # Convert to R Datetime
    r('twilight$tSecond <- as.POSIXlt(twilight$tFirst, origin="1970-01-01")') # Convert to R Datetime
    r('coord <- coord(twilight$tFirst, twilight$tSecond, twilight$type, elevation = %s)' % sunelevation)
    c = mongoconnect('geologger','coord')
    data = { 
            "data": json.loads(r('toJSON(coord)')[0]), 
            "sunelevation": sunelevation, 
            "tagname": tagname, 
            "user_name": user_name,
            "timestamp": datetime.datetime.now().isoformat()    
            }
    c.insert(data)
    cleanup([twilight])
    return 'http://test.cybercommons.org/mongo/db_find/geologger/coord/{"spec":{"tagname":"%s","user_id":"%s"}}' % (tagname,user_id)

@task
def sunAngle( transdata=None, calib_start=None, calib_stop=None, release_location=None ):
    r = robjects.r
    user_id = getTaskUser(twilightCalc.request.id)
    #task_id = sunAngle.request.id
    r.library('GeoLight')
    lat, lon = release_location
    robjects.FloatVector([lat,lon])

    ligdata = dict2csv(getTagData(tagname,user_id),subkey="data")

    # Place holders for now...will need to accept user upload from web interface instead.
    r('trans <- read.csv( "%s" , header=T)' % transdata )
    r('trans <- twilightCalc(trans$datetime, trans$light, ask=F)')
    r('calib <- subset(trans, as.numeric(trans$tSecond) < as.numeric(strptime("%s", "%%Y-%%m-%%d %%H:%%M:%%S")))' % (calib_stop))
    r('elev <- getElevation(calib$tFirst, calib$tSecond, calib$type, known.coord=c(%s,%s), plot=F)' % (lon, lat) )
    elev = r('elev')
    return {"sunelevation": elev[0] }

@task
def getElevation_test( calib=None ):
    r = robjects.r
    user_id = getTaskUser(twilightCalc.request.id)
    r.library('GeoLight')
    r.library('RJSONIO')
    lat, lon = calib['release_location']
    twjson = stringsave(json.dumps(calib['data']))
    r('twilights <- fromJSON(file("%s"))' % twjson)
    r('elev <- getElevation(twilights$tFirst, twilights$tSecond, calib$type, known.coord=c(%s,%s), plot=F)' %(lon, lat) )
    elev = r('elev')
    return { "sunelevation": elev, "user_id": user_id }

@task
def getElevation( data=None ):
    """ 
    Wrapper for GeoLight getElevation 
    Expects data like:
        {
         "data": [
          {
           "active": true,
           "tSecond": "2011-07-30T16:21:30.000Z",
           "tFirst": "2011-07-30T06:58:15.000Z",
           "type": "sunset"
          },
          {
           "active": true,
           "tSecond": "2011-07-31T06:53:08.181Z",
           "tFirst": "2011-07-30T16:21:30.000Z",
           "type": "sunrise"
          },
          {
           "active": true,
           "tSecond": "2011-07-31T16:25:39.230Z",
           "tFirst": "2011-07-31T06:53:08.181Z",
           "type": "sunset"
          }
         ],
         "tagname": "Pabu_test",
         "release_location": [
          35.1,
          -97.0
         ]
        }
    """ 
    if isinstance(data,unicode or str):
        datain = json.loads(data)
    else:
        datain = data
    r = robjects.r
    user_id = getTaskUser(twilightCalc.request.id)
    r.library('GeoLight')
    r.library('RJSONIO')
    lat, lon = datain['release_location']
    tagname = datain['tagname']
    twjson = dict2csv(datain, subkey="data")
    r('twilights <- read.csv("%s", header=T)' % twjson)
    r('twilights$tFirst <- strptime(twilights$tFirst, format="%Y-%m-%dT%H:%M:%OSZ")')
    r('twilights$tSecond <- strptime(twilights$tSecond, format="%Y-%m-%dT%H:%M:%OSZ")')
    r('paste(levels(twilights$type))')
    r('levels(twilights$type) <- c(1,2)')
    r('twilights <- subset(twilights, twilights$active == "True")')
    r('elev <- getElevation(twilights$tFirst, twilights$tSecond, twilights$type, known.coord=c(%s,%s), plot=F)' %(lon, lat) )
    elev = r('elev')
    dataout = { "user_id": user_id, "sunelevation": elev[0], "timestamp": datetime.datetime.now().isoformat() , "tagname": tagname }
    return dataout
    

