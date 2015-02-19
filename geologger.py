from celery import task
import subprocess
import csv
import simplejson as json
import pymongo
import tempfile
import datetime, time
import urlparse, urllib
import pandas
import rpy2.robjects as robjects
import os
from util import *
import geojson

template = """
library(lattice)
library(ggplot2)
library(plyr)
%(version)s
"""

def runR(datain, 
            script,
            outformat, 
            saveoutput=False,
            savedisplay=False,
            saverdata=False):
    """ Helper function to make running R scripted tasks easier"""
    # Set current work directory to a tmp dir for R script, gather up all output from there when done.
    r = robjects.r
    if saveoutput | savedisplay:
        tempdir = ""# create temporary directory 
    if saveoutput:
        r('setwd("%s")' % tempdir )
    # Optionally store and persist .RData to disk
    # PDF Grabbing - grab PDF output and place in sensible location
    if savedisplay:
        r('pdf("%s")' % tempdir )
    r(script)
    # cleanup temp directory
    if saverdata & saveoutput:
        r('save.image()')
    return 

def getTagData(tagname, user_id="guest", db="geologger", col="lightlogs"):
    """ Get light level data for a tag """ 
    url = "http://test.cybercommons.org/mongo/db_find/%s/%s/{'spec':{'tagname':'%s','user_id':'%s'}}" %(db,col,tagname, user_id)
    url_get = urllib.urlopen(url_fix(url)).read()
    if url_get == "[]":
        return {"error": "Empty result"}
    else:
        return json.loads(url_get)[0]

@task
def importTagData_manual( uploadloc, tagname, notes, location, dateformat=None , task_id=None, user_id=None):
    """ Import a geologger tag to mongodb """ 
    data = {
            "tagname":tagname, 
            "notes": notes, 
            "release_location": location, 
            "user_id": user_id, 
            "timestamp": "%sZ" % datetime.datetime.now().isoformat(),
            "task_id": task_id
           }
    data['data'] = csv2json(uploadloc, dateformat)
    try:
        c = mongoconnect('geologger','lightlogs')
        c.insert( data )
        return url_fix('http://test.cybercommons.org/mongo/db_find/geologger/lightlogs/{"spec":{"tagname":"%s","user_id":"%s"}}' % (tagname,user_id))
    except:
        return "Error saving to mongodb"
@task
def importTagData( data=None, task_id=None, user_id=None ):
    """ A task for importing geologger tag data """
    if isinstance(data,unicode or str):
        datain = json.loads(data)
    else:
        datain = data

    dataout = { "data": datain['data'],
                "tagname": datain['tagname'],
                "notes": datain['notes'],
                "species": datain['species'],
                "timestamp": "%sZ" % datetime.datetime.now().isoformat(),
                "user_id": user_id,
                "task_id": task_id
              }
    try: 
        c = mongoconnect('geologger','lightlogs')
        c.insert(dataout)
        return url_fix('http://test.cybercommons.org/mongo/db_find/geologger/lightlogs/{"spec":{"tagname":"%s","user_id":"%s"}}' % (dataout['tagname'],dataout['user_id']))
    except:
        return "Error saving to mongo"


@task
def twilightCalc( tagname=None, threshold=None, task_id=None, user_id=None):
    """ Python wrapper for GeoLight twilightCalc() """
    r = robjects.r
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
                "format": "RJSONIO",
                "task_id": task_id
                }
        c.insert(data)
        cleanup([ligdata])
        return 'http://test.cybercommons.org/mongo/db_find/geologger/twilights/{"spec":{"tagname":"%s","user_id":"%s"}}' % (tagname, user_id)
    else:
        return "Had a problem finding lightlog data"

@task
def twilightInsert(tagname=None, data=None, threshold=None, task_id=None,user_id=None):
    """ Take twilight data from web interface """
    c = mongoconnect('geologger','twilights')

    data = { 
        "data": json.loads( data ), 
        "tagname": tagname, 
        "user_id": user_id, 
        "threshold": threshold, 
        "timestamp": datetime.datetime.now().isoformat(),
        "format": "JSON-list",
        "task_id": task_id
        }
    c.save(data)
    return 'http://test.cybercommons.org/mongo/db_find/geologger/twilights/{"spec":{"tagname":"%s","user_id":"%s"}}' % (tagname, user_id)

    
@task
def deleteTag(tagname=None, user_id=None):
    l = mongoconnect('geologger','lightlogs')
    l.remove({"tagname":tagname,"user_id":user_id})
    t = mongoconnect('geologger','twilights')
    t.remove({"tagname":tagname,"user_id":user_id})
    c = mongoconnect('geologger','coord')
    c.remove({"tagname":tagname,"user_id":user_id})

@task
def changeLight( tagname=None, riseprob=None, setprob=None, days=None, task_id=None, user_id=None):
    """ Python wrapper for GeoLight changeLight() """
    r = robjects.r
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
            "timestamp": datetime.datetime.now().isoformat(),
            "task_id": task_id
            }
    c.insert(data)
    cleanup([twilight])
    return 'http://test.cybercommons.org/mongo/db_find/geologger/changelight/{"spec":{"tagname":"%s","user_id":"%s"}}' % (tagname, user_id)

@task
def distanceFilter( transdata, elevation, distance, task_id=None, user_id=None ):
    """ Python wrapper for GeoLight distanceFilter() """
    pass

@task
def coord( data=None, task_id=None, user_id=None ):
    """ Python wrapper for GeoLight coord() 
        expects data like:
        data = { 
                         "tagname": "PABU_test",
                         "sunelevation": -4.5,
                         "computed": True,
                         "threshold": 4.5,
                         "twilights": [{ 
                            "tFirst": "2011-07-30T15:21:24.000Z", 
                            "tSecond": "2011-07-31T15:21:24.000Z",
                            "type": "sunrise",
                            "active": True
                            }, { 
                            "tFirst": "2011-07-30T15:21:24.000Z", 
                            "tSecond": "2011-07-31T15:21:24.000Z",
                            "type": "sunrise",
                            "active": True
                            }],
                          "calibperiod": ["2011-07-30T15:21:24.000Z", "2011-07-30T15:21:24.000Z"]
                    
                }

        Data can be provided as JSON string or as a python dictionary.
    """
    if isinstance(data,unicode or str):
        datain = json.loads(data)
    else:
        datain = data

    datain['user_id'] = user_id
    datain['timestamp'] = datetime.datetime.now().isoformat()
    tagname = datain['tagname']
    sunelevation = datain['sunelevation']
    r = robjects.r
    r.library('GeoLight')
    r.library('RJSONIO')
    # Save input twilights from UI
    t = mongoconnect('geologger','twilights')
    t.save(datain)
    # Convert input to csv for reading in R
    twilight = df2csv(datain, subkey="twilights")
    r('twilights <- read.csv("%s", header=T)' % (twilight))
    # Filter actives
    r('twilights <- subset(twilights, twilights$active == "True")')
    # Convert sunrise/sunset to 1,2
    r('twilights$typecat[twilights$type == "sunrise"] <- 1')
    r('twilights$typecat[twilights$type == "sunset"] <- 2')
    # Convert datetimes
    r('twilights$tFirst <- as.POSIXct(strptime(twilights$tFirst, format="%Y-%m-%dT%H:%M:%OSZ", tz="GMT"))')
    r('twilights$tSecond <- as.POSIXct(strptime(twilights$tSecond, format="%Y-%m-%dT%H:%M:%OSZ", tz="GMT"))')
    r('coord <- coord(twilights$tFirst, twilights$tSecond, twilights$typecat, degElevation = %s)'% sunelevation) 
    r('coord <- as.data.frame(cbind(as.data.frame(coord), twilights$tFirst, twilights$tSecond))' ) 
    
    r('names(coord) <- c("x","y","tFirst","tSecond")')
    r('coord <- subset(coord, !is.na(y) & !is.na(x))')
    r('coord$tFirst <- as.character(strftime(coord$tFirst, "%Y-%m-%dT%H:%M:%SZ"))')
    r('coord$tSecond <- as.character(strftime(coord$tSecond, "%Y-%m-%dT%H:%M:%SZ"))')
    #r('coord <- subset(coord, !is.na(x))')
    d = mongoconnect('geologger', 'debug')
    c = mongoconnect('geologger','coord')


#    dataout = dict(geojson.FeatureCollection(geojson.Feature(geojson.MultiPoint(json.loads(r('toJSON(coord)')[0])))))
    df = pandasdf(json.loads(r('toJSON(coord)')[0]))
    track = [ dict([
        (colname, row[i]) 
        for i,colname in enumerate(df.columns)
        ])
        for row in df.values
    ]

    d.insert({"dataframe": df.to_string(), "fromR": json.loads(r('toJSON(coord)')[0])})
        
    dataout = json.loads(
                geojson.dumps(
                    geojson.FeatureCollection( [
                         geojson.Feature(geometry=geojson.Point(
                            [item['x'],item['y']]), properties={"tFirst": item['tFirst'], "tSecond": item['tSecond']}
                          ) 
                            for item in track 
                        ] 
                    )
                )
              )
    dataout['properties'] = {  
            "sunelevation": sunelevation, 
            "tagname": tagname, 
            "user_id": user_id,
            "timestamp": datetime.datetime.now().isoformat(),
            "task_id": task_id   
        }
    c.insert(dataout)
    cleanup([twilight])
    return 'http://test.cybercommons.org/mongo/db_find/geologger/coord/{"spec":{"tagname":"%s","user_id":"%s"}}' % (tagname,user_id)


@task
def getElevation( data=None, task_id=None, user_id=None):
    """ 
    Wrapper for GeoLight getElevation 
    Expects data like:
     data =  {
         "twilights": [
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
         ],
        "threshold": 5.5
        }
    """ 
    if isinstance(data,unicode or str):
        datain = json.loads(data)
    else:
        datain = data
   
    r = robjects.r
    r.library('GeoLight')
    r.library('RJSONIO')
    lat, lon = datain['release_location']
    tagname = datain['tagname']
    twjson = dict2csv(datain, subkey="twilights")
    r('twilights <- read.csv("%s", header=T)' % twjson)
    r('twilights$tFirst <- strptime(twilights$tFirst, format="%Y-%m-%dT%H:%M:%OSZ")')
    r('twilights$tSecond <- strptime(twilights$tSecond, format="%Y-%m-%dT%H:%M:%OSZ")')
    r('paste(levels(twilights$type))')
    r('levels(twilights$type) <- c(1,2)')
    r('twilights <- subset(twilights, twilights$active == "True")')
    r('elev <- getElevation(twilights$tFirst, twilights$tSecond, twilights$type, known.coord=c(%s,%s), plot=F)' %(lon, lat) )
    elev = r('elev')
    dataout = { "task_id": task_id, "user_id": user_id, "sunelevation": elev[0], "timestamp": datetime.datetime.now().isoformat() , "tagname": tagname }
    return dataout
    

