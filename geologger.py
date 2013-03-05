from celery import task
import subprocess
import csv
import simplejson as json
import pymongo
import tempfile


def csv2json(file, db, col):
    """ Convert CSV file to JSON document for dumping to Mongo """
    pass

def json2csv(jsond,outfile=None):
    """ Convert regular structured json document to CSV """
    jsond = json.loads( jsond )
    if not outfile:
        outfile = tempfile.NamedTemporaryFile(mode="wb+", delete=False).name
    f = csv.writer(open(outfile,'wb+'))
    f.writerow( jsond[0].keys() )
    for item in jsond:
        f.writerow( item.values() )
    return outfile

@task
def getElevation( calibdata, xylocation ):
    """ Wrapper for GeoLight getElevation """ 
    return json2csv( calibdata )
    #subprocess.call(['R', 'BATCH', '-f', 'getElevation.R'])
    

