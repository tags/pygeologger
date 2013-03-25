import pymongo
import csv
import tempfile
import pandas
import urllib
import urlparse
import os

def csv2json(fname):
    """ Convert CSV file to JSON document """
    reader = csv.DictReader(open(fname,'rU'))
    rows = [row for row in reader]
    return rows

def cleanup( files ):
    for file in files:
        os.remove(file)
    return "Deleted %s" % files

def dict2csv(data, outfile=None, subkey=None):
    """ Convert regular structured list of dictionaries to CSV 
        - If outfile is not specified a temporary file is created and its name returned
        - Subkey will select a subkey of the returned JSON to generate the CSV from:
            Example: 
            data = {"data": [ { "date": "2011-15-10T12:00:00Z", "light": "10" } ],
                    "location": [ "a", "b" ], "tagname": "PABU"
                }
            subkey = "data"
            dict2csv(data,subkey)
            
    """
    if subkey:
        data = data[subkey]
    if not outfile:
        outfile = tempfile.NamedTemporaryFile(mode="wb+", delete=False).name
    f = csv.writer(open(outfile,'wb+'))
    f.writerow( data[0].keys() )
    for item in data:
        f.writerow( item.values() )
    return outfile

def df2csv(data, outfile=None, subkey=None):
    """ Deserializes a JSON representation of an R Data frame convereted using RJSONIO toJSON """
    if subkey:
        data = data[subkey]
    if not outfile:
        outfile = tempfile.NamedTemporaryFile(mode="wb+", delete=False).name
    pandas.DataFrame(data).to_csv(outfile)
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
