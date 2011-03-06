#
# Funcions to initialize environment for SAME test submissions
#

import time
import os
import sys
import re
#import rgma
#import RgmaIter
from Config import config
import ConfigParser
import Log
import Database


#
# Function to create the list of resource from the GOC DB
#
def gocSiteList (outfile):
    

    Log.debug("Creating new GOC list file " + outfile )
   
    nodes = None
    if config:
        try: # Issuing the WS query
            wsdl = config.get("webservices", "query_wsdl")
            Log.debug("Querying web service at %s" % wsdl)

            # define query parameters
            attrs = ['siteid', 'regionname', 'sitename', 'serviceabbr', 
                    'nodename', 'ismonitored', 'status', 'type', 
                    'inmaintenance']
            filter = {}

            ws = Database.Database(wsdl)
            nodes = ws.query(attrs, filter)
        except ConfigParser.NoOptionError:
            Log.warn("Web service url not configured")
        except Exception:
            Log.warn("Web service query failed")

    # First using a temporary file     
    # If this could be created, it will be moved to the permanent one     
    timestamp=time.strftime("%y-%m-%d-%H%M%S")   
    tmpfile=outfile + '-' + timestamp
    Log.debug("Writing first to temporary file " + tmpfile )
    
    # Opening file
    try: 
        of=file(tmpfile, 'w')
    except IOError:
        Log.critical("An IO error occured while opening file " + outfile + ". Exiting...")
        sys.exit(1)
        
            
    # Writing results to file 

    # Header
    header="# List of LCG sites in the GOC Database\n" + \
           "#\n" + \
           "# Generated at " + time.strftime("%Y-%m-%d, %H: %M: %S") + "\n" + \
           "# \n" + \
           "# Fields: \n" + \
           "# siteID || region || sitename || nodetype || nodetype2 || hostname || " + \
           "node is monitored || site status ||  site type || site is monitored || " + \
           "site is in maintainance\n" + \
           "#\n"    
    try:
        of.write(header+ '\n')
    except IOError:
        Log.critical("An IO error occured while writing to file " + outfile + ". Exiting...")
        sys.exit(1)

    try:
        line=None
        format = "%s\t|| %s\t|| %s\t|| %s\t|| None\t|| %s\t||" + \
                 "%s\t|| %s\t|| %s\t|| 1\t|| %s\n"
        for node in nodes:
	    global tuple
            line = format % tuple([convertYN(attr) for attr in node])
            of.write(line)

    except IOError:
        Log.critical("An IO error occured while writing to file " + outfile + ". Exiting...")
        sys.exit(1)
            
    if not line: 
        Log.warning("No results returned from RGMA. Using old file " + outfile)
        return 0
            
    # Closing file
    try: 
        of.close()
    except IOError:
        Log.critical("An IO error occured while closing file " + outfile + ". Exiting...")
        sys.exit(1)

    # Moving the temporary file to the permanent one
    try: 
        os.rename(tmpfile, outfile)    
    except OSError:
        Log.critical("Couldn't move " + tmpfile + " to "  + outfile + ". Exiting...")
        sys.exit(1)
        

    Log.info("New cache file " + outfile + " created")


    return 0

        
#                
# Internal helper  functions    
#
    
def convertYN (s):

    if s == 'Y' or s == 'y':
        return '1'
    elif s == 'N' or s == 'n':
        return '0'
    else:
        return s
        






    
