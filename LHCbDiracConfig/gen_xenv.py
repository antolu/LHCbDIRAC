#!/usr/bin/env python

import os
import logging
import sys
import getopt

optList, args = getopt.getopt( sys.argv[1:], "f:", ["filename"] )

for optKey, optVal in optList:
   if optKey in ( "-f", "filename" ):
       xenv_file = optVal


base = os.path.dirname( xenv_file )
nameManifest = os.path.join( base, 'manifest.xml' )

_lbconf_dir = '.'

'''
Generate the Manifest.xml and xenv file for LbScripts
'''
# Checking the environment
log = logging.getLogger()

# Building the paths for the input and output files
jsonMetadataDir = os.path.join( _lbconf_dir , "cmt" )
mxmlFullname = nameManifest
xenvFullname = xenv_file

# Now import the code to generate the XML files
from LbUtils.LbRunConfigTools import prettify, loadConfig
from LbUtils.LbRunConfigTools import ManifestGenerator, XEnvGenerator
log.info( "Loading projectConfig.json from %s" % jsonMetadataDir )
config = loadConfig( jsonMetadataDir )

# the manifest.xml
log.info( "Generating %s" % mxmlFullname )
mg = ManifestGenerator( config )
manifest = mg.getDocument()
with open( mxmlFullname, "w" ) as f:
    f.write( prettify( manifest ) )

# Now the xenv file
log.info( "Generating %s" % xenvFullname )
xg = XEnvGenerator( config )
xe = xg.getDocument()
with open( xenvFullname, "w" ) as f:
    f.write( prettify( xe ) )

