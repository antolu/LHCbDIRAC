#!/usr/bin/env python

import os
import logging
import sys
import getopt

optList, args = getopt.getopt( sys.argv[1:], "c:f:p:d:", ["config", "filename", "python", "dir_base"] )
dir_base = ''

for optKey, optVal in optList:
   if optKey in ( "-c", "config" ):
       cmtconfig = optVal
   if optKey in ( "-f", "filename" ):
       xenv_file = optVal
   if optKey in ( "-p", "python" ):
       python_ver = optVal
   if optKey in ( "-d", "dir_base" ):
       dir_base = optVal

base = os.path.dirname( xenv_file )
nameManifest = os.path.join( base, 'manifest.xml' )

_lbconf_dir = '.'

'''
Generate the Manifest.xml and xenv file for LbScripts
'''
# Checking the environment
log = logging.getLogger()

# Building the paths for the input and output files
jsonMetadataDir = _lbconf_dir
mxmlFullname = nameManifest
xenvFullname = xenv_file

# Now import the code to generate the XML files
from LbUtils.LbRunConfigTools import prettify, loadConfig
from LbUtils.LbRunConfigTools import ManifestGenerator, XEnvGenerator
log.info( "Loading projectConfig.json from %s" % jsonMetadataDir )
config = loadConfig( jsonMetadataDir )
if not config.has_key( 'cmtconfig' ):
  config['cmtconfig'] = cmtconfig

if not config.has_key( 'python_version' ):
  config['python_version'] = python_ver

if not config.has_key( 'dir_base' ):
  config['dir_base'] = dir_base

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

