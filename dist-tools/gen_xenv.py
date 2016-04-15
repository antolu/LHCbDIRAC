#!/usr/bin/env python

import os
import logging
import sys

from optparse import OptionParser

parser = OptionParser()
parser.add_option( '-c', '--cmtconfig', action = 'store',
                  help = 'help message' )
parser.add_option( '-f', '--xenv-file', action = 'store',
                  help = 'help message' )
parser.add_option( '-m', '--manifest-file', action = 'store',
                  help = 'help message' )
parser.add_option( '-p', '--python-version', action = 'store',
                  help = 'help message' )
parser.add_option( '-d', '--dir-base', action = 'store',
                  help = 'help message' )

options, args = parser.parse_args()

_lbconf_dir = 'dist-tools'

'''
Generate the Manifest.xml and xenv file for LbScripts
'''
# Checking the environment
log = logging.getLogger()

# Building the paths for the input and output files
jsonMetadataDir = _lbconf_dir

# Now import the code to generate the XML files
from LbUtils.LbRunConfigTools import prettify, loadConfig
from LbUtils.LbRunConfigTools import ManifestGenerator, XEnvGenerator
log.info( "Loading projectConfig.json from %s" % jsonMetadataDir )
config = loadConfig( jsonMetadataDir )

for opt in ( 'cmtconfig', 'python_version', 'dir_base' ):
  if opt not in config:
    if getattr( options, opt ):
      config[opt] = getattr( options, opt )
    else:
      parser.error( 'option %s not specified', opt )

# the manifest.xml
if options.manifest_file:
  log.info( "Generating %s" % options.manifest_file )
  mg = ManifestGenerator( config )
  manifest = mg.getDocument()
  with open( options.manifest_file, "w" ) as f:
      f.write( prettify( manifest ) )


# Now the xenv file
if options.xenv_file:
  log.info( "Generating %s" % options.xenv_file )
  xg = XEnvGenerator( config )
  xe = xg.getDocument()
  with open( options.xenv_file, "w" ) as f:
      f.write( prettify( xe ) )

