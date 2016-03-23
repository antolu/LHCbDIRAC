#!/usr/bin/env python

import os
import logging

_lbconf_dir = '/afs/cern.ch/user/j/joel/work/certification/testMakefile/LHCbDirac'
_prj_dir = '/afs/cern.ch/user/j/joel/work/certification/testMakefile/LHCbDirac/InstallArea'

def generateEnvXML():
    '''
    Generate the Manifest.xml and xenv file for LbScripts
    '''
    # Checking the environment
    log = logging.getLogger()

    # Building the paths for the input and output files
    jsonMetadataDir = os.path.join( _lbconf_dir , "cmt" )
    mxmlFullname = os.path.join( _prj_dir, "manifest.xml" )
    xenvFullname = os.path.join( _prj_dir, "LHCbDirac.xenv" )

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

    return 0

generateEnvXML()
