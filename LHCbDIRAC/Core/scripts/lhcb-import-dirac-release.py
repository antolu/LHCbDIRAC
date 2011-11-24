#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/Core/scripts/lhcb-create-svn-tag.py $
# File :    lhcb-import-dirac-release
# Author :  Adria Casajus
########################################################################
"""
  Import a DIRAC release from git into SVN
"""
__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base      import Script
from DIRAC.Core.Utilities import List
from LHCbDIRAC.Core.Utilities import Distribution

import sys, os, tempfile, shutil, getpass

module = "DIRAC"
release = ""
svnUsername = ""

def setRelease( optionValue ):
  global release
  release = optionValue
  return S_OK()

def setPackage( optionValue ):
  global svnPackages
  svnPackages = optionValue
  return S_OK()

def setUsername( optionValue ):
  global svnUsername
  svnUsername = optionValue
  return S_OK()

Script.disableCS()

Script.registerSwitch( "r:", "release=", "release to import (mandatory)", setRelease )
Script.registerSwitch( "m:", "module=", "Module to import (default = DIRAC)", setPackage )
Script.registerSwitch( "u:", "username=", "svn username to use", setUsername )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName ] ) )

Script.parseCommandLine( ignoreErrors = False )

gLogger.notice( 'Executing: %s ' % ( ' '.join( sys.argv ) ) )

if __name__ == "__main__":
  if not release:
    gLogger.fatal( "No release specified" )
    sys.exit( 1 )
  packageDistribution = Distribution.Distribution( module )
  packageDistribution.setSVNUser( svnUsername )

  svnComment = "Import for release %s" % release
  svnRelease = "%s/tags/%s/%s" % ( module, module, release )

  exitStatus, data = packageDistribution.doLS( '%s/tags/%s' % ( module, module ) )
  if exitStatus:
    createdReleases = []
  else:
    createdReleases = [ v.strip( "/" ) for v in data.split( "\n" ) if v.find( "/" ) > -1 ]

  gLogger.verbose( "Existing releases for %s:\n\t%s" % ( module, "\n\t".join( createdReleases ) ) )

  if release in createdReleases:
    gLogger.fatal( "Release %s is already in svn!" % release )
    sys.exit( 1 )

  gLogger.notice( "Creating %s" % svnRelease )
  if not packageDistribution.doMakeDir( svnRelease, svnComment ):
    gLogger.fatal( "Oops. Can't create %s" % svnRelease )
    sys.exit( 1 )

  tmpDir = tempfile.mkdtemp( "DIRACIMPORTR0X" )
  workDir = os.path.join( tmpDir, module )
  gLogger.info( "Working dir will be %s" % workDir )

  if not packageDistribution.doCheckout( svnRelease, workDir ):
    gLogger.fatal( "Oops. Can't create %s" % svnRelease )
    sys.exit( 1 )

  os.rename( os.path.join( workDir, ".svn" ), os.path.join( tmpDir, ".svn" ) )

  gLogger.notice( "Retrieving %s" % module )
  if os.system( "git clone https://github.com/DIRACGrid/%s %s" % ( module, workDir ) ):
    gLogger.fatal( "Could not retrieve %s from git" % module )
    sys.exit( 1 )

  gLogger.notice( "Checking out release %s" % release )
  if os.system( "cd '%s'; git checkout -b releasetoimport %s" % ( workDir, release ) ):
    gLogger.fatal( "Could not checkout release %s" % release )
    sys.exit( 1 )

  try:
    shutil.rmtree( os.path.join( workDir, '.git' ) )
  except Exception, excp:
    gLogger.fatal( "Could not delete %s/.git directory: %s" % ( workDir, excp ) )
    sys.exit( 1 )

  os.rename( os.path.join( tmpDir, ".svn" ), os.path.join( workDir, ".svn" ) )

  gLogger.notice( "Commiting to %s..." % svnRelease )

  if os.system( "cd '%s'; svn add *" % workDir ):
    gLogger.fatal( "Could not svn add all the files" )
    sys.exit( 1 )

  if not packageDistribution.doCommit( workDir, svnComment ):
    gLogger.fatal( "Could not commit release" )
    sys.exit( 1 )

  sys.exit( 0 )
