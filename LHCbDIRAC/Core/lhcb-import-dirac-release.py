#!/usr/bin/env python
########################################################################
# File :    lhcb-import-dirac-release
# Author :  Adria Casajus
########################################################################
"""
  Import a DIRAC release from git into SVN
"""
__RCSID__ = "$Id$"

from DIRAC import S_OK, gLogger
from DIRAC.Core.Base      import Script
from DIRAC.Core.Utilities.CFG import CFG
from LHCbDIRAC.Core.Utilities import Distribution

import sys, os, tempfile, shutil

module = "DIRAC"
release = ""
svnUsername = ""
checkNew = True

def setRelease( optionValue ):
  """ set the release version """
  global release
  release = optionValue
  return S_OK()

def setPackage( optionValue ):
  """ set the package name """
  global svnPackages
  svnPackages = optionValue
  return S_OK()

def setUsername( optionValue ):
  """ set th eusername to access SVN"""
  global svnUsername
  svnUsername = optionValue
  return S_OK()

def unsetCheckNew( optionValue ):
  """ unset the value CheckNow """
  global checkNew
  checkNew = False
  return S_OK()

Script.disableCS()

Script.registerSwitch( "r:", "release=", "release to import (mandatory)", setRelease )
Script.registerSwitch( "m:", "module=", "Module to import (default = DIRAC)", setPackage )
Script.registerSwitch( "u:", "username=", "svn username to use", setUsername )
Script.registerSwitch( "k", "nonewcheck", "Don't check if the release has been ported", unsetCheckNew )


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
  buildCFG = packageDistribution.getVersionsCFG()
  localCFG = CFG()
  mergedCFG = CFG()

  if 'Versions' not in buildCFG.listSections():
    gLogger.error( "versions.cfg file in package %s does not contain a Versions top section" % module )

  tmpDir = tempfile.mkdtemp( "DIRACIMPORTR0X" )
  workDir = os.path.join( tmpDir, release )
  if not os.path.isdir( workDir ):
    os.makedirs( workDir )

  packageDistribution.doCheckout( os.path.join( module, 'trunk', module ), workDir )
  gLogger.info( buildCFG['Versions'].listSections() )
  if release in buildCFG['Versions'].listSections():
    gLogger.info( buildCFG['Versions'][ release ].listOptions() )
    print "NONE"
    sys.exit( 0 )
  else:
    localCFG.createNewSection( 'Versions' )
    localCFG['Versions'].createNewSection( release )
    localreleaseCFG = localCFG['Versions']
    localreleaseCFG[release].setOption( 'AccountingSystem', release )
    localreleaseCFG[release].setOption( 'ConfigurationSystem', release )
    localreleaseCFG[release].setOption( 'Core', release )
    localreleaseCFG[release].setOption( 'DataManagementSystem', release )
    localreleaseCFG[release].setOption( 'FrameworkSystem', release )
    localreleaseCFG[release].setOption( 'Interfaces', release )
    localreleaseCFG[release].setOption( 'RequestManagementSystem', release )
    localreleaseCFG[release].setOption( 'ResourceStatusSystem', release )
    localreleaseCFG[release].setOption( 'Resources', release )
    localreleaseCFG[release].setOption( 'StorageManagementSystem', release )
    localreleaseCFG[release].setOption( 'WorkloadManagementSystem', release )
    localreleaseCFG[release].setOption( 'Workflow', release )
    localreleaseCFG[release].setOption( 'TransformationSystem', release )
    localCFG.writeToFile( 'versions-new.cfg' )
    mergedCFG = localCFG.mergeWith( buildCFG )
    mergedCFG.writeToFile( os.path.join( workDir, 'versions.cfg' ) )
    packageDistribution.doCommit( workDir , 'Release ' + release )

  try:
    shutil.rmtree( workDir )
  except Exception, excp:
    gLogger.fatal( "Could not delete %s directory: %s" % ( workDir, excp ) )
    sys.exit( 1 )

  svnComment = "Import for release %s" % release
  svnRelease = "%s/tags/%s/%s" % ( module, module, release )

  exitStatus, data = packageDistribution.doLS( '%s/tags/%s' % ( module, module ) )
  if exitStatus:
    createdReleases = []
  else:
    createdReleases = [ v.strip( "/" ) for v in data.split( "\n" ) if v.find( "/" ) > -1 ]

  gLogger.verbose( "Existing releases for %s:\n\t%s" % ( module, "\n\t".join( createdReleases ) ) )

  if release in createdReleases:
    if checkNew:
      gLogger.fatal( "Release %s is already in svn!" % release )
      sys.exit( 1 )
  else:
    gLogger.notice( "======   Creating %s" % svnRelease )
    if not packageDistribution.doMakeDir( svnRelease, svnComment ):
      gLogger.fatal( "Oops. Can't create %s" % svnRelease )
      sys.exit( 1 )

  tmpDir = tempfile.mkdtemp( "DIRACIMPORTR0X" )
  workDir = os.path.join( tmpDir, module )
  gLogger.info( "======   Working dir will be %s" % workDir )

  if not packageDistribution.doCheckout( svnRelease, workDir ):
    gLogger.fatal( "Oops. Can't create %s" % svnRelease )
    sys.exit( 1 )

  clean = False
  for entry in os.listdir( workDir ):
    if entry != ".svn":
      clean = True
      if os.system( "cd '%s'; svn rm '%s'" % ( workDir, os.path.join( workDir, entry ) ) ):
        gLogger.fatal( "Could not svn clean the directory" )
        sys.exit( 1 )

  if clean:
    if not packageDistribution.doCommit( workDir, svnComment ):
      gLogger.fatal( "Could not commit release" )
      sys.exit( 1 )


  os.rename( os.path.join( workDir, ".svn" ), os.path.join( tmpDir, ".svn" ) )

  gLogger.notice( "======   Retrieving %s" % module )
  if os.system( "git clone git://github.com/DIRACGrid/%s %s" % ( module, workDir ) ):
    gLogger.fatal( "Could not retrieve %s from git" % module )
    sys.exit( 1 )

  gLogger.notice( "======   Checking out release %s" % release )
  if os.system( "cd '%s'; git checkout -b releasetoimport %s" % ( workDir, release ) ):
    gLogger.fatal( "Could not checkout release %s" % release )
    sys.exit( 1 )

  try:
    shutil.rmtree( os.path.join( workDir, '.git' ) )
  except Exception, excp:
    gLogger.fatal( "Could not delete %s/.git directory: %s" % ( workDir, excp ) )
    sys.exit( 1 )

  os.rename( os.path.join( tmpDir, ".svn" ), os.path.join( workDir, ".svn" ) )

  gLogger.notice( "======   Commiting to %s..." % svnRelease )

  if os.system( "cd '%s'; svn add *" % workDir ):
    gLogger.fatal( "Could not svn add all the files" )
    sys.exit( 1 )

  if not packageDistribution.doCommit( workDir, svnComment ):
    gLogger.fatal( "Could not commit release" )
    sys.exit( 1 )

  exitStatus, dataSystem = packageDistribution.doLS( svnRelease )
  if exitStatus:
    createdSystem = []
  else:
    createdSystem = [ v.strip( "/" ) for v in dataSystem.split( "\n" ) if v.find( "/" ) > -1 ]

  gLogger.verbose( "======   Existing system for %s:\n\t%s" % ( svnRelease, "\n\t".join( createdSystem ) ) )

  for diracSystem in createdSystem:
    exitStatus, data = packageDistribution.doLS( '%s/tags/%s/%s' % ( module, module, diracSystem ) )
    if exitStatus:
      createdReleases = []
    else:
      createdReleases = [ v.strip( "/" ) for v in data.split( "\n" ) if v.find( "/" ) > -1 ]

    gLogger.verbose( "======   Existing releases for %s:\n\t%s" % ( module, "\n\t".join( createdReleases ) ) )

    if release in createdReleases:
      if checkNew:
        gLogger.fatal( "Release %s is already in svn!" % release )
        sys.exit( 1 )
    else:
      systemRelease = os.path.join( module, 'tags', module, diracSystem, release )
      trunkSystem = os.path.join( module, 'trunk', module, diracSystem, 'cmt' )
      gLogger.notice( "======   Creating %s" % systemRelease )
      packageDistribution.queueCopy( os.path.join( svnRelease, diracSystem ), systemRelease, svnComment )
      gLogger.notice( "======   Creating %s" % trunkSystem )
      packageDistribution.queueCopy( trunkSystem, systemRelease, svnComment )

    gLogger.notice( "======   Commiting to %s..." % svnRelease )
    if not packageDistribution.executeCommandQueue():
      gLogger.fatal( "Could not commit release" )
      sys.exit( 1 )

  try:
    shutil.rmtree( tmpDir )
  except Exception, excp:
    gLogger.fatal( "Could not delete %s directory: %s" % ( tmpDir, excp ) )

  sys.exit( 0 )
