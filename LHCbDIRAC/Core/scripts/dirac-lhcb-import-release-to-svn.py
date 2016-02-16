#!/usr/bin/env python
########################################################################
# Author :  Adria Casajus
########################################################################
"""
  Import DIRAC releases from git into SVN
"""
__RCSID__ = "$Id: dirac-lhcb-import-release-to-svn.py 58933 2012-11-27 11:39:12Z ubeda $"

import sys, tempfile, os, imp, shutil

from DIRAC import S_OK, S_ERROR, gLogger, rootPath
from DIRAC.Core.Base import Script
from LHCbDIRAC.Core.Utilities import Distribution

repoName = 'DIRAC'
version = ""
userName = ""
overwrite = False

gRepos = { 'DIRAC' : 'https://github.com/DIRACGrid/DIRAC.git',
           'Web'   : 'https://github.com/DIRACGrid/DIRACWeb.git' }

def setRepoName( opVal ):
  """ setthe repository name """
  global repoName
  if opVal not in userName:
    return S_ERROR( "Only %s are valid" " or ".join( gRepos.keys() ) )
  repoName = opVal
  return S_OK()

def setVersion( opVal ):
  """ set the version of the package """
  global version
  version = opVal
  return S_OK()

def setUser( opVal ):
  """ set the username for SVN """
  global userName
  userName = opVal
  return S_OK()

def setOverwrite( opVal ):
  """ set if you need to overwrite existing version """
  global overwrite
  overwrite = True
  return S_OK()

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... ' % Script.scriptName,
                                     'Mandatory to specify the version' ] ) )

Script.registerSwitch( 'n:', 'name=', 'Name of the repo to import (by default DIRAC)', setRepoName )
Script.registerSwitch( 'v:', 'version=', 'Version to import to SVN', setVersion )
Script.registerSwitch( 'u:', 'username=', 'Username to commit to SVN', setUser )
Script.registerSwitch( 't', 'overwrite', 'Import even if it has already been imported', setOverwrite )

Script.disableCS()
Script.addDefaultOptionValue( "/DIRAC/Setup", "Dummy" )
Script.parseCommandLine( ignoreErrors = False )

if not version:
  gLogger.fatal( "Missing version to import" )
  sys.exit( 1 )

deleteTag = False

distMaker = Distribution.Distribution( repoName )
if userName:
  distMaker.setSVNUser( userName )
#distMaker.doMakeDir( "%s/tags/%s" % ( repoName, repoName ), "Creation of the base directory for tags" )
result = distMaker.doLS( "%s/tags/%s" % ( repoName, repoName ) )
if result[0]:
  gLogger.error( "Could not retrieve the list of tags. Maybe the base directory does not exist?" )
  sys.exit( 1 )
tags = result[1].split( "\n" )
if "%s/" % version in tags:
  deleteTag = True
  gLogger.notice( "Version is already imported" )
  if not overwrite:
    gLogger.always( "Aborting" )
    sys.exit( 1 )

gLogger.notice( "Creating temporary work dir" )
workDir = tempfile.mkdtemp( ".git2svn.%s" % repoName )
gitRepoDir = os.path.join( workDir, repoName )
gLogger.notice( "Workdir will be %s" % workDir )

gLogger.notice( "Retrieving from Git.." )

cmd = "git clone '%s' '%s'" % ( gRepos[ repoName ], gitRepoDir )
print cmd
os.system( cmd )

isTag = not os.system( "(cd '%s'; git tag -l | grep -q '%s')" % ( gitRepoDir, version ) )
if isTag:
  gLogger.notice( "Switching to tag %s" % ( version ) )
  os.system( "(cd '%s'; git checkout -b SVNIMPORT-%d '%s')" % ( gitRepoDir, os.getpid(), version ) )
else:
  gLogger.notice( "Switching to branch %s" % ( version ) )
  os.system( "(cd '%s'; git checkout -b SVNIMPORT-%d 'origin/%s')" % ( gitRepoDir, os.getpid(), version ) )

gLogger.notice( "Replacing keywords..." )
distTarModFile = os.path.join( rootPath, "DIRAC" , "Core", "scripts", "dirac-create-distribution-tarball.py" )
fd = open( distTarModFile )
DistCreateTarball = imp.load_module( 'createDistTarball', fd, distTarModFile, ( "", "r", imp.PY_SOURCE ) )
#This none is a horrible hack, needs fix
tarCreator = DistCreateTarball.TarModuleCreator.replaceKeywordsWithGit( gitRepoDir )

gLogger.notice( "Stripping the .git directory" )
shutil.rmtree( "%s/.git" % gitRepoDir )
Distribution.writeVersionToInit( gitRepoDir, version )

gLogger.notice( "Ready to send to SVN.." )
ciMsg = "Import version %s from Git" % version
tagPath = "%s/tags/%s/%s" % ( repoName, repoName, version )
if deleteTag:
  gLogger.notice( "Deleting previous tag in svn" )
  result = distMaker.doRM( tagPath, ciMsg )
  if result[0]:
    gLogger.error( "Could not delete previous tag" )
    sys.exit( 1 )

def createSVNTag( repoName, subDir, tagSVNPath, sourceDir ):
  """ create the TAG for SVN """
  result = distMaker.doLS( tagSVNPath )
  if result[0] == 0:
    if distMaker.doRM( tagSVNPath, ciMsg )[0]:
      gLogger.error( "Could not delete previous tag" )
      sys.exit( 1 )
  gLogger.notice( "Creating tag %s..." % tagSVNPath )
  if not distMaker.doMakeDir( tagSVNPath, ciMsg ):
    gLogger.error( "Could not generate tag" )
    sys.exit( 1 )
  tagDir = os.path.join( workDir, "%s.%s.tag" % ( repoName, os.getpid() ) )
  if not distMaker.doCheckout( tagSVNPath, tagDir ):
    gLogger.error( "Could not generate tag" )
    sys.exit( 1 )
  for objName in os.listdir( sourceDir ):
    objPath = os.path.join( sourceDir, objName )
    if os.path.isdir( objPath ):
      shutil.copytree( objPath, os.path.join( tagDir, objName ) )
    else:
      shutil.copy( objPath, tagDir )
    os.system( "( cd '%s'; svn add '%s' > /dev/null )" % ( tagDir, objName ) )
  cmtDir = os.path.join( tagDir, "cmt" )
  if not os.path.isdir( cmtDir ):
    os.makedirs( cmtDir )
    os.system( "( cd '%s'; svn add '%s' > /dev/null )" % ( tagDir, cmtDir ) )
  reqsFile = os.path.join( cmtDir, "requirements" )
  try:
    fd = open( reqsFile, "w" )
    if subDir:
      fd.write( "package %s\n\nuse DiracPolicy *\n" % subDir )
    else:
      fd.write( "package %s\n" % repoName )
    fd.close()
    os.system( "( cd '%s'; svn add '%s' > /dev/null )" % ( tagDir, reqsFile ) )
  except Exception, excp:
    gLogger.error( "Could not create cmt requirements file: %s" % str( excp ) )

  if os.system( "( cd '%s'; svn commit -m '%s' > /dev/null)" % ( tagDir, ciMsg ) ):
    gLogger.fatal( "Could not generate tag" )
    sys.exit( 1 )
  shutil.rmtree( tagDir )

createSVNTag( repoName, "", "%s/tags/%s/%s" % ( repoName, repoName, version ), gitRepoDir )

if repoName != "Web":
  gLogger.notice( "Creating system tags" )
  for systemName in os.listdir( gitRepoDir ):
    gitSystemPath = os.path.join( gitRepoDir, systemName )
    if not os.path.isdir( gitSystemPath ):
      continue
    gLogger.notice( "Creating system tag for %s" % systemName )
    createSVNTag( repoName, systemName,
                  "%s/tags/%s/%s/%s" % ( repoName, repoName, systemName, version ),
                  gitSystemPath )

gLogger.always( "Done!" )
