#!/usr/bin/env python
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/Dirac/trunk/DiracPolicy/scripts/dirac-deploy-scripts.py $
"""
Deploy all scripts and extensions
"""
__RCSID__ = "$Id$"

import os
import shutil
import stat
import re

moduleSuffix = "DIRAC"
defaultPerms = stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
excludeMask = [ '__init__.py' ]
simpleCopyMask = [ os.path.basename( __file__ ), 'dirac-compile-externals.py', 'dirac-install.py' ]


def lookForScriptsInPath( basePath, rootModule ):
  isScriptsDir = os.path.split( rootModule )[1] == "scripts"
  scriptFiles = []
  for entry in os.listdir( basePath ):
    absEntry = os.path.join( basePath, entry )
    if os.path.isdir( absEntry ):
      scriptFiles.extend( lookForScriptsInPath( absEntry, os.path.join( rootModule, entry ) ) )
    elif isScriptsDir and os.path.isfile( absEntry ):
      scriptFiles.append( ( os.path.join( rootModule, entry ), entry ) )
  return scriptFiles

def findDIRACRoot( path ):
  dirContents = os.listdir( path )
  print path
  print dirContents
  if 'DIRAC' in dirContents and os.path.isdir( os.path.join( path, 'DIRAC' ) ):
    return path
  parentPath = os.path.dirname( path )
  if parentPath == path or len( parentPath ) == 1:
    return False
  return findDIRACRoot( os.path.dirname( path ) )

if __name__ == "__main__":
  import sys, getopt
  opts, args = getopt.gnu_getopt( sys.argv[1:], \
                            "hd:vi:", \
                            ["help", "directory=", "verbose", "install-area="] )

  diracPath = findDIRACRoot( os.path.dirname( os.path.realpath( __file__ ) ) )
  targetScriptsPath = os.path.join( diracPath, "scripts" )
  verbose = False
  for o, a in opts:
    if o in ( "-h", "--help" ):
      print "dirac-deploy-scripts [options]"
      print "\t-h\t--help\tThis display"
      print "\t-d\t--directory=<dir>\tStarts looking for scripts in <dir>"
      print "\t-i\t--install-area=<dir>\tCreates the script in <dir> (default = <top>/scripts)"
      sys.exit( 0 )
    if o in ( "-d", "--directory" ):
      diracPath = a
    if o in ( "-i", "--install-area" ):
      targetScriptsPath = a
    if o in ( "-v", "--verbose" ):
      verbose = True

  if not diracPath:
    print "Error: Cannot find DIRAC root!"
    sys.exit( 1 )
  if diracPath:
    rootModule = os.path.realpath( diracPath )
    modulePath = rootModule
  else:
    rootPath = diracPath
    rootModule = None
    for rootModule in os.listdir( rootPath ):
      modulePath = os.path.join( rootPath, rootModule )
      if not os.path.isdir( modulePath ):
        continue
      if rootModule == moduleSuffix:
        break
    if not rootModule:
      print "Could not find a root directory..."

  if verbose:
    print "Scripts from %s will be deployed at %s" % ( diracPath, targetScriptsPath )

  if not os.path.isdir( targetScriptsPath ):
    os.mkdir( targetScriptsPath )

  pythonScriptRE = re.compile( "(.*/)*([a-z]+-[a-zA-Z0-9-]+).py" )
  if True:
    if verbose: print "Inspecting %s module" % rootModule
    scripts = lookForScriptsInPath( modulePath, rootModule )
    for script in scripts:
      scriptPath = script[0]
      scriptName = script[1]
      if scriptName in excludeMask:
        continue
      scriptLen = len( scriptName )
      if scriptName not in simpleCopyMask and pythonScriptRE.match( scriptName ):
        fakeScriptPath = os.path.join( targetScriptsPath, scriptName[:-3] )
        if not os.path.exists( fakeScriptPath ):
          os.symlink( os.path.join( diracPath, 'scripts', scriptName ), fakeScriptPath )
          os.chmod( fakeScriptPath, defaultPerms )
      else:
        copyPath = os.path.join( targetScriptsPath, scriptName )
        if not os.path.exists( copyPath ):
          os.symlink( os.path.join( diracPath, scriptPath ), copyPath )
          os.chmod( copyPath, defaultPerms )
          cLen = len( copyPath )
          reFound = pythonScriptRE.match( copyPath )
          if reFound:
            destPath = "".join( list( reFound.groups() ) )
            os.rename( copyPath, destPath )
