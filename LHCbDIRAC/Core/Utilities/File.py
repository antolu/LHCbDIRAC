""" File utilities module (e.g. make GUIDs)
"""

import os

from DIRAC import gLogger
from DIRAC.Core.Utilities.Subprocess        import systemCall
from DIRAC.Core.Utilities.File              import makeGuid as DIRACMakeGUID

from LHCbDIRAC.Core.Utilities.ClientTools   import setupProjectEnvironment


def makeGuid( fileNames, cleanUp = True ):
  """ Function to retrieve a file GUID using Root.
  """
  if type( fileNames ) == str:
    fileNames = [fileNames]

  # Setup the root enviroment
  res = setupProjectEnvironment( 'DaVinci' )
  if not res['OK']:
    gLogger.error( res['Message'] + ": Failed to setup the ROOT environment" )
    return res
  rootEnv = res['Value']


  fileGUIDs = {}
  for fileName in fileNames:

    # Write the script to be executed
    fopen = open( 'tmpRootScript.py', 'w' )
    fopen.write( 'from ROOT import TFile\n' )
    fopen.write( "l=TFile.Open('%s')\n" % fileName )
    fopen.write( "if not( not( l.Get('##Params') ) ): #if the structure is '##Params'\n" )
    fopen.write( "  t=l.Get(\'##Params\')\n" )
    fopen.write( '  t.Show(0)\n' )
    fopen.write( '  leaves=t.GetListOfLeaves()\n' )
    fopen.write( '  leaf=leaves.UncheckedAt(0)\n' )
    fopen.write( '  val=leaf.GetValueString()\n' )
    fopen.write( "  fid=val.split('=')[2].split(']')[0]\n" )
    fopen.write( "  print 'GUID%sGUID' %fid\n" )
    fopen.write( "elif  not( not( l.Get('Refs') ) ):#if the structure is 'Refs/Params'\n" )
    fopen.write( "  t_ref = l.Get('Refs')\n" )
    fopen.write( "  b_param = t_ref.GetBranch('Params')\n" )
    fopen.write( "  for i in range(b_param.GetEntries()):\n" )
    fopen.write( "    b_param.GetEntry(i)\n" )
    fopen.write( "    leaves=b_param.GetListOfLeaves()\n" )
    fopen.write( "    leaf=leaves.UncheckedAt(0)\n" )
    fopen.write( "    val=leaf.GetValueString()\n" )
    fopen.write( "    if 'FID' in val:\n" )
    fopen.write( "      fid=val.split('=')[1]\n" )
    fopen.write( "      print 'GUID%sGUID' %fid\n" )
    fopen.write( "      break\n" )
    fopen.write( "else:\n" )
    fopen.write( "  # the structure is not recognised print empty string\n" )
    fopen.write( "  # that will raise an exception later.\n" )
    fopen.write( "  print ''\n" )
    fopen.write( 'l.Close()\n' )
    fopen.close()
    # Execute the root script
    cmd = ['python']
    cmd.append( 'tmpRootScript.py' )
    gLogger.debug( cmd )
    ret = systemCall( 1800, cmd, env = rootEnv )
    if not ret['OK']:
      gLogger.error( 'Problem using root\n%s' % ret )
      gLogger.error( 'Could not obtain GUID from file through Gaudi, using standard DIRAC method' )
      fileGUIDs[fileName] = DIRACMakeGUID( fileName )
    if cleanUp:
      os.remove( 'tmpRootScript.py' )
    stdout = ret['Value'][1]
    try:
      guid = stdout.split( 'GUID' )[1]
      gLogger.verbose( 'GUID found to be %s' % guid )
      fileGUIDs[fileName] = guid
    except Exception:
      gLogger.error( 'Could not obtain GUID from file through Gaudi, using standard DIRAC method' )
      fileGUIDs[fileName] = DIRACMakeGUID( fileName )

  return fileGUIDs
