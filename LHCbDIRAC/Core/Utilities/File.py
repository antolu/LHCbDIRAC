""" File utilities module (e.g. make GUIDs)
"""


from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.File              import makeGuid as DIRACMakeGUID

from LHCbDIRAC.Core.Utilities.ClientTools   import setupProjectEnvironment

def getRootFileGUIDs( fileList ):
  guids = {'Successful':{}, 'Failed':{}}
  for fileName in fileList:
    res = getRootFileGUID( fileName )
    if res['OK']:
      guids['Successful'][fileName] = res['Value']
    else:
      guids['Failed'][fileName] = res['Message']
  return S_OK( guids )

def getRootFileGUID( fileName ):
  """ Function to retrieve a file GUID using Root.
  """
  # Setup the root environment
  import sys, os
  try:
    import ROOT
  except Exception:
    return S_ERROR( "ROOT environment not set up: use 'SetupProject LHCbDirac ROOT'" )

  from ctypes import create_string_buffer
  try:
    ROOT.gErrorIgnoreLevel = 2001
    f = ROOT.TFile.Open( fileName )
    b = f.Get( 'Refs' ).GetBranch( 'Params' )
    text = create_string_buffer( 100 )
    b.SetAddress( text )
    for i in range( b.GetEntries() ):
      b.GetEvent( i )
      x = text.value
      if x.startswith( 'FID=' ):
        guid = x.split( '=' )[1]
        return S_OK( guid )
    return S_ERROR( 'GUID not found' )
  except:
    return S_ERROR( "Error extracting GUID" )
  finally:
    if f:
      f.Close()

def makeGuid( fileNames ):
  """ Function to retrieve a file GUID using Root.
  """
  if type( fileNames ) == str:
    fileNames = [fileNames]

  fileGUIDs = {}
  for fileName in fileNames:
    guid = getRootFileGUID( fileName )
    if guid['OK']:
      gLogger.verbose( 'GUID found to be %s' % guid )
      fileGUIDs[fileName] = guid['Value']
    else:
      gLogger.error( 'Could not obtain GUID from file through Gaudi, using standard DIRAC method' )
      fileGUIDs[fileName] = DIRACMakeGUID( fileName )

  return fileGUIDs
