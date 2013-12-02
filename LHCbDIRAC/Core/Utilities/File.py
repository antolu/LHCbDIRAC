""" File utilities module (e.g. make GUIDs)
"""
__RCSID__ = "$Id: File.py 42387 2011-09-07 13:53:37Z phicharp $"

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.File              import makeGuid as DIRACMakeGUID

def getRootFileGUIDs( fileList ):
  """ Retrieve a list of GUIDs for a list of files
  """
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
  try:
    import ROOT
  except ImportError:
    return S_ERROR( "ROOT environment not set up: use 'SetupProject LHCbDirac ROOT'" )

  from ctypes import create_string_buffer
  try:
    ROOT.gErrorIgnoreLevel = 2001
    fr = ROOT.TFile.Open( fileName )
    branch = fr.Get( 'Refs' ).GetBranch( 'Params' )
    text = create_string_buffer( 100 )
    branch.SetAddress( text )
    for i in range( branch.GetEntries() ):
      branch.GetEvent( i )
      x = text.value
      if x.startswith( 'FID=' ):
        guid = x.split( '=' )[1]
        return S_OK( guid )
    return S_ERROR( 'GUID not found' )
  except:
    return S_ERROR( "Error extracting GUID" )
  finally:
    if fr:
      fr.Close()

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
