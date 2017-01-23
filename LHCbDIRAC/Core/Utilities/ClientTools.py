"""  The ClientTools module provides additional functions for use by users
     of the DIRAC client in the LHCb environment.
"""

import os
import tempfile

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.List import breakListIntoChunks

from LHCbDIRAC.Core.Utilities.RunApplication import RunApplication

__RCSID__ = "$Id$"

def _errorReport( error, message = None ):
  """ Internal function to return errors and exit with an S_ERROR()
  """
  if not message:
    message = error

  gLogger.warn( error )
  return S_ERROR( message )

def mergeRootFiles( outputFile, inputFiles, cleanUp = True ):
  """ Merge several ROOT files

  Args:
      outputFile (str): output file name
      inputFiles (list): list of input files
      cleanUp (bool): remove input files after merge, or not
  """
  if not isinstance( inputFiles, list ):
    return _errorReport( "please provide a list of input files" )

  # Performs the merging
  chunkSize = 20
  if len( inputFiles ) > chunkSize:
    lists = breakListIntoChunks( inputFiles, chunkSize )
    tempFiles = []
    for filelist in lists:
      tempOutputFile = tempfile.mktemp()
      tempFiles.append( tempOutputFile )
      res = _mergeRootFiles( tempOutputFile, filelist )
      if not res['OK']:
        return _errorReport( res['Message'], "Failed to perform ROOT merger" )
    res = _mergeRootFiles( outputFile, tempFiles )
    if not res['OK']:
      return _errorReport( res['Message'], "Failed to perform final ROOT merger" )

    if cleanUp:
      for filename in tempFiles:
        if os.path.exists( filename ):
          os.remove( filename )
  else:
    res = _mergeRootFiles( outputFile, inputFiles )
    if not res['OK']:
      return _errorReport( res['Message'], "Failed to perform ROOT merger" )
  return S_OK( outputFile )

#############################################################################
def _mergeRootFiles( outputFile, inputFiles ):
  """ Merge ROOT files """
  cmd = "hadd -f %s " % outputFile + ' '.join( inputFiles )

  ra = RunApplication()
  ra.applicationName = 'ROOT'
  ra.command = cmd

  try:
    ra.run()
    return S_OK()
  except RuntimeError:
    return _errorReport( "Failed to merge root files" )
