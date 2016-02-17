########################################################################
# File :   AncestorFiles.py
# Author : Stuart Paterson
########################################################################

"""   This utility simply queries the BK for ancestor files of a specified
      LFN with a given ancestor depth.

      N.B. this made more sense during the transitional period between old
      and new BK but now could most likely be refactored into the BK client.
"""

__RCSID__ = "$Id$"

from DIRAC import gLogger, S_OK, S_ERROR

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient      import BookkeepingClient

import string

#############################################################################
def getFileAncestors( inputData, ancestorDepth ):
  """ Returns S_OK({inputFile1:[ancestor1,],}) or S_ERROR(<Message>) after querying the
      Bookkeeping for ancestor files.

      Input data can be an LFN string or a list of LFNs.  Ancestor depth is an integer or
      string that converts to an integer.
  """
  if not type( inputData ) == type( [] ):
    inputData = [inputData]

  inputData = [ i.replace( 'LFN:', '' ) for i in inputData]
  bk = BookkeepingClient()

  result = bk.getFileAncestors( inputData, depth = ancestorDepth, replica=True )
  gLogger.debug( result )
  if not result['OK']:
    gLogger.warn( 'Problem during getAncestors call:\n%s' % ( result['Message'] ) )
    return result

  data = result['Value']
  if data['Failed']:
    return S_ERROR( 'No ancestors found for the following files:\n%s' % ( string.join( data['Failed'], '\n' ) ) )

  returnedInputData = data['Successful'].keys()
  if not inputData.sort() == returnedInputData.sort():
    gLogger.warn( 'Not all ancestors returned after getAncestors call:\n%s' % result )
    return S_ERROR( 'Not all ancestors returned after getAncestors call' )
  return S_OK( data['Successful'] )

def getAncestorFiles( inputData, ancestorDepth ):
  """ Returns S_OK(<list of files>) or S_ERROR(<Message>) after querying the
      Bookkeeping for ancestor files.

      Input data can be an LFN string or a list of LFNs.  Ancestor depth is an integer or
      string that converts to an integer.

      If successful, the original input data LFNs are also returned in the list.
  """
  res = getFileAncestors( inputData, ancestorDepth )
  if not res['OK']:
    return res
  inputDataWithAncestors = res['Value'].keys()
  for ancestorList in res['Value']:
    inputDataWithAncestors += res['Value'][ancestorList]
  totalFiles = len( inputDataWithAncestors ) - len( inputData )
  gLogger.verbose( '%s ancestor files retrieved from the bookkeeping \
  for ancestor depth %s' % ( totalFiles, ancestorDepth ) )
  return S_OK( inputDataWithAncestors )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
