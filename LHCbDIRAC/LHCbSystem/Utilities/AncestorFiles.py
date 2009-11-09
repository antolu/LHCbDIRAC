########################################################################
# $HeadURL$
# File :   AncestorFiles.py
# Author : Stuart Paterson
########################################################################

"""   This utility simply queries the BK for ancestor files of a specified
      LFN with a given ancestor depth.  
"""

__RCSID__ = "$Id$"

from DIRAC import gConfig, gLogger, S_OK, S_ERROR

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient      import BookkeepingClient

import time, string

#############################################################################
def getAncestorFiles(inputData,ancestorDepth):
  """ Returns S_OK(<list of files>) or S_ERROR(<Message>) after querying the
      Bookkeeping for ancestor files.

      Input data can be an LFN string or a list of LFNs.  Ancestor depth is an integer or
      string that converts to an integer.

      If successful, the original input data LFNs are also returned in the list.
  """
  if not type(inputData) == type([]):
    inputData = [inputData]

  inputData = [ i.replace('LFN:','') for i in inputData]
  bk = BookkeepingClient()

  result = bk.getAncestors(inputData,depth=ancestorDepth)
  gLogger.debug(result)
  if not result['OK']:
    gLogger.warn('Problem during getAncestors call:\n%s' %(result['Message']))
    return result

  data = result['Value']
  if data['Failed']:
    return S_ERROR('No ancestors found for the following files:\n%s' %(string.join(data['Failed'],'\n')))

  returnedInputData = data['Successful'].keys()
  if not inputData.sort() == returnedInputData.sort():
    gLogger.warn('Not all ancestors returned after getAncestors call:\n%s' %result)
    return S_ERROR('Not all ancestors returned after getAncestors call')

  inputDataWithAncestors = returnedInputData
  for input,ancestorList in data['Successful'].items():
    inputDataWithAncestors += ancestorList

  totalFiles = len(inputDataWithAncestors)-len(inputData)
  gLogger.verbose('%s ancestor files retrieved from the bookkeeping for ancestor depth %s' %(totalFiles,ancestorDepth))
  return S_OK(inputDataWithAncestors)

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#