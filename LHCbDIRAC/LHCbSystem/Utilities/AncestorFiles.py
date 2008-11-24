########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Utilities/AncestorFiles.py,v 1.3 2008/11/24 17:40:03 paterson Exp $
# File :   AncestorFiles.py
# Author : Stuart Paterson
########################################################################

"""   This utility simply queries the BK for ancestor files of a specified
      LFN with a given ancestor depth.  Initially written with the old BK
      interface this will eventually be updated to use the DIRAC3 BK Client.
"""

__RCSID__ = "$Id: AncestorFiles.py,v 1.3 2008/11/24 17:40:03 paterson Exp $"

from DIRAC import gConfig, gLogger, S_OK, S_ERROR

try:
  from DIRAC.BookkeepingSystem.Client.genCatalogOld          import getAncestors
except Exception,x:
  gLogger.warn('Could not import old BK genCatalog utility.')

from DIRAC.BookkeepingSystem.Client.BookkeepingClient      import BookkeepingClient

import time, string

#############################################################################
def getAncestorFilesOld(inputData,ancestorDepth):
  """ Returns S_OK(<list of files>) or S_ERROR(<Message>) after querying the
      Bookkeeping for ancestor files.

      Input data can be an LFN string or a list of LFNs.  Ancestor depth is an integer or
      string that converts to an integer.

      If successful, the original input data LFNs are also returned in the list.
  """
  if not type(inputData) == type([]):
    inputData = [inputData]

  inputData = [ i.replace('LFN:','') for i in inputData]

  start = time.time()
  try:
    result = getAncestors(inputData,int(ancestorDepth))
  except Exception,x:
    gLogger.warn('getAncestors failed with exception:\n%s' %x)
    return S_ERROR('getAncestors failed with exception:\n%s' %x)

  gLogger.info('genCatalog.getAncestors lookup time %.2f s' %(time.time()-start))
  gLogger.debug(result)
  if not result:
    gLogger.warn('Null result from genCatalog utility')
    return S_ERROR(result)
  if not type(result)==type({}):
    gLogger.warn('Non-dict object returned from genCatalog utility')
    return S_ERROR(result)
  if not result.has_key('PFNs'):
    gLogger.warn('----------BK-Result------------')
    gLogger.warn(result)
    gLogger.warn('--------End-BK-Result----------')
    gLogger.warn('Missing key PFNs from genCatalog utility')
    return S_ERROR(result)

  newInputData = result['PFNs']

  missingFiles = []
  for i in inputData:
    if not i in newInputData:
      missingFiles.append(i)

  #If no missing files and ancestor depth is 1 can return
  if ancestorDepth==1 and not missingFiles:
    return S_OK(newInputData)

  if missingFiles:
    message = '%s input data files missing from genCatalog utility result:\n%s' %(len(missingFiles),string.join(missingFiles,',\n'))
    gLogger.warn(message)
    return S_ERROR(message)

  ancestorFiles = []
  for i in newInputData:
    if not i in inputData:
      ancestorFiles.append(i)

  gLogger.verbose('%s ancestor files retrieved from genCatalog utility' %(len(ancestorFiles)))
  if not ancestorFiles:
    gLogger.warn('No ancestor files returned from genCatalog')
    return S_ERROR('No Ancestor Files Found')

  return S_OK(newInputData)

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