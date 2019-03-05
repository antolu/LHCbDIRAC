""" File utilities module (e.g. make GUIDs)
"""

__RCSID__ = "$Id$"


import shlex

from DIRAC import gLogger, S_OK
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.Core.Utilities.File import makeGuid as DIRACMakeGUID


def getRootFileGUIDs(fileList):
  """ Retrieve a list of GUIDs for a list of files
  """
  guids = {'Successful': {}, 'Failed': {}}
  for fileName in fileList:

    res = systemCall(timeout=0, cmdSeq=shlex.split("getRootFileGUID.py %s" % fileName))
    if not res['OK']:
      guids['Failed'][fileName] = res['Message']
    else:
      if res['Value'][0]:
        guids['Failed'][fileName] = res['Value'][2]
      else:
        guids['Successful'][fileName] = res['Value'][1]
  return S_OK(guids)


def makeGuid(fileNames):
  """ Function to retrieve a file GUID using Root.
  """
  if isinstance(fileNames, basestring):
    fileNames = [fileNames]

  fileGUIDs = {}
  for fileName in fileNames:

    res = systemCall(timeout=0, cmdSeq=shlex.split("getRootFileGUID.py %s" % fileName))

    if not res['OK']:
      gLogger.error('Could not obtain GUID from file through Gaudi, using standard DIRAC method')
      fileGUIDs[fileName] = DIRACMakeGUID(fileName)
    else:
      if res['Value'][0]:
        gLogger.error('Could not obtain GUID from file through Gaudi, using standard DIRAC method')
        fileGUIDs[fileName] = DIRACMakeGUID(fileName)
      else:
        gLogger.verbose('GUID found to be %s' % res['Value'][1])
        fileGUIDs[fileName] = res['Value'][1]

  return fileGUIDs
