###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
""" File utilities module (e.g. make GUIDs)
"""

__RCSID__ = "$Id$"


import shlex

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.Core.Utilities.File import makeGuid as DIRACMakeGUID


def getRootFileGUIDs(fileList):
  """ Retrieve a list of GUIDs for a list of files
  """
  guids = {'Successful': {}, 'Failed': {}}
  for fileName in fileList:
    res = getRootFileGUID(fileName)
    if res['OK']:
      gLogger.verbose('GUID from ROOT', '%s' % res['Value'])
      guids['Successful'][fileName] = res['Value']
    else:
      guids['Failed'][fileName] = res['Message']
  return S_OK(guids)


def getRootFileGUID(fileName):
  """ Function to retrieve a file GUID using Root.
  """
  res = systemCall(timeout=0, cmdSeq=shlex.split("getROOTFileGUID.py %s" % fileName))
  if not res['OK']:
    return res
  if res['Value'][0]:
    return S_ERROR(res['Value'][2])
  return S_OK(res['Value'][1])


def makeGuid(fileNames):
  """ Function to retrieve a file GUID using Root.
  """
  if isinstance(fileNames, basestring):
    fileNames = [fileNames]

  fileGUIDs = {}
  for fileName in fileNames:
    res = getRootFileGUID(fileName)
    if res['OK']:
      gLogger.verbose('GUID from ROOT', '%s' % res['Value'])
      fileGUIDs[fileName] = res['Value']
    else:
      gLogger.error('Could not obtain GUID from file through Gaudi, using standard DIRAC method')
      fileGUIDs[fileName] = DIRACMakeGUID(fileName)

  return fileGUIDs
