#!/usr/bin/env python
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
########################################################################
# File :    dirac-bookkeeping-job-info
# Author :  Zoltan Mathe
########################################################################
"""
  It returns the job meta data for a given list of LFNs
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, printDMResult
import DIRAC
from DIRAC import gLogger

if __name__ == "__main__":

  bkScript = DMScript()
  bkScript.registerFileSwitches()
  Script.registerSwitch('', 'Summary', '   Only report job IDs')
  Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ... [LFN|File]' % Script.scriptName,
                                    'Arguments:',
                                    '  LFN:      Logical File Name',
                                    '  File:     Name of the file with a list of LFNs']))

  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()
  for lfn in args:
    bkScript.setLFNsFromFile(lfn)
  lfnList = bkScript.getOption('LFNs', [])
  if not lfnList:
    Script.showHelp()
    DIRAC.exit(0)
  summary = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Summary':
      summary = True

  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
  retVal = BookkeepingClient().bulkJobInfo(lfnList)
  if retVal['OK']:
    success = retVal['Value']['Successful']
    group = {}
    jobs = {}
    for lfn in success:
      if isinstance(success[lfn], type([])) and len(success[lfn]) == 1:
        success[lfn] = success[lfn][0]
      job = success[lfn]
      jobID = 'Job %d' % job['DIRACJobId']
      group.setdefault(jobID, []).append(lfn)
      jobs.setdefault(jobID, job)
    if not summary:
      for jobID in group:
        lfns = ','.join(group[jobID])
        for lfn in group[jobID]:
          del success[lfn]
        success[lfns] = jobs[jobID]

  if summary and retVal['OK']:
    gLogger.always('List of DIRAC jobs')
    gLogger.always(','.join(sorted([job.replace('Job ', '') for job in group])))
  else:
    printDMResult(retVal, empty="File does not exists in the Bookkeeping")
