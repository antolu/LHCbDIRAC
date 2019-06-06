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
# File :    dirac-wms-job-parameters
# Author :  Philippe Charpentier
########################################################################
"""
  Retrieve parameters associated to the given DIRAC job
"""

# FIXME: this is almost the same of vanilla DIRAC script that has the same name

__RCSID__ = "$Id$"

import DIRAC
from LHCbDIRAC.DataManagementSystem.Client.DMScript import Script, printDMResult

Script.registerSwitch('', 'Parameters=', '   If present, print out only those parameters')
Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option|cfgfile] ... JobID ...' % Script.scriptName,
                                  'Arguments:',
                                  '  JobID:    DIRAC Job ID']))
Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()

if len(args) < 1:
  Script.showHelp()
parameters = None
for switch in Script.getUnprocessedSwitches():
  if switch[0] == 'Parameters':
    parameters = switch[1].split(',')

from DIRAC.Interfaces.API.Dirac import Dirac, parseArguments
dirac = Dirac()

results = {'OK': True, 'Value': {'Successful': {}, 'Failed': {}}}
success = results['Value']['Successful']
failed = results['Value']['Failed']
for job in parseArguments(args):
  jobStr = 'Job %s' % job
  result = dirac.getJobParameters(job, printOutput=False)
  if not result['OK']:
    failed.update({jobStr: result['Message']})
  elif not result['Value']:
    failed.update({jobStr: 'Job not found'})
  elif parameters:
    params = dict((key, val) for key, val in result['Value'].iteritems() if key in parameters)
    success.update({jobStr: params})
  else:
    success.update({jobStr: result['Value']})

DIRAC.exit(printDMResult(results, empty="Job not found"))
