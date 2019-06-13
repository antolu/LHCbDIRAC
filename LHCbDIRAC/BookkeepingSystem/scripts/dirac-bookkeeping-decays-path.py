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
""" Get Bookkeeping paths given a decay

    @author Vanya BELYAEV Ivan.Belyaev@itep.ru
            Federico Stagni fstagni@cern.ch
"""

__RCSID__ = "$Id$"

import ast
import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequestClient import ProductionRequestClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient


Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s eventType  ' % Script.scriptName]))

Script.registerSwitch('p', 'production', "Obtain the paths in ``Production format'' for Ganga")
Script.parseCommandLine(ignoreErrors=True)

productionFormat = False
for p, _v in Script.getUnprocessedSwitches():
  if p.lower() in ('p', 'production'):
    productionFormat = True
  break

args = Script.getPositionalArgs()
if len(args) != 1:
  Script.showHelp()
  DIRAC.exit(1)

eventType = args[0]

bkClient = BookkeepingClient()

# # get productions for given event type
res = bkClient.getProductionSummaryFromView({'EventType': eventType, 'Visible': True})
if not res['OK']:
  gLogger.error('Could not retrieve production summary for event %s' % eventType, res['Message'])
  DIRAC.exit(1)
prods = res['Value']

# # get production-IDs
prodIDs = [p['Production'] for p in prods]

# # loop over all productions
for prodID in sorted(prodIDs):

  res = bkClient.getProductionInformations(prodID)
  if not res['OK']:
    gLogger.error('Could not retrieve production infos for production %s' % prodID, res['Message'])
    continue
  prodInfo = res['Value']
  steps = prodInfo['Steps']
  if isinstance(steps, str):
    continue
  files = prodInfo["Number of files"]
  events = prodInfo["Number of events"]
  path = prodInfo["Path"]

  dddb = None
  conddb = None

  for step in reversed(steps):
    if step[4] and step[4].lower() != 'frompreviousstep':
      dddb = step[4]
    if step[5] and step[5].lower() != 'frompreviousstep':
      conddb = step[5]

  result = TransformationClient().getTransformation(prodID, True)
  if not result['OK']:
    gLogger.error('Could not retrieve parameters for production %d:' % prodID, result['Message'])
    continue
  parameters = result['Value']

  if not dddb:
    dddb = parameters.get('DDDBTag')
  if not conddb:
    conddb = parameters.get('CondDBTag')

  if not (dddb and conddb):  # probably the production above was not a MCSimulation
    reqID = int(parameters.get('RequestID'))
    res = ProductionRequestClient().getProductionList(reqID)
    if not res['OK']:
      gLogger.error('Could not retrieve productions list for request %d:' % reqID, result['Message'])
      continue
    simProdID = res['Value'][0]  # this should be the MCSimulation
    res = TransformationClient().getTransformation(simProdID, True)
    if not res['OK']:
      gLogger.error('Could not retrieve parameters for production %d:' % simProdID, result['Message'])
      continue
    if not dddb:
      dddb = res['Value'].get('DDDBTag', 0)
    if not conddb:
      conddb = res['Value'].get('CondDBTag', 0)

  if not (dddb and conddb):  # this is for more recent productions (in fact, most of them)
    dddb = ast.literal_eval(res['Value']['BKProcessingPass'])['Step0']['DDDb']
    conddb = ast.literal_eval(res['Value']['BKProcessingPass'])['Step0']['CondDb']

  evts = 0
  ftype = None
  for i in events:
    if i[0] in ['GAUSSHIST', 'LOG', 'SIM', 'DIGI']:
      continue
    evts += i[1]
    if not ftype:
      ftype = i[0]

  nfiles = 0
  for f in files:
    if f[1] in ['GAUSSHIST', 'LOG', 'SIM', 'DIGI']:
      continue
    if f[1] != ftype:
      continue
    nfiles += f[0]

  p0, n, p1 = path.partition('\n')
  if n:
    path = p1

  if productionFormat:
    p, s, e = path.rpartition('/')
    if s and e:
      path = '/%d/%d/%s' % (prodID, int(eventType), e)
  print (path, dddb, conddb, nfiles, evts, prodID)
