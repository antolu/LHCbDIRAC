#!/usr/bin/env python
"""
This script allows to print information about a (list of) transformations
"""

from DIRAC.Core.Base import Script

if __name__ == "__main__":

  informations = ['AuthorDN', 'AuthorGroup', 'Body', 'CreationDate',
                  'Description', 'EventsPerTask', 'FileMask', 'GroupSize', 'Hot',
                  'InheritedFrom', 'LastUpdate', 'LongDescription', 'MaxNumberOfTasks',
                  'Plugin', 'Status', 'TransformationGroup',
                  'TransformationName', 'Type', 'Request']
  Script.registerSwitch('', 'Information=', '   Specify which information is required')
  Script.setUsageMessage('\n'.join([__doc__,
                                    'Usage:',
                                    '  %s [options] transID1 [transID2 ...]' % Script.scriptName,
                                    'Arguments:',
                                    '\ttransID1,... : transformantion IDs',
                                    'Possible informations:',
                                    '\t%s' % ', '.join(sorted(informations))
                                    ])
                         )
  Script.parseCommandLine(ignoreErrors=True)

  from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  from DIRAC import gLogger

  tr = TransformationClient()

  requestedInfo = informations
  switches = Script.getUnprocessedSwitches()
  infoList = []
  for switch, val in switches:
    if switch == 'Information':
      infoList = [info.lower() for info in val.split(',')]
      requestedInfo = [info for info in informations if info.lower() in infoList]
  if 'body' not in infoList and 'Body' in requestedInfo:
    requestedInfo.remove('Body')

  args = Script.getPositionalArgs()

  for arg in args:
    try:
      res = tr.getTransformation(int(arg))
      gLogger.notice("==== Transformation %s ====" % arg)
      for info in requestedInfo:
        getInfo = info if info != 'Request' else 'TransformationFamily'
        gLogger.notice("\t%s: %s" %
                       (info, res.get('Value', {}).get(getInfo, 'Unknown')))
    except Exception:
      gLogger.error("Invalid transformation ID: '%s'" % arg)
