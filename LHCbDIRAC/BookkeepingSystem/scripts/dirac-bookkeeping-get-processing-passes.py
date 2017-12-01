#!/usr/bin/env python

"""
 List all BK paths matching a wildcard path ('...' is the wildcard character, or '*' but enclose with quotes)
"""

__RCSID__ = "$Id$"

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script, ProgressBar
from DIRAC import gLogger, exit

if __name__ == "__main__":

  Script.registerSwitch("B:", "BKQuery=", "   Bookkeeping query path")
  Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ...' % Script.scriptName, ]))

  Script.parseCommandLine(ignoreErrors=True)

  bkPaths = []
  for opt, val in Script.getUnprocessedSwitches():
    if opt == 'BKQuery':
      bkPaths = val.split(',')
  if not bkPaths:
    gLogger.error('No BK path provided...')
    Script.showHelp()
    exit(1)

  from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import getProcessingPasses, BKQuery

  for i, bkPath in enumerate(bkPaths):
    if i:
      gLogger.notice('=========================')
    bkQuery = BKQuery(bkPath.replace('Real Data', 'RealData'))
    progressBar = ProgressBar(1, title="Getting processing passes for BK path %s" % bkPath)
    processingPasses = getProcessingPasses(bkQuery)
    progressBar.endLoop()
    if processingPasses:
      gLogger.notice('\n'.join([''] + [procPass.replace('Real Data', 'RealData')
                                       for procPass in sorted(processingPasses)]))
    else:
      gLogger.notice("No processing passes matching the BK path")
