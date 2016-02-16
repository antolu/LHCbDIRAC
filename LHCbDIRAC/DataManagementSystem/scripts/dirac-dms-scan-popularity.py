#!/usr/bin/env python
"""
  Script for scanning the popularity table
  """
if __name__ == '__main__':

  # Script initialization
  from DIRAC.Core.Base import Script

  since = 30
  getAllDatasets = False
  Script.registerSwitch( '', 'Since=', '   Number of days to look for (default: %d)' % since )
  Script.registerSwitch( '', 'All', '   If used, gets all existing datasets, not only the used ones' )
  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile]' % Script.scriptName, ] ) )
  Script.parseCommandLine( ignoreErrors = True )
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Since':
      try:
        since = int( switch[1] )
      except:
        pass
    elif switch[0] == 'All':
      getAllDatasets = True

  try:
    from ScanPopularity import scanPopularity
  except ImportError:
    from LHCbDIRAC.DataManagementSystem.Client.ScanPopularity import scanPopularity
  scanPopularity( since, getAllDatasets )


