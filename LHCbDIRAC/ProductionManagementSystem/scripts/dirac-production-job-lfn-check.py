#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-production-job-lfn-check.py
# Author :  Stuart Paterson
########################################################################
"""
  Select jobs and check the status of: LFC, BK and prodDB
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Job ...' % Script.scriptName,
                                     'Arguments:',
                                     '  Job:      DIRAC Job Id' ] ) )
Script.parseCommandLine( ignoreErrors = True )

args = Script.getPositionalArgs()
if len( args ) == 0:
  Script.showHelp()

def printDict( dictionary ):
  """ Dictionary pretty printing
  """
  key_max = 0
  value_max = 0
  for key, value in dictionary.items():
    if len( key ) > key_max:
      key_max = len( key )
    if len( str( value ) ) > value_max:
      value_max = len( str( value ) )
  for key, value in dictionary.items():
    print key.rjust( key_max ), ' : ', str( value ).ljust( value_max )

from LHCbDIRAC.Core.Utilities.JobInfoFromXML        import JobInfoFromXML
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from DIRAC.Core.DISET.RPCClient                       import RPCClient

rm = ReplicaManager()
bk = BookkeepingClient()
prodClient = RPCClient( 'ProductionManagement/ProductionManager' )

for arg in args:
  try:
    job = int( arg )
  except:
    print "Wrong argument, job must be integer %s  " % arg
    continue

  jobinfo = JobInfoFromXML( job )
  result = jobinfo.valid()
  if not result['OK']:
    print result['Message']
    continue

  result = jobinfo.getOutputLFN()
  if not result['OK']:
    print result['Message']
  else:
    lfns = result['Value']

    replicas = rm.getReplicas( lfns )
    print "LFC:"
    if replicas['OK']:
      value = replicas['Value']
      if len( value['Successful'] ):
        print 'Successful:'
        printDict( value['Successful'] )
      if len( value['Failed'] ):
        print 'Failed:'
        printDict( value['Failed'] )
    else:
      print replicas['Message']

    bkresponce = bk.exists( lfns )
    print "Bookkeeping:"
    if bkresponce['OK']:
      bkvalue = bkresponce['Value']
      printDict( bkvalue )
    else:
      print bkresponce['Message']

  result = jobinfo.getInputLFN()
  if not result['OK']:
    print result['Message']
  else:
    lfns = result['Value']
    if len( lfns ):
      prodid = int( jobinfo.prodid )
      print 'ProductionDB for production %d' % prodid
      fs = prodClient.getFileSummary( lfns, prodid )
      if fs['OK']:
        for lfn in lfns:
          print fs['Value']['Successful'][lfn][prodid]
    else:
      print fs['Message']
