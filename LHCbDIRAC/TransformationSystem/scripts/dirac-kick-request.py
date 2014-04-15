#!/usr/bin/env python
"""
    Script for resetting Waiting old Assigned requests
    """
from DIRAC.Core.Base import Script

requestType = ''
kickRequests = False
delay = 2

Script.registerSwitch( '', 'RequestType=', '   Type of request to look for (transfer, removal)' )
Script.registerSwitch( '', 'KickRequests', '   Resets the selected requests to Waiting' )
Script.registerSwitch( '', 'Delay=', '   Number of hours of delay before kicking [%d]' % delay )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] --RequestType <type>' % Script.scriptName, ] ) )
Script.parseCommandLine( ignoreErrors = True )

import DIRAC
from DIRAC.RequestManagementSystem.Client.RequestClient             import RequestClient
requestClient = RequestClient()

switches = Script.getUnprocessedSwitches()
for switch in switches:
  if switch[0] == 'RequestType':
    requestType = switch[1].lower()
  elif switch[0] == 'KickRequests':
    kickRequests = True
  elif switch[0] == 'Delay':
    delay = float( switch[1] )

if not requestType:
  print "--RequestType <type> is a mandatory option"
  Script.showHelp()
  DIRAC.exit( 0 )


import datetime
now = datetime.datetime.utcnow()
timeLimit = now - datetime.timedelta( hours = delay )
selectDict = { 'RequestType':requestType, 'Status':'Assigned', 'ToDate':timeLimit}
res = requestClient.getRequestSummaryWeb( selectDict, [], 0, 100000 )
nkicks = 0
if res['OK']:
  params = res['Value']['ParameterNames']
  records = res['Value']['Records']
  for rec in records:
    subReqDict = {}
    subReqStr = ''
    conj = ''
    for i in range( len( params ) ):
      subReqDict.update( { params[i]:rec[i] } )
      subReqStr += conj + params[i] + ': ' + rec[i]
      conj = ', '

    if subReqDict['Status'] == 'Assigned' and subReqDict['LastUpdateTime'] < str( timeLimit ):
      nkicks += 1
      print subReqStr
      if kickRequests:
        res = requestClient.setRequestStatus( subReqDict['RequestName'], 'Waiting' )
        if res['OK']:
          print 'Request %s kicked to Waiting' % str( subReqDict['RequestID'] )

if kickRequests:
  comment = 'have been'
else:
  comment = 'can be'
print nkicks, 'requests', comment, 'kicked'
