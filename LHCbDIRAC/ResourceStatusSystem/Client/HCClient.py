''' HCClient

 Client to connect and interact with HammerCloud.

'''

import json
import urllib2

from datetime import datetime

__RCSID__ = "$Id$"

def checkInt( name, value ):
 '''
 If value is not of type `int`, raise Exception
 '''

 if not isinstance( value, int ):
   raise Exception( 'Wrong %s, expected int.' % name )
 return value

def checkBool( name, value ):
 '''
 If value is not of type `bool`, raise Exception
 '''

 if not isinstance( value, bool ):
   raise Exception( 'Wrong %s, expected bool.' % name )
 return value

def checkDatetime( name, value ):
 '''
 If value is not of type `datetime`, raise Exception
 '''

 if not isinstance( value, datetime ):
   raise Exception( 'Wrong %s, expected datetime.' % name )
 return value.strftime( "%Y-%m-%d %H:%M:%S" )

def checkDict( name, value ):
 '''
 If value is not of type `dict`, raise Exception
 '''

 if not isinstance( value, dict ):
   raise Exception( 'Wrong %s, expected dict.' % name )
 return value

################################################################################
# END TYPE checkers
################################################################################

class HCClient:
 '''
 Class HCClient. It creates a connection to the HammerCloud server.

 The available methods are:

 - getTestResults
 - getHistoryReport

 '''

################################################################################

 def __init__( self ):
  '''
  Initialize HCClient
  '''

  # URL of the hammercloud API
  self.hc_api = "http://hammercloud.cern.ch/hc/app/lhcb"

################################################################################

 def getTestResults( self, testID ):
  '''
  Returns JSON results with information of a given given test ID
  -for example with test ID 1750.

  :params:
   :attr: `test` : str - test ID

  :return:
     {
     'start_time': ...,
     'site': ...,
     'backendID': ...,
     'ganga_status': ...,
     'stop_time': ...',
     'submit_time': ...,
     'id': ...
     }
     {
     'start_time': ...,
     'site': ...,
     .
     .
     }
  '''

  try:
    response = urllib2.urlopen( self.hc_api + "/xhr/json/?action=results&test=" + testID )
  except urllib2.HTTPError, e:
    return 'HTTP Error: ' + str(e.code) + ', ' + str(e.reason)
  except urllib2.URLError, e:
    return 'URL Error: ' + str(e.reason)

  fetchedData = response.read()
  # The following is nessesary since the json will be invalid if it contains unquoted 'None'
  fetchedData = fetchedData.replace('None', '"None"')
  fetchedData = json.loads(fetchedData)

  return fetchedData[testID]['results']

################################################################################

 def getHistoryReport( self, pastDays ):
  '''
  Returns JSON results of the tests submitted by HC at each site.

  :params:
   :attr: `pastDays` : str - days in the past to take in consideration when querying the results of the report.

  :return:
  {
    "generated": ...,
    "starttime": ...,
    "time": ...,
    "sites": {
      "site.example.org": [{
        "count": ...,
        "reason": ...,
        "ganga_status": ...,
        "failover": ...,
        "minor_reason": ...
      },
      {
      .
      .
      }
  }
  '''

  try:
    response = urllib2.urlopen( self.hc_api + "/ajax/historyreport/" + pastDays )
  except urllib2.HTTPError, e:
    return 'HTTP Error: ' + str(e.code) + ', ' + str(e.reason)
  except urllib2.URLError, e:
    return 'URL Error: ' + str(e.reason)

  fetchedData = json.load(response)

  return fetchedData

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
