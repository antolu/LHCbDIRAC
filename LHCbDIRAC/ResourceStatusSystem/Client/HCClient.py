''' HCClient

 Client to connect and interact with HammerCloud.

'''

import json
import urllib2

__RCSID__ = "$Id$"

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
   :attr: `test` : int - test ID

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
    response = urllib2.urlopen( self.hc_api + "/xhr/json/?action=results&test=" + str(testID) )
    fetchedData = response.read()
  except urllib2.HTTPError as e:
    return 'HTTP Error: ' + repr(e.code) + ', ' + repr(e.reason)
  except urllib2.URLError as e:
    return 'URL Error: ' + repr(e.reason)

  # The following is nessesary since the json will be invalid if it contains unquoted 'None'
  fetchedData = fetchedData.replace('None', '"None"')

  try:
    fetchedData = json.loads(fetchedData)
  except ValueError as e:
    return 'Invalid JSON: ' + repr(e.message)

  return fetchedData[str(testID)]['results']

################################################################################

 def getHistoryReport( self, pastDays ):
  '''
  Returns JSON results of the tests submitted by HC at each site.

  :params:
   :attr: `pastDays` : int - days in the past to take in consideration when querying the results of the report.

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
    response = urllib2.urlopen( self.hc_api + "/ajax/historyreport/" + str(pastDays) )
  except urllib2.HTTPError, e:
    return 'HTTP Error: ' + repr(e.code) + ', ' + repr(e.reason)
  except urllib2.URLError, e:
    return 'URL Error: ' + repr(e.reason)

  try:
    fetchedData = json.load(response)
  except ValueError as e:
    return 'Invalid JSON: ' + repr(e.message)

  return fetchedData

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
