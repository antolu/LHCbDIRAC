# $Id: GridSiteMonitoringAgent.py,v 1.2 2008/11/05 19:55:19 acasajus Exp $

__author__ = 'Greig A Cowan'
__date__ = 'September 2008'
__version__ = 0.1

'''
Queries BDII to pick out information about SRM2.2 space token descriptions.
'''

from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.Agent import Agent
from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
from DIRAC.Core.Utilities import Time, List, DEncode
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager

import sys, os
import tempfile
import time
import datetime
import types

AGENT_NAME = "LHCb/GridSiteMonitoringAgent"

class GridSiteMonitoringAgent(Agent):

  __dataT1 = [ 'PIC', 'GRIDKA', 'CNAF', 'IN2P3', 'NIKHEF', 'RAL']

  def __init__( self ):
    Agent.__init__( self, AGENT_NAME )

  def initialize( self ):
    result = Agent.initialize( self )
    if not result[ 'OK' ]:
      return result
    self._lastUpdateTime = 0
    return S_OK()

  def execute( self ):
    elapsedTime = time.time() - self._lastUpdateTime
    if elapsedTime < gConfig.getValue( "%s/GenerationInterval" % self.section, 1800 ):
      return S_OK()
    result = self._retrieveDataContents()
    if not result[ 'OK' ]:
      return result
    fileContents = "%s\n" % "\n".join( result[ 'Value' ] )
    self._lastUpdateTime = time.time()
    return self._commitFileContents( fileContents )

  def _commitFileContents( self, fileData ):
    fd, fName = tempfile.mkstemp()
    os.write( fd, fileData )
    os.close( fd )
    rm = ReplicaManager()
    result = rm.put( "/lhcb/monitoring/lhcb.sitemonitoring.csv", fName, 'LogSE' )
    try:
      os.unlink( fName )
    except Exception, e:
      gLogger.error( "Can't unlink temporal file", "%s: %s" % ( fName, str(e) ) )
    return result


  def _sumDataBuckets( self, bucketedData, filterList = [], average = False ):
    granularity = bucketedData[ 'granularity' ]
    bucketedData = bucketedData[ 'data' ]
    addedData = {}
    extraData = {}
    for key in bucketedData:
      if key in filterList:
        continue
      if not key in addedData:
        addedData[ key ] = 0
        extraData[ key ] = [ 0, 0, 0]
      for time in bucketedData[ key ]:
        addedData[ key ] += bucketedData[ key ][ time ]
        if extraData[ key ][0]:
          extraData[ key ][0] = min( extraData[ key ][1], time )
        else:
          extraData[ key ][0] = time
        extraData[ key ][1] = max( extraData[ key ][2], time + granularity )
        extraData[ key ][2] += 1
    if average:
      for key in addedData:
        addedData[key] /= extraData[key][2]
    return addedData, extraData

  def _retrieveDataContents(self):
    gLogger.info( "[DATA] Retrieving info...")
    finalData = []
    endT = Time.dateTime()
    startT = endT - datetime.timedelta( seconds = gConfig.getValue( "%s/Timespan" % self.section, 3600 ) )
    t0t1Cond = { 'Source' : 'CERN', 'Destination' : self.__dataT1, 'Protocol' : 'FTS' }
    t1t1Cond = { 'Source' : self.__dataT1, 'Destination' : self.__dataT1, 'Protocol' : 'FTS' }
    rC = ReportsClient()
    for func in ( self._dataGetSuccessRate, self._dataGetThroughput ):
      for cond, acName in ( ( t0t1Cond, 'data_transfer_t0_t1' ), ( t1t1Cond, 'data_transfer_t1_t1' ) ):
        result = func( startT, endT, cond, acName, rC )
        if not result[ 'OK' ]:
          return result
        finalData.extend( result[ 'Value' ][1] )
    return S_OK( finalData )

  def _dataGetSuccessRate( self, startT, endT, reportCond, metricName, rC ):
    result = rC.getReport( "DataOperation", 'SuceededTransfers', startT, endT,
                          reportCond, 'Channel' )
    if not result[ 'OK' ]:
      return result
    suceededData, extraData = self._sumDataBuckets( result[ 'Value' ], [ 'Failed' ] )
    result = rC.getReport( "DataOperation", 'FailedTransfers', startT, endT,
                          reportCond, 'Channel' )
    if not result[ 'OK' ]:
      return result
    failedData, extraData = self._sumDataBuckets( result[ 'Value' ], [ 'Suceeded' ] )
    okRateData = {}
    for channel in suceededData:
      if channel in failedData:
        okRateData[channel] = ( suceededData[channel] / ( suceededData[channel] + failedData[channel] ) ) * 100
    finalData = []
    extraURL = self._generateDataExtraURL( "SuceededTransfers", reportCond )
    for channel in okRateData:
      sites = List.fromChar( channel, "->" )
      finalData.append( "%s,%s,%s,success_rate,%.1f,-1,unknown,%d,%d,%s" % ( sites[0], sites[1], metricName,
                                                                           okRateData[channel],
                                                                           extraData[channel][0],
                                                                           extraData[channel][1],
                                                                           extraURL ) )
    gLogger.info( "[DATA]%s successRate data" % metricName, "%s records" % len( finalData ) )
    return S_OK( ( okRateData, finalData ) )

  def _dataGetThroughput( self, startT, endT, reportCond, metricName, rC ):
    result = rC.getReport( "DataOperation", 'Throughput', startT, endT,
                          reportCond, 'Channel' )
    if not result[ 'OK' ]:
      return result
    throughtputData, extraData = self._sumDataBuckets( result[ 'Value' ], average= True )
    finalData = []
    extraURL = self._generateDataExtraURL( "Throughput", reportCond )
    for channel in throughtputData:
      sites = List.fromChar( channel, "->" )
      finalData.append( "%s,%s,%s,average_transfer_rate,%.1f,-1,unknown,%d,%d,%s" % ( sites[0], sites[1], metricName,
                                                                         throughtputData[channel],
                                                                           extraData[channel][0],
                                                                           extraData[channel][1],
                                                                           extraURL ) )
    gLogger.info( "[DATA]%s throughput data" % metricName, "%s records" % len( finalData ) )
    return S_OK( ( throughtputData, finalData ) )

  def _generateDataExtraURL( self, reportName, reportCond ):
    baseURL = "http://lhcbweb.pic.es/DIRAC/%s/visitor/systems/accountingPlots/dataOperation" % gConfig.getValue( "/DIRAC/Setup" )
    baseReportDesc = { '_plotName': reportName, '_grouping': 'Channel', '_typeName': 'DataOperation', '_timeSelector': '86400' }
    for key in reportCond:
      value = reportCond[ key ]
      if type( value ) == types.StringType:
        baseReportDesc[ "_%s" % key ] = value
      else:
        baseReportDesc[ "_%s" % key ] = ",".join( value )
    return "%s#%s" % ( baseURL, DEncode.encode( baseReportDesc ) )