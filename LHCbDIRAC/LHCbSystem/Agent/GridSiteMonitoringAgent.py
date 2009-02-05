# $Id: GridSiteMonitoringAgent.py,v 1.8 2009/02/05 16:58:19 acasajus Exp $

__author__ = 'Greig A Cowan'
__date__ = 'September 2008'
__version__ = 0.1

'''
Queries BDII to pick out information about SRM2.2 space token descriptions.
'''

from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.AccountingSystem.Client.ReportsClient import ReportsClient
from DIRAC.Core.Utilities import Time, List, DEncode
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager

import sys, os
import tempfile
import time
import datetime
import types

class GridSiteMonitoringAgent(AgentModule):

  __sitesT1 = [ 'PIC', 'GRIDKA', 'CNAF', 'IN2P3', 'NIKHEF', 'RAL']
  __sitesAll = [ 'CERN', 'PIC', 'GRIDKA', 'CNAF', 'IN2P3', 'NIKHEF', 'RAL']

  def initialize( self ):
    self._lastUpdateTime = 0
    return S_OK()

  def execute( self ):
    elapsedTime = time.time() - self._lastUpdateTime
    if elapsedTime < self.am_getOption( "GenerationInterval", 1800 ):
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
    startT = endT - datetime.timedelta( seconds = self.am_getOption( "Timespan", 3600 ) )
    activities = []
    activities.append( ( { 'Source' : [ 'CERN' ], 'Destination' : self.__sitesT1, 'Protocol' : 'FTS' },
                         'data_transfer_t0_t1', 'Channel' ) )
    activities.append( ( { 'Source' : [ 'CERN' ], 'Destination' : self.__sitesT1, 'Protocol' : 'FTS' },
                         'data_transfer_t0_t1', 'Source' ) )
    activities.append( ( { 'Source' : self.__sitesT1, 'Destination' : self.__sitesT1, 'Protocol' : 'FTS' },
                         'data_transfer_t1_t1', 'Channel' ) )
    activities.append( ( { 'Source' : self.__sitesT1, 'Destination' : self.__sitesT1, 'Protocol' : 'FTS' },
                         'data_transfer_t1_t1', 'Source' ) )
    activities.append( ( { 'Source' : self.__sitesT1, 'Destination' : self.__sitesT1, 'Protocol' : 'FTS' },
                         'data_transfer_t1_t1', 'Destination' ) )
    activities.append( ( { 'Source' : self.__sitesAll, 'Destination' : self.__sitesAll, 'Protocol' : 'FTS' },
                         'data_transfer', 'Channel' ) )
    activities.append( ( { 'Source' : self.__sitesAll, 'Destination' : self.__sitesAll, 'Protocol' : 'FTS' },
                         'data_transfer', 'Source' ) )
    activities.append( ( { 'Source' : self.__sitesAll, 'Destination' : self.__sitesAll, 'Protocol' : 'FTS' },
                         'data_transfer', 'Destination' ) )
    rC = ReportsClient()
    for func in ( self._dataGetSuccessRate, self._dataGetThroughput ):
      for cond, acName, grouping in activities:
        result = func( startT, endT, cond, acName, rC, grouping )
        if not result[ 'OK' ]:
          return result
        finalData.extend( result[ 'Value' ][1] )
    return S_OK( finalData )

  def _dataGetSuccessRate( self, startT, endT, reportCond, metricName, rC, groupBy = 'Channel' ):
    metricName = "%s,success_rate" % metricName
    result = rC.getReport( "DataOperation", 'SuceededTransfers', startT, endT,
                          reportCond, 'Channel' )
    startEpoch = Time.toEpoch( startT )
    endEpoch = Time.toEpoch( endT )
    if not result[ 'OK' ]:
      return result
    suceededData, extraData = self._sumDataBuckets( result[ 'Value' ], [ 'Failed' ] )
    result = rC.getReport( "DataOperation", 'FailedTransfers', startT, endT,
                          reportCond, groupBy )
    if not result[ 'OK' ]:
      return result
    failedData, extraData = self._sumDataBuckets( result[ 'Value' ], [ 'Suceeded' ] )
    okRateData = {}
    for channel in suceededData:
      if channel in failedData:
        okRateData[channel] = ( suceededData[channel] / ( suceededData[channel] + failedData[channel] ) ) * 100
    finalData = []
    extraURL = self._generateDataExtraURL( "SuceededTransfers", reportCond, groupBy )
    for channel in okRateData:
      self._appendToFinalData( finalData, channel, metricName, okRateData[ channel ], 100.0,
                               startEpoch, endEpoch, extraURL, groupBy  )
    if groupBy == 'Channel':
      allSites =[ "%s -> %s" % ( source, destination ) for source in reportCond[ 'Source' ]
                                                       for destination in reportCond[ 'Destination' ] ]
    else:
      allSites = reportCond[ groupBy ]
    for channel in allSites:
      if channel in okRateData:
        continue
      self._appendToFinalData( finalData, channel, metricName, 0.0, 0.0,
                               startEpoch, endEpoch, extraURL, groupBy  )

    gLogger.info( "[DATA]%s data" % metricName, "%s records" % len( finalData ) )
    return S_OK( ( okRateData, finalData ) )

  def _appendToFinalData( self, finalData, sites, fullMetricName, value, expected, start, end, URL, groupBy ):
      if groupBy == "Channel":
        sites = List.fromChar( sites, "->" )
      elif groupBy == 'Source':
        sites = [ sites, 'all' ]
      elif groupBy == 'Destination':
        sites = [ 'all', sites ]
      finalData.append( "%s,%s,%s,%.1f,%.1f,unknown,%d,%d,%s" % ( sites[0], sites[1], fullMetricName,
                                                                  value, expected, start, end,
                                                                  URL ) )

  def _dataGetThroughput( self, startT, endT, reportCond, metricName, rC, groupBy = 'Channel' ):
    metricName = "%s,average_transfer_rate" % metricName
    result = rC.getReport( "DataOperation", 'Throughput', startT, endT,
                          reportCond, groupBy )
    if not result[ 'OK' ]:
      return result
    startEpoch = Time.toEpoch( startT )
    endEpoch = Time.toEpoch( endT )
    throughtputData, extraData = self._sumDataBuckets( result[ 'Value' ], average= True )
    finalData = []
    extraURL = self._generateDataExtraURL( "Throughput", reportCond, groupBy  )
    for channel in throughtputData:
      self._appendToFinalData( finalData, channel, metricName, throughtputData[channel], -1,
                               startEpoch, endEpoch, extraURL, groupBy  )
    gLogger.info( "[DATA]%s data" % metricName, "%s records" % len( finalData ) )
    return S_OK( ( throughtputData, finalData ) )

  def _generateDataExtraURL( self, reportName, reportCond, groupBy = 'Channel' ):
    baseURL = "http://lhcbweb.pic.es/DIRAC/%s/visitor/systems/accountingPlots/dataOperation" % gConfig.getValue( "/DIRAC/Setup" )
    baseReportDesc = { '_plotName': reportName, '_grouping': groupBy, '_typeName': 'DataOperation', '_timeSelector': '86400' }
    for key in reportCond:
      value = reportCond[ key ]
      if type( value ) == types.StringType:
        baseReportDesc[ "_%s" % key ] = value
      else:
        baseReportDesc[ "_%s" % key ] = ",".join( value )
    return "%s#%s" % ( baseURL, DEncode.encode( baseReportDesc ) )