## $Id: GridSiteMonitoringAgent.py 48769 2012-03-15 11:20:39Z ubeda $
#
#__author__ = 'Greig A Cowan'
#__date__ = 'September 2008'
#__version__ = 0.1
#
#'''
#Queries BDII to pick out information about SRM2.2 space token descriptions.
#'''
#
#from DIRAC                                            import S_OK, S_ERROR, gConfig
#from DIRAC.Core.Base.AgentModule                      import AgentModule
#from DIRAC.AccountingSystem.Client.ReportsClient      import ReportsClient
#from DIRAC.Core.Utilities                             import Time, List, DEncode
#from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
#
#import sys, os
#import tempfile
#import time
#import datetime
#import types
#
#class GridSiteMonitoringAgent( AgentModule ):
#
#  __sitesT1  = [ 'PIC', 'GRIDKA', 'CNAF', 'IN2P3', 'NIKHEF', 'RAL' ]
#  __sitesAll = [ 'CERN', 'PIC', 'GRIDKA', 'CNAF', 'IN2P3', 'NIKHEF', 'RAL' ]
#
#  def initialize( self ):
#    self.am_setOption( "PollingTime", 900 )
#    return S_OK()
#
#  def execute( self ):
#    if self.am_getOption( "EnabledDataTransferMonitoring", True ):
#      result = self._retrieveDataContents()
#      if not result[ 'OK' ]:
#        return result
#      fileContents = "%s\n" % "\n".join( result[ 'Value' ] )
#      self._lastUpdateTime = time.time()
#      result = self._commitFileContents( fileContents )
#      if not result[ 'OK' ]:
#        return result
#    #Generate
#    if self.am_getOption( "EnabledStartedJobsMonitoring", True ):
#      result = self._retrieveStartedJobs()
#      if not result[ 'OK' ]:
#        return result
#      fileContents = "%s\n" % "\n".join( result[ 'Value' ] )
#      result = self._commitFileContents( fileContents, remoteFileName = 'lhcb.sitemonitoring.startedjobs.csv' )
#      if not result[ 'OK' ]:
#        return result
#    return S_OK()
#
#  def _commitFileContents( self, fileData, remoteFileName = 'lhcb.sitemonitoring.csv' ):
#    fd, fName = tempfile.mkstemp()
#    os.write( fd, fileData )
#    os.close( fd )
#    rm = ReplicaManager()
#    self.log.info( "Uploading %s" % remoteFileName )
#    result = rm.put( "/lhcb/monitoring/%s" % remoteFileName, fName, 'LogSE' )
#    try:
#      os.unlink( fName )
#    except Exception, e:
#      self.log.error( "Can't unlink temporal file", "%s: %s" % ( fName, str(e) ) )
#    return result
#
#
#  def _sumDataBuckets( self, bucketedData, filterList = [], average = False ):
#    granularity = bucketedData[ 'granularity' ]
#    bucketedData = bucketedData[ 'data' ]
#    addedData = {}
#    extraData = {}
#    for key in bucketedData:
#      if key in filterList:
#        continue
#      if not key in addedData:
#        addedData[ key ] = 0
#        extraData[ key ] = [ 0, 0, 0]
#      for time in bucketedData[ key ]:
#        addedData[ key ] += bucketedData[ key ][ time ]
#        if extraData[ key ][0]:
#          extraData[ key ][0] = min( extraData[ key ][1], time )
#        else:
#          extraData[ key ][0] = time
#        extraData[ key ][1] = max( extraData[ key ][2], time + granularity )
#        extraData[ key ][2] += 1
#    if average:
#      for key in addedData:
#        addedData[key] /= extraData[key][2]
#    return addedData, extraData
#
#  def _retrieveDataContents(self):
#    self.log.info( "[DATA] Retrieving info...")
#    finalData = []
#    endT = Time.dateTime()
#    startT = endT - datetime.timedelta( seconds = self.am_getOption( "Timespan", 3600 ) )
#    activities = []
#    activities.append( ( { 'Source' : [ 'CERN' ], 'Destination' : self.__sitesT1, 'Protocol' : 'FTS' },
#                         'data_transfer_t0_t1', 'Channel' ) )
#    activities.append( ( { 'Source' : [ 'CERN' ], 'Destination' : self.__sitesT1, 'Protocol' : 'FTS' },
#                         'data_transfer_t0_t1', 'Source' ) )
#    activities.append( ( { 'Source' : self.__sitesT1, 'Destination' : self.__sitesT1, 'Protocol' : 'FTS' },
#                         'data_transfer_t1_t1', 'Channel' ) )
#    activities.append( ( { 'Source' : self.__sitesT1, 'Destination' : self.__sitesT1, 'Protocol' : 'FTS' },
#                         'data_transfer_t1_t1', 'Source' ) )
#    activities.append( ( { 'Source' : self.__sitesT1, 'Destination' : self.__sitesT1, 'Protocol' : 'FTS' },
#                         'data_transfer_t1_t1', 'Destination' ) )
#    activities.append( ( { 'Source' : self.__sitesAll, 'Destination' : self.__sitesAll, 'Protocol' : 'FTS' },
#                         'data_transfer', 'Channel' ) )
#    activities.append( ( { 'Source' : self.__sitesAll, 'Destination' : self.__sitesAll, 'Protocol' : 'FTS' },
#                         'data_transfer', 'Source' ) )
#    activities.append( ( { 'Source' : self.__sitesAll, 'Destination' : self.__sitesAll, 'Protocol' : 'FTS' },
#                         'data_transfer', 'Destination' ) )
#    rC = ReportsClient()
#    for func in ( self._dateGetTransferedBytes, self._dataGetSuccessRate, self._dataGetThroughput ):
#      for cond, acName, grouping in activities:
#        result = func( startT, endT, cond, acName, rC, grouping )
#        if not result[ 'OK' ]:
#          return result
#        finalData.extend( result[ 'Value' ] )
#    finalData.sort()
#    return S_OK( finalData )
#
#  def _dataGetSuccessRate( self, startT, endT, reportCond, metricName, rC, groupBy = 'Channel' ):
#    result = rC.getReport( "DataOperation", 'SuceededTransfers', startT, endT,
#                          reportCond, groupBy )
#    startEpoch = Time.toEpoch( startT )
#    endEpoch = Time.toEpoch( endT )
#    if not result[ 'OK' ]:
#      return result
#    suceededData, extraData = self._sumDataBuckets( result[ 'Value' ], [ 'Failed' ] )
#    result = rC.getReport( "DataOperation", 'FailedTransfers', startT, endT,
#                          reportCond, groupBy )
#    if not result[ 'OK' ]:
#      return result
#    failedData, extraData = self._sumDataBuckets( result[ 'Value' ], [ 'Suceeded' ] )
#    okRateData = {}
#    totalFilesData = {}
#    #Calculate OK rate
#    for channel in suceededData:
#      if channel in failedData:
#        okRateData[channel] = ( suceededData[channel] / ( suceededData[channel] + failedData[channel] ) ) * 100
#    #Calculate total files transfered
#    for channel in suceededData:
#      totalFilesData[channel] = suceededData[channel]
#    for channel in failedData:
#      if channel in totalFilesData:
#        totalFilesData[channel] += failedData[channel]
#      else:
#        totalFilesData[channel] = failedData[channel]
#    finalData = []
#    if groupBy == 'Channel':
#      allSites =[ "%s -> %s" % ( source, destination ) for source in reportCond[ 'Source' ]
#                                                       for destination in reportCond[ 'Destination' ] if source != destination ]
#    else:
#      allSites = reportCond[ groupBy ]
#    #Success_rate name
#    extraURL = self._generateDataExtraURL( "SuceededTransfers", reportCond, groupBy )
#    subMetricName = "%s,success_rate" % metricName
#    self.__addWithMissing( finalData, subMetricName, allSites, okRateData, startEpoch, endEpoch, extraURL, groupBy, 100 )
#    #Failed files
#    extraURL = self._generateDataExtraURL( "FailedTransfers", reportCond, groupBy )
#    subMetricName = "%s,failed_files" % metricName
#    self.__addWithMissing( finalData, subMetricName, allSites, failedData, startEpoch, endEpoch, extraURL, groupBy, -1 )
#    #Total files
#    extraURL = self._generateDataExtraURL( "SuceededTransfers", reportCond, groupBy )
#    subMetricName = "%s,total_files" % metricName
#    self.__addWithMissing( finalData, subMetricName, allSites, totalFilesData, startEpoch, endEpoch, extraURL, groupBy, -1 )
#    return S_OK( finalData )
#
#  def __addWithMissing( self, finalData, subMetricName, allSites, data,
#                        startEpoch, endEpoch, extraURL, groupBy, max ):
#    for channel in data:
#      self._appendToFinalData( finalData, channel, subMetricName, data[ channel ], max,
#                               startEpoch, endEpoch, extraURL, groupBy  )
#    for channel in allSites:
#      if channel in data:
#        continue
#      self._appendToFinalData( finalData, channel, subMetricName, 0.0, 0.0,
#                               startEpoch, endEpoch, extraURL, groupBy  )
#    self.log.info( "[DATA]%s data" % subMetricName, "%s records" % len( finalData ) )
#
#  def _appendToFinalData( self, finalData, sites, fullMetricName, value, expected, start, end, URL, groupBy ):
#    if groupBy == "Channel":
#      sites = List.fromChar( sites, "->" )
#    elif groupBy == 'Source':
#      sites = [ sites, 'all' ]
#    elif groupBy == 'Destination':
#      sites = [ 'all', sites ]
#    finalData.append( "%s,%s,%s,%.1f,%.1f,unknown,%d,%d,%s" % ( sites[0], sites[1], fullMetricName,
#                                                                value, expected, start, end,
#                                                                URL ) )
#
#  def _dataGetThroughput( self, startT, endT, reportCond, metricName, rC, groupBy = 'Channel' ):
#    metricName = "%s,average_transfer_rate" % metricName
#    result = rC.getReport( "DataOperation", 'Throughput', startT, endT,
#                          reportCond, groupBy )
#    if not result[ 'OK' ]:
#      return result
#    startEpoch = Time.toEpoch( startT )
#    endEpoch = Time.toEpoch( endT )
#    throughtputData, extraData = self._sumDataBuckets( result[ 'Value' ], average= True )
#    finalData = []
#    extraURL = self._generateDataExtraURL( "Throughput", reportCond, groupBy  )
#    for channel in throughtputData:
#      self._appendToFinalData( finalData, channel, metricName, throughtputData[channel], -1,
#                               startEpoch, endEpoch, extraURL, groupBy  )
#    self.log.info( "[DATA]%s data" % metricName, "%s records" % len( finalData ) )
#    return S_OK( finalData )
#
#  def _dateGetTransferedBytes( self, startT, endT, reportCond, metricName, rC, groupBy = 'Channel' ):
#    metricName = "%s,transfered_bytes" % metricName
#    result = rC.getReport( "DataOperation", 'DataTransfered', startT, endT,
#                          reportCond, groupBy )
#    if not result[ 'OK' ]:
#      return result
#    startEpoch = Time.toEpoch( startT )
#    endEpoch = Time.toEpoch( endT )
#    transferedData = result[ 'Value' ][ 'data' ]
#    finalData = []
#    extraURL = self._generateDataExtraURL( "DataTransfered", reportCond, groupBy  )
#    for channel in transferedData:
#      self._appendToFinalData( finalData, channel, metricName, transferedData[channel], -1,
#                               startEpoch, endEpoch, extraURL, groupBy  )
#    self.log.info( "[DATA]%s data" % metricName, "%s records" % len( finalData ) )
#    return S_OK( finalData )
#
#  def _generateDataExtraURL( self, reportName, reportCond, groupBy = 'Channel' ):
#    baseURL = "http://lhcbweb.pic.es/DIRAC/%s/visitor/systems/accountingPlots/dataOperation" % gConfig.getValue( "/DIRAC/Setup" )
#    baseReportDesc = { '_plotName': reportName, '_grouping': groupBy, '_typeName': 'DataOperation', '_timeSelector': '86400' }
#    for key in reportCond:
#      value = reportCond[ key ]
#      if type( value ) == types.StringType:
#        baseReportDesc[ "_%s" % key ] = value
#      else:
#        baseReportDesc[ "_%s" % key ] = ",".join( value )
#    return "%s#%s" % ( baseURL, DEncode.encode( baseReportDesc ) )
#
#  def _generateRunningJobsExtraURL( self, reportName, reportCond, groupBy = 'Site' ):
#    baseURL = "http://lhcbweb.pic.es/DIRAC/%s/visitor/systems/accountingPlots/WMSHistory" % gConfig.getValue( "/DIRAC/Setup" )
#    baseReportDesc = { '_plotName': reportName, '_grouping': groupBy, '_typeName': 'DataOperation', '_timeSelector': '86400' }
#    for key in reportCond:
#      value = reportCond[ key ]
#      if type( value ) == types.StringType:
#        baseReportDesc[ "_%s" % key ] = value
#      else:
#        baseReportDesc[ "_%s" % key ] = ",".join( value )
#    return "%s#%s" % ( baseURL, DEncode.encode( baseReportDesc ) )
#
#  def _retrieveStartedJobs( self ):
#    rC = ReportsClient()
#    endT = Time.dateTime()
#    startT = endT - datetime.timedelta( seconds = self.am_getOption( "Timespan", 3600 ) )
#    reportCond = { 'Status' : [ 'Running' ] }
#    result = rC.getReport( "WMSHistory", 'NumberOfJobs', startT, endT,
#                          reportCond, 'Site' )
#
#    if not result[ 'OK' ]:
#      return result
#    acData = result[ 'Value' ][ 'data' ]
#    granularity = result[ 'Value' ][ 'granularity' ]
#    endEpoch = int( Time.toEpoch( endT ) )
#    endEpoch = endEpoch - endEpoch % granularity
#    startEpoch = int( Time.toEpoch( startT ) )
#    startEpoch = startEpoch - startEpoch % granularity
#    epochs = range( startEpoch, endEpoch + granularity, granularity )
#    siteStarted = {}
#    for site in acData:
#      siteStarted[ site ] = 0
#      siteData = acData[ site ]
#      for i in range( 0, len( epochs ) - 1):
#        nextEp = epochs[ i + 1 ]
#        curEp = epochs[ i ]
#        started = 0
#        if nextEp in siteData:
#          started = siteData[ nextEp ]
#          if curEp in siteData:
#            started = started - siteData[ curEp ]
#          started = max( 0, started )
#        siteStarted[ site ] += started
#      if siteStarted[ site ] == 0:
#        del( siteStarted[ site ] )
#    cvsLines = []
#    sites = siteStarted.keys()
#    sites.sort()
#    url = self._generateRunningJobsExtraURL( 'NumberOfJobs', reportCond )
#    for site in sites:
#      startedJobs = siteStarted[ site ]
#      gridName = site[ :site.find(".") ]
#      site = gConfig.getValue( "/Resources/Sites/%s/%s/Name" %( gridName, site ), site )
#      cvsLines.append( "%s,job_processing,started_jobs,%d,-1,unknown,%d,%d,%s" % ( site, startedJobs,
#                                                                                startEpoch, endEpoch,
#                                                                                url ) )
#    return S_OK( cvsLines )
#
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
