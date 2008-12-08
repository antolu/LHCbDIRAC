# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Agent/GridSiteWMSMonitoringAgent.py,v 1.3 2008/12/08 09:38:47 atsareg Exp $


'''
GridSiteWMSMonitoringAgent extracts information on the current Grid activity from the DIRAC WMS
and publishes it to the web
'''

from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.Agent import Agent
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.WorkloadManagementSystem.DB.JobDB       import JobDB
from DIRAC.Core.Utilities import Time

import sys, os, re
import tempfile
import time
import datetime
import types

MC_JOB_TYPES = ['normal','^MC']
DATA_JOB_TYPES = ['^Data']
USER_JOB_TYPES = ['user']
SAM_JOB_TYPES = ['sam']

AGENT_NAME = "LHCb/GridSiteWMSMonitoringAgent"

class GridSiteWMSMonitoringAgent(Agent):
  def __init__( self ):
    Agent.__init__( self, AGENT_NAME )

  def initialize( self ):
    result = Agent.initialize( self )
    if not result[ 'OK' ]:
      return result
    self._lastUpdateTime = 0
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',120)
    self.jobDB = JobDB()
    self.siteGOCNameDict = {}
    return S_OK()

  def execute( self ):
    elapsedTime = time.time() - self._lastUpdateTime
    if elapsedTime < gConfig.getValue( "%s/GenerationInterval" % self.section, 1800 ):
      return S_OK()
    result = self._getWMSData()
    if not result[ 'OK' ]:
      return result

    fileContents = ''
    href = 'http://lhcbweb.pic.es/DIRAC/LHCb-Production/anonymous/systems/accountingPlots/WMSHistory#ds9:_plotNames12:NumberOfJobss13:_timeSelectors5:86400s7:_Statuss7:Runnings9:_typeNames10:WMSHistorys9:_groupings4:Sitee'
    for site,sDict in result['Value'].items():
      parallel_jobs = 0
      completed_jobs = 0
      successfully_completed_jobs = 0
      CPU_time = 0
      wall_time = 0
      for activity,aDict in result['Value'][site].items():
        parallel_jobs += aDict['Running']
        lTuple = (site,activity,aDict['Running'],int(time.time())-3600,int(time.time()),href)
        line = '%s,%s,parallel_jobs,%d,-1,unknown,%d,%d,%s ' % lTuple
        fileContents += line+'\n'
        completed_jobs += aDict['FinishedTotal']
        lTuple = (site,activity,aDict['FinishedTotal'],int(time.time())-3600,int(time.time()),href)
        line = '%s,%s,completed_jobs,%d,-1,unknown,%d,%d,%s ' % lTuple
        fileContents += line+'\n'
        successfully_completed_jobs += aDict['FinishedSuccessful']
        lTuple =  (site,activity,aDict['FinishedSuccessful'],int(time.time())-3600,int(time.time()),href)
        line = '%s,%s,successfully_completed_jobs,%d,-1,unknown,%d,%d,%s ' % lTuple
        fileContents += line+'\n'
        CPU_time += aDict['CPUTime']
        lTuple =  (site,activity,aDict['CPUTime'],int(time.time())-3600,int(time.time()),href)
        line = '%s,%s,CPU_time,%d,-1,unknown,%d,%d,%s ' % lTuple
        fileContents += line+'\n'
        wall_time += aDict['WallClockTime']
        lTuple =  (site,activity,aDict['WallClockTime'],int(time.time())-3600,int(time.time()),href)
        line = '%s,%s,wall_time,%d,-1,unknown,%d,%d,%s ' % lTuple
        fileContents += line+'\n'
      lTuple = (site,'overall_activity',parallel_jobs,int(time.time())-3600,int(time.time()),href)
      line = '%s,%s,parallel_jobs,%d,-1,unknown,%d,%d,%s ' % lTuple
      fileContents += line+'\n'
      lTuple = (site,'overall_activity',completed_jobs,int(time.time())-3600,int(time.time()),href)
      line = '%s,%s,completed_jobs,%d,-1,unknown,%d,%d,%s ' % lTuple
      fileContents += line+'\n'
      lTuple = (site,'overall_activity',successfully_completed_jobs,int(time.time())-3600,int(time.time()),href)
      line = '%s,%s,parallel_jobs,%d,-1,unknown,%d,%d,%s ' % lTuple
      fileContents += line+'\n'
      lTuple = (site,'overall_activity',CPU_time,int(time.time())-3600,int(time.time()),href)
      line = '%s,%s,CPU_time,%d,-1,unknown,%d,%d,%s ' % lTuple
      fileContents += line+'\n'
      lTuple = (site,'overall_activity',wall_time,int(time.time())-3600,int(time.time()),href)
      line = '%s,%s,wall_time,%d,-1,unknown,%d,%d,%s ' % lTuple
      fileContents += line+'\n'
    self._lastUpdateTime = time.time()
    result = self._commitFileContents( fileContents )
    if result['OK']:
      gLogger.info('Successfully sent monitoring data')
      gLogger.verbose(fileContents)
    else:
      gLogger.error('Failed to send SiteMap monitoring data')

    return result

  def _commitFileContents( self, fileData ):
    fd, fName = tempfile.mkstemp()
    os.write( fd, fileData )
    os.close( fd )
    rm = ReplicaManager()
    result = rm.put( "/lhcb/monitoring/lhcb.siteWMSmonitoring.csv", fName, 'LogSE' )
    try:
      os.unlink( fName )
    except Exception, e:
      gLogger.error( "Can't unlink temporal file", "%s: %s" % ( fName, str(e) ) )
    return result

  def _getSiteGOCNameMapping(self):
    """ Get DIRAC Site name to GOC Site name mapping
    """

    result = gConfig.getSections('/Resources/Sites/LCG')
    if not result['OK']:
      return result

    for site in result['Value']:
      gocName = gConfig.getValue('/Resources/Sites/LCG/%s/Name' % site,'Unknown')
      self.siteGOCNameDict[site] = gocName

  def __getGOCName(self,site):
    """ Get GOC site name
    """

    try:
      gocName = self.siteGOCNameDict[site]
      return gocName
    except KeyError:
      return ''

  def __getJobType(self,jType):
    """ Get standard monitoring job type
    """

    for pattern in MC_JOB_TYPES:
      if re.search(pattern,jType):
        return 'mc_production'
    for pattern in DATA_JOB_TYPES:
      if re.search(pattern,jType):
        return 'data_reconstruction'
    for pattern in USER_JOB_TYPES:
      if re.search(pattern,jType):
        return 'user_analysis'
    for pattern in SAM_JOB_TYPES:
      if re.search(pattern,jType):
        return 'SAM_monitoring'

    return 'mc_production'

  def _getWMSData(self):
    """ Get the job data from the WMS
    """

    self._getSiteGOCNameMapping()

    monDict = {}

    # Currently running jobs
    result = self.jobDB.getCounters('Jobs',['Site','JobType'],{'Status':'Running'},None)
    if not result['OK']:
      return result
    for siteTuple in result['Value']:
      siteDict,count = siteTuple
      site = self.__getGOCName(siteDict['Site'])
      if site:
        jobType = self.__getJobType(siteDict['JobType'])
        if site not in monDict.keys():
          monDict[site] = {}
        if jobType not in monDict[site].keys():
          monDict[site][jobType] = {}
          monDict[site][jobType]['Running'] = 0
          monDict[site][jobType]['FinishedTotal'] = 0
          monDict[site][jobType]['FinishedSuccessful'] = 0
          monDict[site][jobType]['CPUTime'] = 0
          monDict[site][jobType]['WallClockTime'] = 0
        if not monDict[site][jobType].has_key('Running'):
          monDict[site][jobType]['Running'] = count
        else:
          monDict[site][jobType]['Running'] += count

    # Jobs finished in the last hour
    dt = Time.dateTime() - datetime.timedelta(seconds=3600)
    result = self.jobDB.getCounters('Jobs',['Site','JobType'],{'Status':['Done','Completed','Failed','Killed']},str(dt),timeStamp='EndExecTime')
    if not result['OK']:
      return result
    for siteTuple in result['Value']:
      siteDict,count = siteTuple
      site = self.__getGOCName(siteDict['Site'])
      if site:
        jobType = self.__getJobType(siteDict['JobType'])
        if site not in monDict.keys():
          monDict[site] = {}
        if jobType not in monDict[site].keys():
          monDict[site][jobType] = {}
          monDict[site][jobType]['Running'] = 0
          monDict[site][jobType]['FinishedTotal'] = 0
          monDict[site][jobType]['FinishedSuccessful'] = 0
          monDict[site][jobType]['CPUTime'] = 0
          monDict[site][jobType]['WallClockTime'] = 0
        if not monDict[site][jobType].has_key('FinishedTotal'):
          monDict[site][jobType]['FinishedTotal'] = count
        else:
          monDict[site][jobType]['FinishedTotal'] += count
        result = self.jobDB.getTimings(siteDict['Site'])
        #result = S_ERROR()
        if result['OK']:
          if not monDict[site][jobType].has_key('CPUTime'):
            monDict[site][jobType]['CPUTime'] = result['Value']['CPUTime']
          else:
            monDict[site][jobType]['CPUTime'] += result['Value']['CPUTime']
          if not monDict[site][jobType].has_key('WallClockTime'):
            monDict[site][jobType]['WallClockTime'] = result['Value']['WallClockTime']
          else:
            monDict[site][jobType]['WallClockTime'] += result['Value']['WallClockTime']

    # Successful jobs in the last hour
    result = self.jobDB.getCounters('Jobs',['Site','JobType'],{'Status':['Done','Completed']},str(dt),timeStamp='EndExecTime')
    if not result['OK']:
      return result
    for siteTuple in result['Value']:
      siteDict,count = siteTuple
      site = self.__getGOCName(siteDict['Site'])
      if site:
        jobType = self.__getJobType(siteDict['JobType'])
        if site not in monDict.keys():
          monDict[site] = {}
        if jobType not in monDict[site].keys():
          monDict[site][jobType] = {}
          monDict[site][jobType]['Running'] = 0
          monDict[site][jobType]['FinishedTotal'] = 0
          monDict[site][jobType]['FinishedSuccessful'] = 0
          monDict[site][jobType]['CPUTime'] = 0
          monDict[site][jobType]['WallClockTime'] = 0
        if not monDict[site][jobType].has_key('FinishedSuccessful'):
          monDict[site][jobType]['FinishedSuccessful'] = count
        else:
          monDict[site][jobType]['FinishedSuccessful'] += count

    return S_OK(monDict)