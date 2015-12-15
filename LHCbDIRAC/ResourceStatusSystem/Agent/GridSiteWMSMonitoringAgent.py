''' LHCbDIRAC.ResourceStatusSystem.Agent.GridSiteWMSMonitoringAgent
 
   GridSiteWMSMonitoringAgent.__bases__:
     DIRAC.Core.Base.AgentModule.AgentModule
     
'''

import datetime
import os
import re
import tempfile
import time

from DIRAC                                               import S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Base.AgentModule                         import AgentModule
from DIRAC.DataManagementSystem.Client.DataManager       import DataManager
from LHCbDIRAC.WorkloadManagementSystem.DB.JobDB         import JobDB
from DIRAC.Core.Utilities                                import Time

MC_JOB_TYPES   = [ 'normal', '^MC' ]
DATA_JOB_TYPES = [ '^Data' ]
USER_JOB_TYPES = [ 'user' ]
SAM_JOB_TYPES = [ 'sam', 'test' ]

__RCSID__  = '$Id$'
AGENT_NAME = 'ResourceStatusSystem/GridSiteWMSMonitoringAgent'

class GridSiteWMSMonitoringAgent( AgentModule ):
  '''
    Extracts information on the current Grid activity from the DIRAC WMS and 
    publishes it to the web
  
    GridSiteWMSMonitoringAgent, extracts information on the current Grid activity 
    from the DIRAC WMS and publishes it to the web 
  '''
  
  __hrefPrefix = 'http://lhcbweb.pic.es/DIRAC/LHCb-Production/anonymous/systems/accountingPlots/WMSHistory' 
  
  def __init__( self, *args, **kwargs ):
    
    AgentModule.__init__( self, *args, **kwargs )
  
    # Members initialization
  
    self.hrefPrefix = self.__hrefPrefix
    
    self.siteGOCNameDict = {}
    self.lastUpdateTime  = 0

    self.jobDB           = None
    self.opHelper        = None

  def initialize( self ):
    '''
      Initialize the agent.
    '''
    
    self.hrefPrefix      = self.am_getOption( 'SiteHrefPrefix', self.hrefPrefix )
    
    self.jobDB           = JobDB()
    self.opHelper        = Operations()   
        
    return S_OK()

  def execute( self ):
    '''
      Main execute method
    '''

    # Get the site mask
    siteMask = []
    result = self.jobDB.getSiteMask( 'Banned' )
    if result[ 'OK' ]:
      siteMask = result[ 'Value' ]

    elapsedTime        = time.time() - self.lastUpdateTime
    generationInterval = self.am_getOption( 'GenerationInterval', 1800 )
    if elapsedTime < generationInterval:
      return S_OK()
    result = self._getWMSData()
    if not result[ 'OK' ]:
      return result

    fileContents = ''
    hrefTemp = self.hrefPrefix+'#ds9:_plotNames12:NumberOfJobss13:_timeSelectors5:86400s7:_Statuss7:Runnings5:_Sites%d:%ss9:_typeNames10:WMSHistorys9:_groupings4:Sitee'
    for site, sDict in result[ 'Value' ].items():
      parallel_jobs                   = 0
      completed_jobs                  = 0
      successfully_completed_jobs     = 0
      completed_jobs_24h              = 0
      successfully_completed_jobs_24h = 0
      CPU_time                        = 0
      wall_time                       = 0
      diracName                       = sDict[ 'DIRACName' ]
      banned                          = False
      if diracName in siteMask:
        banned = True
      del sDict[ 'DIRACName' ]
      href = hrefTemp % ( len( diracName ), diracName )
      for activity, aDict in result[ 'Value' ][ site ].items():

        parallel_jobs += aDict[ 'Running' ]
        lTuple = ( site, activity, aDict[ 'Running' ], int( time.time() ) - 3600, int( time.time() ), href )
        line = '%s,%s,parallel_jobs,%d,-1,unknown,%d,%d,%s ' % lTuple
        fileContents += line + '\n'

        # Evaluate success rate and site status
        if aDict[ 'FinishedTotal' ] > 0:
          success_rate = float( aDict[ 'FinishedSuccessful' ] ) / float( aDict[ 'FinishedTotal' ] ) * 100.
        else:
          success_rate = 0.

        # Evaluate the site status out of the success rate
        if banned:
          site_status = 'banned'
        elif aDict[ 'FinishedTotal' ] < 10:
          site_status = 'unknown'
        elif success_rate > 90.0:
          site_status = 'good'
        elif success_rate > 80.0:
          site_status = 'fair'
        elif success_rate > 50.0:
          site_status = 'degraded'
        else:
          site_status = 'bad'

        completed_jobs += aDict[ 'FinishedTotal' ]
        lTuple = ( site, activity, aDict[ 'FinishedTotal' ], site_status, int( time.time() ) - 3600 , int( time.time() ), href )
        line   = '%s,%s,completed_jobs,%d,-1,%s,%d,%d,%s ' % lTuple
        fileContents += line + '\n'
        
        successfully_completed_jobs += aDict[ 'FinishedSuccessful' ]
        lTuple = ( site, activity, aDict[ 'FinishedSuccessful' ], int( time.time() ) - 3600, int( time.time() ), href )
        line   = '%s,%s,successfully_completed_jobs,%d,-1,unknown,%d,%d,%s ' % lTuple
        fileContents += line + '\n'

        # Evaluate success rate and site status
        if aDict[ 'FinishedTotal24' ] > 0:
          success_rate = float( aDict[ 'FinishedSuccessful24' ] ) / float( aDict[ 'FinishedTotal24' ] )*100.
        else:
          success_rate = 0.

        # Evaluate the site status out of the success rate
        if banned:
          site_status = 'banned'
        elif aDict[ 'FinishedTotal24' ] < 10:
          site_status = 'idle'
        elif success_rate > 90.0:
          site_status = 'good'
        elif success_rate > 80.0:
          site_status = 'fair'
        elif success_rate > 50.0:
          site_status = 'degraded'
        else:
          site_status = 'bad'

        completed_jobs_24h += aDict[ 'FinishedTotal24' ]
        lTuple = ( site, activity, aDict[ 'FinishedTotal24' ], site_status, int( time.time() ) - 86400, int( time.time() ), href )
        line   = '%s,%s,completed_jobs_24h,%d,-1,%s,%d,%d,%s ' % lTuple
        fileContents += line + '\n'
        
        successfully_completed_jobs_24h += aDict[ 'FinishedSuccessful24' ]
        lTuple = ( site, activity, aDict[ 'FinishedSuccessful24' ], int( time.time() ) - 86400, int( time.time() ), href )
        line   = '%s,%s,successfully_completed_jobs_24h,%d,-1,unknown,%d,%d,%s ' % lTuple
        fileContents += line + '\n'
        
        CPU_time += aDict[ 'CPUTime' ]
        lTuple = ( site, activity, aDict[ 'CPUTime' ], int( time.time() ) - 3600, int( time.time() ), href )
        line   = '%s,%s,CPU_time,%d,-1,unknown,%d,%d,%s ' % lTuple
        fileContents += line + '\n'
        
        wall_time += aDict[ 'WallClockTime' ]
        lTuple = ( site, activity, aDict[ 'WallClockTime' ], int( time.time() ) - 3600, int( time.time() ), href )
        line   = '%s,%s,wall_time,%d,-1,unknown,%d,%d,%s ' % lTuple
        fileContents += line + '\n'

      lTuple = ( site, 'job_processing', parallel_jobs, int( time.time() ) - 3600, int( time.time() ), href )
      line   = '%s,%s,parallel_jobs,%d,-1,unknown,%d,%d,%s ' % lTuple
      fileContents += line + '\n'

      # Evaluate success rate and site status
      if completed_jobs > 0:
        success_rate = float( successfully_completed_jobs ) / float( completed_jobs ) * 100.
      else:
        success_rate = 0.

      # Evaluate the site status out of the success rate
      if banned:
        site_status = 'banned'
      elif completed_jobs < 10:
        site_status = 'idle'
      elif success_rate > 90.0:
        site_status = 'good'
      elif success_rate > 80.0:
        site_status = 'fair'
      elif success_rate > 50.0:
        site_status = 'degraded'
      else:
        site_status = 'bad'

      lTuple = ( site, 'job_processing', completed_jobs, site_status, int( time.time() ) - 3600, int( time.time() ), href )
      line   = '%s,%s,completed_jobs,%d,-1,%s,%d,%d,%s ' % lTuple
      fileContents += line + '\n'
      
      lTuple = ( site, 'job_processing', successfully_completed_jobs, int( time.time() ) - 3600, int( time.time() ), href )
      line   = '%s,%s,successfully_completed_jobs,%d,-1,unknown,%d,%d,%s ' % lTuple
      fileContents += line + '\n'

      # Evaluate success rate and site status
      if completed_jobs_24h > 0:
        success_rate = float( successfully_completed_jobs_24h ) / float( completed_jobs_24h ) * 100.
      else:
        success_rate = 0.

      # Evaluate the site status out of the success rate
      if banned:
        site_status = 'banned'
      elif completed_jobs_24h < 10:
        site_status = 'idle'
      elif success_rate > 90.0:
        site_status = 'good'
      elif success_rate > 80.0:
        site_status = 'fair'
      elif success_rate > 50.0:
        site_status = 'degraded'
      else:
        site_status = 'bad'

      lTuple = (site,'job_processing',completed_jobs_24h,site_status,int(time.time())-86400,int(time.time()),href)
      line = '%s,%s,completed_jobs_24h,%d,-1,%s,%d,%d,%s ' % lTuple
      fileContents += line+'\n'
      
      lTuple = ( site, 'job_processing', successfully_completed_jobs_24h, int( time.time() ) - 86400, int( time.time() ), href )
      line   = '%s,%s,successfully_completed_jobs_24h,%d,-1,unknown,%d,%d,%s ' % lTuple
      fileContents += line + '\n'
      
      lTuple = ( site, 'job_processing', CPU_time, int( time.time() ) - 3600, int( time.time() ), href )
      line   = '%s,%s,CPU_time,%d,-1,unknown,%d,%d,%s ' % lTuple
      fileContents += line + '\n'
      
      lTuple = ( site, 'job_processing', wall_time, int( time.time() ) - 3600, int( time.time() ), href )
      line   = '%s,%s,wall_time,%d,-1,unknown,%d,%d,%s ' % lTuple
      fileContents += line + '\n'

    self.lastUpdateTime = time.time()
    result = self._commitFileContents( fileContents )
    if result[ 'OK' ]:
      self.log.info( 'Successfully sent monitoring data' )
      self.log.verbose( fileContents )
    else:
      self.log.error( 'Failed to send SiteMap monitoring data' )

    return result

  def _commitFileContents( self, fileData ):
    fd, fName = tempfile.mkstemp()
    os.write( fd, fileData )
    os.close( fd )
    dm = DataManager()
    result = dm.put( "/lhcb/monitoring/lhcb.siteWMSmonitoring.csv", fName, 'LogSE' )
    try:
      os.unlink( fName )
    except Exception as e:
      self.log.error( "Can't unlink temporal file", "%s: %s" % ( fName, str(e) ) )
    return result

  def _getSiteGOCNameMapping( self ):
    """ Get DIRAC Site name to GOC Site name mapping
    """

    result = self.opHelper.getSections( 'Sites/LCG' )
    if not result[ 'OK' ]:
      return result

    for site in result[ 'Value' ]:
      gocName = self.opHelper.getValue( 'Sites/LCG/%s/Name' % site, 'Unknown' )
      self.siteGOCNameDict[ site ] = gocName

  def __getGOCName( self, site ):
    """ Get GOC site name
    """

    try:
      gocName = self.siteGOCNameDict[ site ]
      return gocName
    except KeyError:
      return site

  def __getJobType( self, jType ):
    """ Get standard monitoring job type
    """

    for pattern in MC_JOB_TYPES:
      if re.search( pattern, jType ):
        return 'mc_production'
    for pattern in DATA_JOB_TYPES:
      if re.search( pattern, jType ):
        return 'data_reconstruction'
    for pattern in USER_JOB_TYPES:
      if re.search( pattern, jType ):
        return 'user_analysis'
    for pattern in SAM_JOB_TYPES:
      if re.search( pattern, jType ):
        return 'SAM_monitoring'

    return 'mc_production'

  def _getWMSData( self ):
    """ Get the job data from the WMS
    """

    self._getSiteGOCNameMapping()

    monDict = {}
    result = self.jobDB.getDistinctJobAttributes( 'Site' )
    if not result[ 'OK' ]:
      return result

    for siteDIRAC in result[ 'Value' ]+[ 'ANY' ]:
      site = self.__getGOCName( siteDIRAC )
      monDict[ site ] = {}
      for jobType in [ 'mc_production', 'data_reconstruction', 'user_analysis', 'SAM_monitoring' ]:
        monDict[ site ][ jobType ]                           = {}
        monDict[ site ][ jobType ][ 'Running' ]              = 0
        monDict[ site ][ jobType ][ 'FinishedTotal' ]        = 0
        monDict[ site ][ jobType ][ 'FinishedSuccessful' ]   = 0
        monDict[ site ][ jobType ][ 'FinishedTotal24' ]      = 0
        monDict[ site ][ jobType ][ 'FinishedSuccessful24' ] = 0
        monDict[ site ][ jobType ][ 'CPUTime' ]              = 0
        monDict[ site ][ jobType ][ 'WallClockTime' ]        = 0
        
        monDict[ site ][ 'DIRACName' ] = siteDIRAC

    # Currently running jobs
    result = self.jobDB.getCounters( 'Jobs', [ 'Site', 'JobType' ],
                                    { 'Status' : 'Running' }, None )
    if not result[ 'OK' ]:
      return result
    for siteTuple in result[ 'Value' ]:
      siteDict, count = siteTuple
      site = self.__getGOCName( siteDict[ 'Site' ] )
      if site:
        jobType = self.__getJobType( siteDict[ 'JobType' ] )
        monDict[ site ][ jobType ][ 'Running' ] += count

    # Jobs finished in the last hour
    dt = Time.dateTime() - datetime.timedelta( seconds = 3600 )
    monDict = self.__getFinishedJobs( dt, monDict )
    # Jobs finished in the last 24 hours
    dt = Time.dateTime() - datetime.timedelta( hours = 24 )
    monDict = self.__getFinishedJobs( dt, monDict, '24' )

    return S_OK( monDict )

  def __getFinishedJobs( self, date, monDict, interval = '' ):

    result = self.jobDB.getCounters( 'Jobs', [ 'Site', 'JobType' ],
                                    { 'Status' : [ 'Done' , 'Completed' , 'Failed' , 'Killed' ] },
                                    newer = str( date ), timeStamp = 'EndExecTime' )
    if not result[ 'OK' ]:
      return result
    for siteTuple in result[ 'Value' ]:
      siteDict, count = siteTuple
      site            = self.__getGOCName( siteDict[ 'Site' ] )
      if site:
        jobType = self.__getJobType( siteDict[ 'JobType' ] )
        monDict[ site ][ jobType ][ 'FinishedTotal' + interval ] += count
        if not interval:
          result = self.jobDB.getTimings( siteDict[ 'Site' ] )
          if result[ 'OK' ]:
            if not monDict[ site ][ jobType ].has_key( 'CPUTime' ):
              monDict[ site ][ jobType ][ 'CPUTime' ] = result[ 'Value' ][ 'CPUTime' ]
            else:
              monDict[ site ][ jobType ][ 'CPUTime' ] += result[ 'Value' ][ 'CPUTime' ]
            if not monDict[ site ][ jobType ].has_key( 'WallClockTime' ):
              monDict[ site ][ jobType ][ 'WallClockTime' ] = result[ 'Value' ][ 'WallClockTime' ]
            else:
              monDict[ site ][ jobType ][ 'WallClockTime' ] += result[ 'Value' ][ 'WallClockTime' ]

    # Successful jobs in the last hour
    result = self.jobDB.getCounters( 'Jobs', [ 'Site', 'JobType' ],
                                     { 'Status' : [ 'Done', 'Completed' ] },
                                     newer = str( date ), timeStamp = 'EndExecTime' )
    if not result[ 'OK' ]:
      return result
    for siteTuple in result[ 'Value' ]:
      siteDict, count = siteTuple
      site            = self.__getGOCName( siteDict[ 'Site' ] )
      if site:
        jobType = self.__getJobType( siteDict[ 'JobType' ] )
        monDict[ site ][ jobType ][ 'FinishedSuccessful' + interval ] += count

    return monDict

#...............................................................................
#EOF
