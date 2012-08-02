########################################################################
# $HeadURL$
# File :   DiracProduction.py
# Author : Stuart Paterson
########################################################################

from DIRAC.Core.Base import Script
Script.parseCommandLine()

"""DIRAC Production Management Class

   This class allows to monitor the progress of productions operationally.

   Of particular use are the monitoring functions allowing to drill down
   by site, minor status and application status for a given transformation.

"""

__RCSID__ = "$Id$"

import string, os, types
import pprint

from DIRAC.Interfaces.API.Dirac                               import Dirac
from DIRAC.Core.DISET.RPCClient                               import RPCClient
from DIRAC.Core.Utilities.Time                                import toString
from LHCbDIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient

from DIRAC                                          import gConfig, gLogger, S_OK, S_ERROR

COMPONENT_NAME = 'DiracProduction'

class DiracProduction:

  #############################################################################
  def __init__( self ):
    """Instantiates the Workflow object and some default parameters.
    """
    self.log = gLogger.getSubLogger( COMPONENT_NAME )
    #gLogger.setLevel('verbose')
    self.section = COMPONENT_NAME
    self.scratchDir = gConfig.getValue( '/LocalSite/ScratchDir', '/tmp' )
    self.scratchDir = gConfig.getValue( '/LocalSite/ScratchDir', '/tmp' )
    self.submittedStatus = gConfig.getValue( self.section + '/ProcDBSubStatus', 'Submitted' )
    self.createdStatus = gConfig.getValue( self.section + '/ProcDBCreatedStatus', 'Created' )
    self.defaultOwnerGroup = gConfig.getValue( self.section + '/DefaultOwnerGroup', 'lhcb_prod' )
    self.diracAPI = Dirac()
    self.pPrint = pprint.PrettyPrinter()
    self.toCleanUp = []
    self.prodHeaders = {'AgentType':'SubmissionMode', 'Status':'Status', 'CreationDate':'Created', \
                        'TransformationName':'Name', 'Type':'Type'}
    self.prodAdj = 22
    self.proxy = None
    self.commands = {'start':['Active', 'Manual'],
                     'stop':['Stopped', 'Manual'],
                     'automatic':['Active', 'Automatic'],
                     'manual':['Active', 'Manual'],
                     'completed':['Completed', 'Manual'],
                     'completing':['Completing', 'Automatic'],
                     'cleaning':['Cleaning', 'Manual'],
                     'flush':['Flush', 'Automatic'],
                     'deleted':['Deleted', 'Manual'],
                     'archived':['Archived', 'Manual'],
                     'valinput':['ValidatingInput', 'Manual'],
                     'valoutput':['ValidatingOutput', 'Manual'],
                     'remove':['RemovingFiles', 'Manual'],
                     'validated':['ValidatedOutput', 'Manual'],
                     'removed':['RemovedFiles', 'Manual']}
    self.prodClient = TransformationClient()

  #############################################################################
  def getAllProductions( self, printOutput = False ):
    """Returns a dictionary of production IDs and metadata. If printOutput is
       specified, a high-level summary of the productions is printed.
    """
    result = self.prodClient.getTransformationSummary()
    if not result['OK']:
      return result

    if not printOutput:
      return result

    adj = self.prodAdj
    headers = self.prodHeaders.values()
    prodDict = result['Value']
    top = ''
    for i in headers:
      top += i.ljust( adj )
    message = ['ProductionID'.ljust( adj ) + top + '\n']
    for prodID, params in prodDict.items():
      line = str( prodID ).ljust( adj )
      for key, name in self.prodHeaders.items():
        for n, v in params.items():
          if n == key:
            line += str( v ).ljust( adj )
      message.append( line )

    print string.join( message, '\n' )
    return result

  #############################################################################
  def getProduction( self, productionID, printOutput = False ):
    """Returns the metadata associated with a given production ID. Protects against
       LFN: being prepended and different types of production ID.
    """
    if type( productionID ) == type( 2 ):
      productionID = int( productionID )
    if not type( productionID ) == type( long( 1 ) ):
      if not type( productionID ) == type( " " ):
        return self.__errorReport( 'Expected string, long or int for production ID' )

    result = self.prodClient.getTransformation( int( productionID ) )
    if not result['OK']:
      return result

    #to fix TODO
    if printOutput:
      adj = self.prodAdj
      prodInfo = result['Value']
      headers = self.prodHeaders.values()
      top = ''
      for i in headers:
        top += i.ljust( adj )
      message = ['ProductionID'.ljust( adj ) + top + '\n']
      #very painful to make this consistent, better improved first on the server side
      message.append( productionID.ljust( adj ) + prodInfo['Status'].ljust( adj ) + prodInfo['Type'].ljust( adj ) + prodInfo['AgentType'].ljust( adj ) + toString( prodInfo['CreationDate'] ).ljust( adj ) + prodInfo['TransformationName'].ljust( adj ) )
      print string.join( message, '\n' )
    return S_OK( result['Value'] )

  #############################################################################
  def getActiveProductions( self, printOutput = False ):
    """Returns a dictionary of active production IDs and their status, e.g. automatic, manual.
    """
    result = self.prodClient.getTransformations()
    if not result['OK']:
      return result
    prodList = result['Value']
    currentProductions = {}
    for prodDict in prodList:
      self.log.debug( prodDict )
      if prodDict.has_key( 'AgentType' ) and prodDict.has_key( 'TransformationID' ):
        prodID = prodDict['TransformationID']
        status = prodDict['AgentType']
        currentProductions[prodID] = status
        if status.lower() == 'automatic':
          self.log.verbose( 'Found active production %s eligible to submit jobs' % prodID )

    if printOutput:
      self._prettyPrint( currentProductions )

    return S_OK( currentProductions )

  #############################################################################
  def getProductionLoggingInfo( self, productionID, printOutput = False ):
    """The logging information for the given production is returned.  This includes
       the operation performed, any messages associated with the operation and the
       DN of the production manager performing it.
    """
    if type( productionID ) == type( 2 ):
      productionID = long( productionID )
    if not type( productionID ) == type( long( 1 ) ):
      if not type( productionID ) == type( " " ):
        return self.__errorReport( 'Expected string, long or int for production ID' )

    result = self.prodClient.getTransformationLogging( long( productionID ) )
    if not result['OK']:
      self.log.warn( 'Could not get transformation logging information for productionID %s' % ( productionID ) )
      return result
    if not result['Value']:
      self.log.warn( 'No logging information found for productionID %s' % ( productionID ) )
      return S_ERROR( 'No logging info found' )

    if not printOutput:
      return result

    message = ['ProdID'.ljust( int( 0.5 * self.prodAdj ) ) + 'Message'.ljust( 3 * self.prodAdj ) + 'DateTime [UTC]'.ljust( self.prodAdj ) + 'AuthorCN'.ljust( 2 * self.prodAdj )]
    for line in result['Value']:
      message.append( str( line['TransformationID'] ).ljust( int( 0.5 * self.prodAdj ) ) + line['Message'].ljust( 3 * self.prodAdj ) + toString( line['MessageDate'] ).ljust( self.prodAdj ) + line['AuthorDN'].split( '/' )[-1].ljust( 2 * self.prodAdj ) )

    print '\nLogging summary for productionID ' + str( productionID ) + '\n\n' + string.join( message, '\n' )

    return result

  #############################################################################
  def getProductionSummary( self, productionID = None, printOutput = False ):
    """Returns a detailed summary for the productions in the system. If production ID is
       specified, the result is restricted to this value. If printOutput is specified,
       the result is printed to the screen.
    """
    if type( productionID ) == type( 2 ):
      productionID = long( productionID )
    if productionID:
      if not type( productionID ) == type( long( 1 ) ):
        if not type( productionID ) == type( " " ):
          return self.__errorReport( 'Expected string or long for production ID' )

    result = self.prodClient.getTransformationSummary()
    if not result['OK']:
      return result

    if productionID:
      if result['Value'].has_key( long( productionID ) ):
        newResult = S_OK()
        newResult['Value'] = {}
        newResult['Value'][long( productionID )] = result['Value'][long( productionID )]
        result = newResult
      else:
        prods = result['Value'].keys()
        self.log.info( 'Specified productionID was not found, the list of active productions is:\n%s' % ( prods ) )
        return S_ERROR( 'Production ID %s was not found' % ( productionID ) )

    if printOutput:
      self._prettyPrint( result['Value'] )

    return result

  #############################################################################
  def getProductionApplicationSummary( self, productionID, status = None, minorStatus = None, printOutput = False ):
    """Returns an application status summary for the productions in the system. If printOutput is
       specified, the result is printed to the screen.  This queries the WMS
       for the given productionID and provides an up-to-date snapshot of the application status
       combinations and associated WMS JobIDs.
    """
    if type( productionID ) == type( 2 ):
      productionID = long( productionID )
    if not type( productionID ) == type( long( 1 ) ):
      if not type( productionID ) == type( " " ):
        return self.__errorReport( 'Expected string, long or int for production ID' )

    statusDict = self.getProdJobMetadata( productionID, status, minorStatus )
    if not statusDict['OK']:
      self.log.warn( 'Could not get production metadata information' )
      return statusDict

    jobIDs = statusDict['Value'].keys()
    if not jobIDs:
      return S_ERROR( 'No JobIDs with matching conditions found' )

    self.log.verbose( 'Considering %s jobs with selected conditions' % ( len( jobIDs ) ) )
    #now need to get the application status information
    monClient = RPCClient( 'WorkloadManagement/JobMonitoring', timeout = 120 )
    result = monClient.getJobsApplicationStatus( jobIDs )
    if not result['OK']:
      self.log.warn( 'Could not get application status for jobs list' )
      return result

    appStatus = result['Value']
#    self._prettyPrint(appStatus)
#    self._prettyPrint(statusDict['Value'])
    #Now format the result.
    summary = {}
    submittedJobs = 0
    doneJobs = 0
    for job, atts in statusDict['Value'].items():
      for key, val in atts.items():
        if key == 'Status':
          uniqueStatus = val.capitalize()
          if not summary.has_key( uniqueStatus ):
            summary[uniqueStatus] = {}
          if not summary[uniqueStatus].has_key( atts['MinorStatus'] ):
            summary[uniqueStatus][atts['MinorStatus']] = {}
          if not summary[uniqueStatus][atts['MinorStatus']].has_key( appStatus[job]['ApplicationStatus'] ):
            summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']] = {}
            summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']]['Total'] = 1
            submittedJobs += 1
            if uniqueStatus == 'Done':
              doneJobs += 1
            summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']]['JobList'] = [job]
          else:
            if not summary[uniqueStatus][atts['MinorStatus']].has_key( appStatus[job]['ApplicationStatus'] ):
              summary[uniqueStatus][atts['MinorStatus']] = {}
              summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']] = {}
              summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']]['Total'] = 1
              submittedJobs += 1
              if uniqueStatus == 'Done':
                doneJobs += 1
              summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']]['JobList'] = [job]
            else:
              current = summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']]['Total']
              summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']]['Total'] = current + 1
              submittedJobs += 1
              if uniqueStatus == 'Done':
                doneJobs += 1
              jobList = summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']]['JobList']
              jobList.append( job )
              summary[uniqueStatus][atts['MinorStatus']][appStatus[job]['ApplicationStatus']]['JobList'] = jobList

    if not printOutput:
      result = S_OK()
      if not status and not minorStatus:
        result['Totals'] = {'Submitted':int( submittedJobs ), 'Done':int( doneJobs )}
      result['Value'] = summary
      return result

    #If a printed summary is requested
    statAdj = int( 0.5 * self.prodAdj )
    mStatAdj = int( 2.0 * self.prodAdj )
    totalAdj = int( 0.5 * self.prodAdj )
    exAdj = int( 0.5 * self.prodAdj )
    message = '\nJob Summary for ProductionID %s considering status %s' % ( productionID, status )
    if minorStatus:
      message += 'and MinorStatus = %s' % ( minorStatus )

    message += ':\n\n'
    message += 'Status'.ljust( statAdj ) + 'MinorStatus'.ljust( mStatAdj ) + 'ApplicationStatus'.ljust( mStatAdj ) + 'Total'.ljust( totalAdj ) + 'Example'.ljust( exAdj ) + '\n'
    for stat, metadata in summary.items():
      message += '\n'
      for minor, appInfo in metadata.items():
        message += '\n'
        for appStat, jobInfo in appInfo.items():
          message += stat.ljust( statAdj ) + minor.ljust( mStatAdj ) + appStat.ljust( mStatAdj ) + str( jobInfo['Total'] ).ljust( totalAdj ) + str( jobInfo['JobList'][0] ).ljust( exAdj ) + '\n'

    print message
    #self._prettyPrint(summary)
    if status or minorStatus:
      return S_OK( summary )

    result = self.getProductionProgress( productionID )
    if not result['OK']:
      self.log.warn( 'Could not get production progress information' )
      return result

    if result['Value'].has_key( 'Created' ):
      createdJobs = int( result['Value']['Created'] ) + submittedJobs
    else:
      createdJobs = submittedJobs

    percSub = int( 100 * submittedJobs / createdJobs )
    percDone = int( 100 * doneJobs / createdJobs )
    print '\nCurrent status of production %s:\n' % productionID
    print 'Submitted'.ljust( 12 ) + str( percSub ).ljust( 3 ) + '%  ( ' + str( submittedJobs ).ljust( 7 ) + 'Submitted / '.ljust( 15 ) + str( createdJobs ).ljust( 7 ) + ' Created jobs )'
    print 'Done'.ljust( 12 ) + str( percDone ).ljust( 3 ) + '%  ( ' + str( doneJobs ).ljust( 7 ) + 'Done / '.ljust( 15 ) + str( createdJobs ).ljust( 7 ) + ' Created jobs )'
    result = S_OK()
    result['Totals'] = {'Submitted':int( submittedJobs ), 'Created':int( createdJobs ), 'Done':int( doneJobs )}
    result['Value'] = summary
    #self.pPrint(result)
    return result

  #############################################################################
  def getProductionJobSummary( self, productionID, status = None, minorStatus = None, printOutput = False ):
    """Returns a job summary for the productions in the system. If printOutput is
       specified, the result is printed to the screen.  This queries the WMS
       for the given productionID and provides an up-to-date snapshot of the job status
       combinations and associated WMS JobIDs.
    """
    if type( productionID ) == type( 2 ):
      productionID = long( productionID )
    if not type( productionID ) == type( long( 1 ) ):
      if not type( productionID ) == type( " " ):
        return self.__errorReport( 'Expected string, long or int for production ID' )

    statusDict = self.getProdJobMetadata( productionID, status, minorStatus )
    if not statusDict['OK']:
      self.log.warn( 'Could not get production metadata information' )
      return statusDict

    #Now format the result.
    summary = {}
    submittedJobs = 0
    doneJobs = 0
    for job, atts in statusDict['Value'].items():
      for key, val in atts.items():
        if key == 'Status':
          uniqueStatus = val.capitalize()
          if not summary.has_key( uniqueStatus ):
            summary[uniqueStatus] = {}
          if not summary[uniqueStatus].has_key( atts['MinorStatus'] ):
            summary[uniqueStatus][atts['MinorStatus']] = {}
            summary[uniqueStatus][atts['MinorStatus']]['Total'] = 1
            submittedJobs += 1
            if uniqueStatus == 'Done':
              doneJobs += 1
            summary[uniqueStatus][atts['MinorStatus']]['JobList'] = [job]
          else:
            current = summary[uniqueStatus][atts['MinorStatus']]['Total']
            summary[uniqueStatus][atts['MinorStatus']]['Total'] = current + 1
            submittedJobs += 1
            if uniqueStatus == 'Done':
              doneJobs += 1
            jobList = summary[uniqueStatus][atts['MinorStatus']]['JobList']
            jobList.append( job )
            summary[uniqueStatus][atts['MinorStatus']]['JobList'] = jobList

    if not printOutput:
      result = S_OK()
      if not status and not minorStatus:
        result['Totals'] = {'Submitted':int( submittedJobs ), 'Done':int( doneJobs )}
      result['Value'] = summary
      return result

    #If a printed summary is requested
    statAdj = int( 0.5 * self.prodAdj )
    mStatAdj = int( 2.0 * self.prodAdj )
    totalAdj = int( 0.5 * self.prodAdj )
    exAdj = int( 0.5 * self.prodAdj )
    message = '\nJob Summary for ProductionID %s considering' % ( productionID )
    if status:
      message += ' Status = %s' % ( status )
    if minorStatus:
      message += ' MinorStatus = %s' % ( minorStatus )
    if not status and not minorStatus:
      message += ' all status combinations'

    message += ':\n\n'
    message += 'Status'.ljust( statAdj ) + 'MinorStatus'.ljust( mStatAdj ) + 'Total'.ljust( totalAdj ) + 'Example'.ljust( exAdj ) + '\n'
    for stat, metadata in summary.items():
      message += '\n'
      for minor, jobInfo in metadata.items():
        message += stat.ljust( statAdj ) + minor.ljust( mStatAdj ) + str( jobInfo['Total'] ).ljust( totalAdj ) + str( jobInfo['JobList'][0] ).ljust( exAdj ) + '\n'

    print message
    #self._prettyPrint(summary)
    if status or minorStatus:
      return S_OK( summary )

    result = self.getProductionProgress( productionID )
    if not result['OK']:
      return result

    if result['Value'].has_key( 'Created' ):
      createdJobs = int( result['Value']['Created'] ) + submittedJobs
    else:
      createdJobs = submittedJobs

    percSub = int( 100 * submittedJobs / createdJobs )
    percDone = int( 100 * doneJobs / createdJobs )
    print '\nCurrent status of production %s:\n' % productionID
    print 'Submitted'.ljust( 12 ) + str( percSub ).ljust( 3 ) + '%  ( ' + str( submittedJobs ).ljust( 7 ) + 'Submitted / '.ljust( 15 ) + str( createdJobs ).ljust( 7 ) + ' Created jobs )'
    print 'Done'.ljust( 12 ) + str( percDone ).ljust( 3 ) + '%  ( ' + str( doneJobs ).ljust( 7 ) + 'Done / '.ljust( 15 ) + str( createdJobs ).ljust( 7 ) + ' Created jobs )'
    result = S_OK()
    result['Totals'] = {'Submitted':int( submittedJobs ), 'Created':int( createdJobs ), 'Done':int( doneJobs )}
    result['Value'] = summary
    return result

  #############################################################################
  def getProductionSiteSummary( self, productionID, site = None, printOutput = False ):
    """Returns a site summary for the productions in the system. If printOutput is
       specified, the result is printed to the screen.  This queries the WMS
       for the given productionID and provides an up-to-date snapshot of the sites
       that jobs were submitted to.
    """
    if type( productionID ) == type( 2 ):
      productionID = long( productionID )
    if not type( productionID ) == type( long( 1 ) ):
      if not type( productionID ) == type( " " ):
        return self.__errorReport( 'Expected string, long or int for production ID' )

    statusDict = self.getProdJobMetadata( productionID, None, None, site )
    if not statusDict['OK']:
      self.log.warn( 'Could not get production metadata information' )
      return statusDict

    summary = {}
    submittedJobs = 0
    doneJobs = 0

    for job, atts in statusDict['Value'].items():
      for key, val in atts.items():
        if key == 'Site':
          uniqueSite = val
          currentStatus = atts['Status'].capitalize()
          if not summary.has_key( uniqueSite ):
            summary[uniqueSite] = {}
          if not summary[uniqueSite].has_key( currentStatus ):
            summary[uniqueSite][currentStatus] = {}
            summary[uniqueSite][currentStatus]['Total'] = 1
            submittedJobs += 1
            if currentStatus == 'Done':
              doneJobs += 1
            summary[uniqueSite][currentStatus]['JobList'] = [job]
          else:
            current = summary[uniqueSite][currentStatus]['Total']
            summary[uniqueSite][currentStatus]['Total'] = current + 1
            submittedJobs += 1
            if currentStatus == 'Done':
              doneJobs += 1
            jobList = summary[uniqueSite][currentStatus]['JobList']
            jobList.append( job )
            summary[uniqueSite][currentStatus]['JobList'] = jobList

    if not printOutput:
      result = S_OK()
      if not site:
        result = self.getProductionProgress( productionID )
        if not result['OK']:
          return result
        if result['Value'].has_key( 'Created' ):
          createdJobs = result['Value']['Created']
        result['Totals'] = {'Submitted':int( submittedJobs ), 'Done':int( doneJobs )}
      result['Value'] = summary
      return result

    #If a printed summary is requested
    siteAdj = int( 1.0 * self.prodAdj )
    statAdj = int( 0.5 * self.prodAdj )
    totalAdj = int( 0.5 * self.prodAdj )
    exAdj = int( 0.5 * self.prodAdj )
    message = '\nSummary for ProductionID %s' % ( productionID )
    if site:
      message += ' at Site %s' % ( site )
    else:
      message += ' at all Sites'
    message += ':\n\n'
    message += 'Site'.ljust( siteAdj ) + 'Status'.ljust( statAdj ) + 'Total'.ljust( totalAdj ) + 'Example'.ljust( exAdj ) + '\n'
    for siteStr, metadata in summary.items():
      message += '\n'
      for stat, jobInfo in metadata.items():
        message += siteStr.ljust( siteAdj ) + stat.ljust( statAdj ) + str( jobInfo['Total'] ).ljust( totalAdj ) + str( jobInfo['JobList'][0] ).ljust( exAdj ) + '\n'

    print message
    #self._prettyPrint(summary)
    result = self.getProductionProgress( productionID )

    if not result['OK']:
      return result

    if result['Value'].has_key( 'Created' ):
      createdJobs = int( result['Value']['Created'] ) + submittedJobs
    else:
      createdJobs = submittedJobs

    percSub = int( 100 * submittedJobs / createdJobs )
    percDone = int( 100 * doneJobs / createdJobs )
    if not site:
      print '\nCurrent status of production %s:\n' % productionID
      print 'Submitted'.ljust( 12 ) + str( percSub ).ljust( 3 ) + '%  ( ' + str( submittedJobs ).ljust( 7 ) + 'Submitted / '.ljust( 15 ) + str( createdJobs ).ljust( 7 ) + ' Created jobs )'
      print 'Done'.ljust( 12 ) + str( percDone ).ljust( 3 ) + '%  ( ' + str( doneJobs ).ljust( 7 ) + 'Done / '.ljust( 15 ) + str( createdJobs ).ljust( 7 ) + ' Created jobs )'
    result = S_OK()
    result['Totals'] = {'Submitted':int( submittedJobs ), 'Created':int( createdJobs ), 'Done':int( doneJobs )}
    result['Value'] = summary
    return result

  #############################################################################
  def getProductionProgress( self, productionID = None, printOutput = False ):
    """Returns the status of jobs as seen by the production management infrastructure.
    """
    if type( productionID ) == type( 2 ):
      productionID = long( productionID )
    if productionID:
      if not type( productionID ) == type( long( 1 ) ):
        if not type( productionID ) == type( " " ):
          return self.__errorReport( 'Expected string, long or int for production ID' )

    if not productionID:
      result = self.getActiveProductions()
      if not result['OK']:
        return result
      productionID = result['Value'].keys()
    else:
      productionID = [productionID]

    productionID = [ str( x ) for x in productionID ]
    self.log.verbose( 'Will check progress for production(s):\n%s' % ( string.join( productionID, ', ' ) ) )
    progress = {}
    for prod in productionID:
      #self._prettyPrint(result)
      result = self.prodClient.getTransformationTaskStats( int( prod ) )
      if not result['Value']:
        self.log.error( result )
        return result
      progress[int( prod )] = result['Value']

    if not printOutput:
      return result
    idAdj = int( self.prodAdj )
    statAdj = int( self.prodAdj )
    countAdj = int( self.prodAdj )
    message = 'ProductionID'.ljust( idAdj ) + 'Status'.ljust( statAdj ) + 'Count'.ljust( countAdj ) + '\n\n'
    for prod, info in progress.items():
      for status, count in info.items():
        message += str( prod ).ljust( idAdj ) + status.ljust( statAdj ) + str( count ).ljust( countAdj ) + '\n'
      message += '\n'

    print message
    return result

  #############################################################################
  def getProductionCommands( self ):
    """ Returns the list of possible commands and their meaning.
    """
    prodCommands = {}
    for keyword, statusSubMode in self.commands.items():
      prodCommands[keyword] = {'Status':statusSubMode[0], 'SubmissionMode':statusSubMode[1]}
    return S_OK( prodCommands )

  #############################################################################
  def production( self, productionID, command, printOutput = False, disableCheck = True ):
    """Allows basic production management by supporting the following commands:
       - start : set production status to Active, job submission possible
       - stop : set production status to Stopped, no job submissions
       - automatic: set production submission mode to Automatic, e.g. submission via Agent
       - manual: set produciton submission mode to manual, e.g. dirac-production-submit
    """
    commands = self.commands
    if type( productionID ) == type( 2 ):
      productionID = long( productionID )
    if not type( productionID ) == type( long( 1 ) ):
      if not type( productionID ) == type( " " ):
        return self.__errorReport( 'Expected string, long or int for production ID' )

    if not type( command ) == type( " " ):
      return self.__errorReport( 'Expected string, for command' )
    if not command.lower() in commands.keys():
      return self.__errorReport( 'Expected one of: %s for command string' % ( string.join( commands.keys(), ', ' ) ) )

    self.log.verbose( 'Requested to change production %s with command "%s"' % ( productionID, command.lower().capitalize() ) )
    if not disableCheck:
      result = self.__promptUser( 'Do you wish to change production %s with command "%s"? ' % ( productionID, command.lower().capitalize() ) )
      if not result['OK']:
        self.log.info( 'Action cancelled' )
        return S_OK( 'Action cancelled' )

    actions = commands[command]
    self.log.info( 'Setting production status to %s and submission mode to %s for productionID %s' % ( actions[0], actions[1], productionID ) )
    result = self.prodClient.setTransformationParameter( long( productionID ), "Status", actions[0] )
    if not result['OK']:
      self.log.warn( 'Problem updating transformation status with result:\n%s' % result )
      return result
    self.log.verbose( 'Setting transformation status to %s successful' % ( actions[0] ) )
    result = self.prodClient.setTransformationParameter( long( productionID ), 'AgentType', actions[1] )
    if not result['OK']:
      self.log.warn( 'Problem updating transformation agent type with result:\n%s' % result )
      return result
    self.log.verbose( 'Setting transformation agent type to %s successful' % ( actions[1] ) )
    return S_OK( 'Production %s status updated' % productionID )

  #############################################################################
  def deleteProduction( self, productionID, printOutput = False ):
    """ Deletes a production from the production management system ONLY. To be
        used with extreme care after BK replica flags / LFC entries / Logs are
        cleaned up.
    """
    if type( productionID ) == type( 2 ):
      productionID = long( productionID )
    if not type( productionID ) == type( long( 1 ) ):
      if not type( productionID ) == type( " " ):
        return self.__errorReport( 'Expected string, long or int for production ID' )

    result = self.prodClient.deleteTransformation( productionID )
    if result['OK'] and printOutput:
      print 'Production %s is deleted from the production management system' % productionID

    return result

  #############################################################################
  def productionFileSummary( self, productionID, selectStatus = None, outputFile = None, orderOutput = True, printSummary = False, printOutput = False ):
    """ Allows to investigate the input files for a given production transformation
        and provides summaries / selections based on the file status if desired.
    """
    adj = 18
    ordering = 'TaskID'
    if not orderOutput:
      ordering = 'LFN'
    fileSummary = self.prodClient.getTransformationFiles( condDict = {'TransformationID':int( productionID )}, orderAttribute = ordering )
    if not fileSummary['OK']:
      return fileSummary

    toWrite = ''
    totalRecords = 0
    summary = {}
    selected = 0
    if fileSummary['OK']:
      for lfnDict in fileSummary['Value']:
        totalRecords += 1
        record = ''
        recordStatus = ''
        for n, v in lfnDict.items():
          record += str( n ) + ' = ' + str( v ).ljust( adj ) + ' '
          if n == 'Status':
            recordStatus = v
            if selectStatus == recordStatus:
              selected += 1
            if summary.has_key( v ):
              new = summary[v] + 1
              summary[v] = new
            else:
              summary[v] = 1

        if outputFile and selectStatus:
          if selectStatus == recordStatus:
            toWrite += record + '\n'
            if printOutput:
              print record
        elif outputFile:
          toWrite += record + '\n'
          if printOutput:
            print record
        else:
          if printOutput:
            print record

    if printSummary:
      print '\nSummary for %s files in production %s\n' % ( totalRecords, productionID )
      print 'Status'.ljust( adj ) + ' ' + 'Total'.ljust( adj ) + 'Percentage'.ljust( adj ) + '\n'
      for n, v in summary.items():
        percentage = int( 100 * int( v ) / totalRecords )
        print str( n ).ljust( adj ) + ' ' + str( v ).ljust( adj ) + ' ' + str( percentage ).ljust( 2 ) + ' % '
      print '\n'

    if selectStatus and not selected:
      return S_ERROR( 'No files were selected for production %s and status "%s"' % ( productionID, selectStatus ) )
    elif selectStatus and selected:
      print '%s / %s files (%s percent) were found for production %s in status "%s"' % ( selected, totalRecords, int( 100 * int( selected ) / totalRecords ), productionID, selectStatus )

    if outputFile:
      if os.path.exists( outputFile ):
        print 'Requested output file %s already exists, please remove this file to continue' % outputFile
        return fileSummary

      fopen = open( outputFile, 'w' )
      fopen.write( toWrite )
      fopen.close()
      if not selectStatus:
        print 'Wrote %s lines to file %s' % ( totalRecords, outputFile )
      else:
        print 'Wrote %s lines to file %s for status "%s"' % ( selected, outputFile, selectStatus )

    return fileSummary

  #############################################################################
  def checkFilesStatus( self, lfns, productionID = '', printOutput = False ):
    """Checks the given LFN(s) status in the productionDB.  All productions
       are considered by default but can restrict to productionID.
    """
    if type( productionID ) == type( 2 ):
      productionID = long( productionID )
    if not type( productionID ) == type( long( 1 ) ):
      if not type( productionID ) == type( " " ):
        return self.__errorReport( 'Expected string, long or int for production ID' )

    if type( lfns ) == type( " " ):
      lfns = lfns.replace( 'LFN:', '' )
    elif type( lfns ) == type( [] ):
      try:
        lfns = [str( lfnName.replace( 'LFN:', '' ) ) for lfnName in lfns]
      except Exception, x:
        return self.__errorReport( str( x ), 'Expected strings for LFN(s)' )
    else:
      return self.__errorReport( 'Expected single string or list of strings for LFN(s)' )

    fileStatus = self.prodClient.getFileSummary( lfns, long( productionID ) )
    if printOutput:
      self._prettyPrint( fileStatus['Value'] )
    return fileStatus


  #############################################################################
  def selectAndSetFileStatus( self, productionID, selectStatus, selectMinorStatus = '',
                              site = '', setFlag = 1, setStatus = 'Unused', printOutput = False ):
    """ Set status for the given files in the lfns list for production specified by its ID
    """
    if type( productionID ) == type( 2 ):
      productionID = long( productionID )
    if not type( productionID ) == type( long( 1 ) ):
      if not type( productionID ) == type( " " ):
        return self.__errorReport( 'Expected string, long or int for production ID' )

    jobs = self.selectProductionJobs( productionID, Status = selectStatus,
                                      MinorStatus = selectMinorStatus, Site = site )
    if not jobs['OK']:
      self.log.error( jobs )
      return jobs

    jobs = jobs['Value']
    self.log.info( 'Selected %s jobs:\n%s' % ( len( jobs ), string.join( jobs, ', ' ) ) )
    lfns = []
    res = self.diracAPI.getJobInputData( jobs )
    #print res
    if not res['OK']:
      self.log.error( 'Could not determine input data for all jobs' )
      return res

    lfnsDict = res['Value']
    for job, lfnList in lfnsDict.items():
      lfns += lfnList

    lfns = [i.replace( 'LFN:', '' ) for i in lfns]

    self.log.verbose( 'Found LFNs:\n%s' % ( string.join( lfns, '\n' ) ) )

    if setFlag:
      result = self.prodClient.setFileStatusForTransformation( productionID, setStatus, lfns )
      if printOutput:
        if not result['OK']:
          print result
          print "Failed to update status for files:\n%s" % ( string.join( lfns, '\n' ) )
        for lfn, message in result['Value']['Successful'].items():
          print "Successful:", lfn, ":", message
        for lfn, message in result['Value']['Failed'].items():
          print "Failed:", lfn, ":", message
      return result
    else:
      self.log.info( 'setFlag disabled, would have set %s files to %s' % ( len( lfns ), setStatus ) )
      return S_OK()

  #############################################################################
  def setFileStatus( self, lfns, productionID, status, printOutput = False ):
    """ Set status for the given files in the lfns list for production specified by its ID
    """
    if type( productionID ) == type( 2 ):
      productionID = long( productionID )
    if not type( productionID ) == type( long( 1 ) ):
      if not type( productionID ) == type( " " ):
        return self.__errorReport( 'Expected string, long or int for production ID' )

    if type( lfns ) in types.StringTypes:
      lfnList = [lfns]
    elif type( lfns ) == types.ListType:
      lfnList = lfns
    else:
      return self.__errorReport( 'Expected string or list for LFNs' )

    result = self.prodClient.setFileStatusForTransformation( productionID, status, lfnList )
    if printOutput:
      if not result['OK']:
        print result
        print "Failed to update status for files:\n%s" % ( string.join( lfnList, '\n' ) )
      for lfn, message in result['Value']['Successful'].items():
        print "Successful:", lfn, ":", message
      for lfn, message in result['Value']['Failed'].items():
        print "Failed:", lfn, ":", message

    return result

  #############################################################################
  def getProdJobInputData( self, jobID ):
    """ For a single jobID / list of jobIDs retrieve the output data LFN list.
    """
    return self.diracAPI.getJobInputData( jobID )

  #############################################################################
  def getProdJobOutputData( self, jobID ):
    """ For a single jobID / list of jobIDs retrieve the output data LFN list.
    """
    result = self.diracAPI.getJobJDL( jobID )
    if not result['OK']:
      return result
    if not result['Value'].has_key( 'ProductionOutputData' ):
      return S_ERROR( 'Could not obtain ProductionOutputData from job JDL' )
    lfns = result['Value']['ProductionOutputData']
    if type( lfns ) == type( ' ' ):
      lfns = [lfns]
    return S_OK( lfns )

  #############################################################################
  def getWMSProdJobID( self, jobID, printOutput = False ):
    """This method takes the DIRAC WMS JobID and returns the Production JobID information.
    """
    result = self.diracAPI.attributes( jobID )
    if not result['OK']:
      return result
    if not result['Value'].has_key( 'JobName' ):
      return S_ERROR( 'Could not establish ProductionID / ProductionJobID, missing JobName' )

    wmsJobName = result['Value']['JobName']
    prodID = wmsJobName.split( '_' )[0]
    prodJobID = wmsJobName.split( '_' )[1]
    info = {'WMSJobID':jobID, 'JobName':wmsJobName, 'ProductionID':prodID, 'JobID':prodJobID}
    if printOutput:
      self._prettyPrint( info )
    return S_OK( info )

  #############################################################################
  def getProdJobOutputSandbox( self, jobID ):
    """Wraps around DIRAC API getOutputSandbox(), takes single jobID or list of jobIDs.
    """
    #TODO: write with wrapper zfilled prod directory
    return self.diracAPI.getOutputSandbox( jobID )

  #############################################################################
  def getProdJobInputSandbox( self, jobID ):
    """Wraps around DIRAC API getInputSandbox(), takes single jobID or list of jobIDs.
    """
    #TODO: write with wrapper zfilled prod directory
    return self.diracAPI.getInputSandbox( jobID )

  #############################################################################
  def getProdJobStatus( self, jobID ):
    """Wraps around DIRAC API status(), takes single jobID or list of jobIDs.
    """
    return self.diracAPI.status( jobID )

  #############################################################################
  def rescheduleProdJobs( self, jobID ):
    """Wraps around DIRAC API reschedule(), takes single jobID or list of jobIDs.
    """
    return self.diracAPI.reschedule( jobID )

  #############################################################################
  def deleteProdJobs( self, jobID ):
    """Wraps around DIRAC API delete(), takes single jobID or list of jobIDs.
    """
    #Notification of the production management infrastructure to be added
    return self.diracAPI.delete( jobID )

  #############################################################################
  def getProdJobInfo( self, productionID, jobID, printOutput = False ):
    """Retrieve production job information from Production Manager service.
    """
    res = self.prodClient.getTransformationTasks( condDict = {'TransformationID':productionID, 'TaskID':jobID}, inputVector = True )
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR( "Job %s not found for production %s" % ( jobID, productionID ) )
    jobInfo = res['Value'][0]
    if printOutput:
      self._prettyPrint( jobInfo )
    return S_OK( jobInfo )

  #############################################################################
  def getProdJobSummary( self, jobID, outputFile = None, printOutput = False ):
    """Wraps around DIRAC API getJobSummary to provide more detailed information.
    """
    return self.diracAPI.getJobSummary( jobID, outputFile, printOutput )

  #############################################################################
  def getProdJobLoggingInfo( self, jobID ):
    """Wraps around DIRAC API getJobLoggingInfo to provide more detailed information.
       Takes single WMS JobID.
    """
    return self.diracAPI.loggingInfo( jobID )

  #############################################################################
  def getProdJobParameters( self, jobID ):
    """Wraps around DIRAC API parameters(), takes single jobID or list of jobIDs.
    """
    return self.diracAPI.parameters( jobID )

  #############################################################################
  def selectProductionJobs( self, ProductionID, Status = None, MinorStatus = None, ApplicationStatus = None, Site = None, Owner = None, Date = None ):
    """Wraps around DIRAC API selectJobs(). Arguments correspond to the web page
       selections. By default, the date is the creation date of the production.
    """
    if not Date:
      self.log.verbose( 'No Date supplied, setting old date for production %s' % ProductionID )
      Date = '2001-01-01'
    return self.diracAPI.selectJobs( Status, MinorStatus, ApplicationStatus, Site, Owner, str( ProductionID ).zfill( 8 ), Date )

  #############################################################################
  def extendProduction( self, productionID, numberOfJobs, printOutput = False ):
    """ Extend Simulation type Production by number of jobs.
        Usage: extendProduction <ProductionNameOrID> nJobs
    """
    if type( productionID ) == type( 2 ):
      productionID = long( productionID )
    if not type( productionID ) == type( long( 1 ) ):
      if not type( productionID ) == type( " " ):
        return self.__errorReport( 'Expected string or long for production ID' )

    if type( numberOfJobs ) == type( " " ):
      try:
        numberOfJobs = int( numberOfJobs )
      except Exception, x:
        return self.__errorReport( str( x ), 'Expected integer or string for number of jobs to submit' )

    result = self.prodClient.extendTransformation( long( productionID ), numberOfJobs )
    if not result['OK']:
      return self.__errorReport( result, 'Could not extend production %s by %s jobs' % ( productionID, numberOfJobs ) )

    if printOutput:
      print 'Extended production %s by %s jobs' % ( productionID, numberOfJobs )

    return result

  #############################################################################
  def getProdJobMetadata( self, productionID, status = None, minorStatus = None, site = None ):
    """Function to get the WMS job metadata for selected fields. Given a production ID will return
       the current WMS status information for all jobs in that production starting from the creation
       date.
    """
    result = self.prodClient.getTransformationParameters( long( productionID ), ['CreationDate'] )
    if not result['OK']:
      self.log.warn( 'Problem getting production metadata for ID %s:\n%s' % ( productionID, result ) )
      return result

    creationDate = toString( result['Value'] ).split()[0]
    result = self.selectProductionJobs( productionID, Status = status, MinorStatus = minorStatus, Site = site, Date = creationDate )
    if not result['OK']:
      self.log.warn( 'Problem selecting production jobs for ID %s:\n%s' % ( productionID, result ) )
      return result

    jobsList = result['Value']
    return self.diracAPI.status( jobsList )

  #############################################################################
  def __errorReport( self, error, message = None ):
    """Internal function to return errors and exit with an S_ERROR()
    """
    if not message:
      message = error

    self.log.warn( error )
    return S_ERROR( message )

  #############################################################################
  def _prettyPrint( self, object ):
    """Helper function to pretty print an object.
    """
    print self.pPrint.pformat( object )

  #############################################################################
  def __promptUser( self, message ):
    """Internal function to prompt user for raw input.
    """
    self.log.info( '%s %s' % ( message, '[yes/no] : ' ) )
    response = raw_input( '%s %s' % ( message, '[yes/no] : ' ) )
    responses = ['yes', 'y', 'n', 'no']
    if not response.strip() or response == '\n':
      self.log.info( 'Possible responses are: %s' % ( string.join( responses, ', ' ) ) )
      response = raw_input( '%s %s' % ( message, '[yes/no] : ' ) )

    if not response.strip().lower() in responses:
      self.log.info( 'Problem interpreting input "%s", assuming negative response.' % ( response ) )
      return S_ERROR( response )

    if response.strip().lower() == 'y' or response.strip().lower() == 'yes':
      return S_OK( response )
    else:
      return S_ERROR( response )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
