#"""
#LHCbDIRAC/ResourceStatusSystem/Agent/HCModes/HCBaseMode.py
#"""
#
#__RCSID__ = "$Id$"
#
## First, pythonic stuff
#from datetime import datetime, timedelta
#
## Second, DIRAC stuff
#from DIRAC import gLogger, S_OK, S_ERROR
#
## Third, LHCbDIRAC stuff
## ...
#
#class HCBaseMode( object ):
#  '''
#  Class HCBaseMode. It defines how is going to be the execution cycle, but it
#  does not provide any implementation. The execution cycle goes through the 
#  following six steps:
#  
#  - updateScheduledTests    
#  - updateOngoingTests       
#  - updateLastFinishedTests  
#  - checkQuarantine          
#  - submitTests              
#  - monitorTests          
#  
#  There is no state machine, so test states are hardcoded here and there. The list
#  of possible test status is:
#  
#  ( happy way )
#  - HCscheduled     : test has been scheduled on the HC framework successfully
#  - HCongoing       : test is running on the HC framework
#  - HClastfinished  : test has just finished 
#  - HCfinished      : another test has finished on the same site
#    
#  ( something went wrong )
#  - HCquarantine    : test(s) failed on the site of this test, no tests for a while
#  - HCfailure       : another test failed on the same site
#  - HCprevent       : after a quarantine, tests are closely monitored
#  - HCskipped       : noisy sites, takes them out and notifies admin
#  '''
#  
#  # Desired time format
#  __TIME_FORMAT_    = "%Y-%m-%d %H:%M:%S"
#  # Lock token
#  __LOCK_TOKEN_     = 'RS_Hold' 
#  # Unlock token
#  __UNLOCK_TOKEN_   = 'RS_SVC' 
#  
#################################################################################  
#  
#  def __init__( self, setup, rssDB, rmDB, hcClient, diracAdmin, maxCounterAlarm, 
#                testDuration, pollingTime ):
#    '''
#    Class constructor
#    
#    :params:
#      :attr: `setup` : string - DIRAC setup
#      :attr: `rssDB` : ResourceStatusDB - database object
#      :attr: `rmDB` : ResourceManagementDB - database object
#      :attr: `hc` : HCClient - HammerCloud client
#      :attr: `da` : DiracAdmin - DIRAC admin class
#      :attr: `maxCounterAlarm` : int - number of errors accepted
#      :attr: `testDuration` : int - test duration on seconds
#      :attr: `pollingTime` : int - agent polling time on seconds
#    '''
#    
#    self.setup           = setup
#    self.rssDB           = rssDB
#    self.rmDB            = rmDB
#    self.hc              = hcClient
#    self.da              = diracAdmin   
#    self.maxCounterAlarm = maxCounterAlarm
#    self.testDuration    = testDuration
#    self.pollingTime     = pollingTime
#      
#################################################################################
## Main (base) loop functions
#################################################################################
#
#  def updateScheduledTests( self ):
#    '''
#    To be overwritten.
#    Update status of scheduled tests if they are already submitted
#    '''
#    
#    print '1.- updateScheduledTests'
#    return S_OK()
#
#################################################################################           
#    
#  def updateOngoingTests( self ):
#    '''
#    To be overwritten
#    Update status of submitted tests if they are already running
#    '''
#    
#    print '2.- updateOngoingTests'
#    return S_OK()
#
#################################################################################           
#  
#  def updateLastFinishedTests( self ):
#    '''
#    To be overwritten
#    Update status of finished tests when another test concludes
#    '''
#    
#    print '3.- updateLastFinishedTests'
#    return S_OK()
#
#################################################################################           
#  
#  def checkQuarantine( self ):
#    '''
#    To be overwritten
#    Checks the results from the test and decides what to do.
#    If it returns S_ERROR or S_OK( ( False, )) next step will not be executed
#    '''
#    
#    print '4.- checkQuarantine'
#    return S_OK( ( True, 'Default quarantine' ) )
#
#################################################################################           
#  
#  def submitTests( self ):
#    '''
#    To be overwritten
#    Submits tests to HammerCloud
#    '''
#    
#    print '5.- submitTests'
#    return S_OK()
#
#################################################################################           
#
#  def monitorTests( self ):
#    '''
#    To be overwritten
#    If we are not submitting tests, we monitor the status of the tests
#    '''
#    
#    print '6.- monitorTests'
#    return S_OK()
#
#################################################################################           
#
#  def run( self ):
#    '''
#    Main method. Defines the flow of actions to be taken.
#    '''
#
#    gLogger.info( '>>> UPDATING TESTS' )
#    gLogger.info( '  + scheduled' )
#    res = self.updateScheduledTests()
#    if not res['OK']: return res
#    
#    gLogger.info( '  + ongoings' )
#    res = self.updateOngoingTests()
#    if not res['OK']: return res
#    
#    gLogger.info( '  + lastFinished' ) 
#    res = self.updateLastFinishedTests()
#    if not res['OK']: return res  
#      
#    gLogger.info( '>>> CHECKING QUARANTINE' )  
#    res = self.checkQuarantine()     
#    if not res['OK']: return res
#    
#    gLogger.info( '  '+res['Value'][1] )    
#    if not res['Value'][0]:
#      res = self.submitTests()
#      if not res['OK']: return res
#    else:
#      res = self.monitorTests()
#      if not res['OK']: return res  
#  
#    return S_OK()  
#
#################################################################################
## END Main (base) loop functions
#################################################################################           
#
#################################################################################
## HCBaseMode getters
#################################################################################
#
#  def _getTimeFormat( self ):
#    '''
#    Returns __TIME_FORMAT__
#    '''
#    
#    return S_OK( self.__TIME_FORMAT_ )           
#  
#################################################################################  
#  
#  def _getLockToken( self ):
#    '''
#    Returns __LOCK_TOKEN_
#    '''
#    
#    return S_OK( self.__LOCK_TOKEN_ )
#  
#################################################################################  
#  
#  def _getUnLockToken( self ):
#    '''
#    Returns __UNLOCK_TOKEN_
#    '''
#    
#    return S_OK( self.__UNLOCK_TOKEN_ )   
#
#################################################################################
## END HCBaseMode getters
#################################################################################
#           
#################################################################################
## AUXILIAR FUNCTIONS
#################################################################################         
#
#  '''      
#  AUXILIAR FUNCTIONS
#  
#  a) _sendEmail               ( subject, body )
#  b) _lockSite                ( siteName, endTime )
#  c) _unLockSite              ( siteName )
#  d) _addTest                 ( siteName, submitTime, agentStatus )
#  e) _getTestList             ( check, *args, **kwargs )
#  f) _updateTest              ( submitTime, *args, **kwargs )
#  h) _getResources            ( granularity, name )
#  
#  i) _checkMaxCounterAlarm    ( siteName, counter ) 
#  j) _checkErrorCounter       ( )
#  k) _checkStartTime          ( siteName )
#  l) _checkFinishedTest       ( testID, siteName )
#  
#  m) _detectErrors            ( result, time, site, formerAgentStatus )
#  
#  '''  
#################################################################################           
#  
#  def _sendEmail( self, subject, body ):
#    '''
#    Every time this agent sends an email, Chuck eats a kitten. 
#    ( The problem with python v2.6 has been fixed. Feel free kittens )
#    
#    :params:
#      :attr: `subject` : string - subject of the email
#      
#      :attr: `body` : string - body of the email
#    '''
#    
#    sendMail = self.da.sendMail( 'mario.ubeda.garcia@cern.ch', subject, body )
#    if not sendMail['OK']:
#      gLogger.error( 'Error sending email' )
#      
#    return S_OK()
#
#################################################################################           
#
#  def _lockToken( self, granularity, name, endTime ):
#    '''
#    Sets OwnerToken = __LOCK_TOKEN_ for the element `name` of granularity 
#    `granularity`
#    
#    :params:
#      :attr: `granularity` : string - one of the RSS valid granularities
#      
#      :attr: `name` : string - element name
#      
#      :attr: `endTime` : datetime - expiration date for the token
#    '''
#    
#    self.rssDB.setToken( granularity, name, self.__LOCK_TOKEN_, endTime )            
#    gLogger.info( '        locked  : Ok, until %s' % endTime )
#    
#    return S_OK()
#
#################################################################################           
#
#  def _unLockToken( self, granularity, name ):
#    '''
#    Sets OwnerToken = __UNLOCK_TOKEN_ for the element `name` of granularity 
#    `granularity` with `infinite` endtime
#    
#    :params:
#      :attr: `granularity` : string - one of the RSS valid granularities
#      
#      :attr: `name` : string - element name
#    '''
#    
#    self.rssDB.setToken( granularity, name, self.__UNLOCK_TOKEN_, 
#                         datetime(9999, 12, 31, 23, 59, 59) )            
#    gLogger.info( '        unlocked  : Ok' )
#    
#    return S_OK()
#
#################################################################################           
#
#  def _addTest( self, siteName, submitTime, agentStatus, resourceName ):
#    '''
#    Adds a test entry to the ResourceManagementDB.
#    
#    :params:
#      :attr: `siteName` : string - name of the destination
#      
#      :attr: `submitTime` : datetime - timestamp of submission order
#      
#      :attr: `agentStatus` : string - status assigned to test by agent
#      
#      :attr: `resourceName` : string - if any, CE where the test is going to run   
#    '''
#    
#    self.rmDB.addHCTest( siteName, submitTime, agentStatus, resourceName )
#    
#    return S_OK() 
#
#################################################################################           
#
#  def _getTestList( self, check = False, *args, **kwargs ):
#    '''
#    Returns a list of test given a set of filters:
#    
#    :params:
#      :attr: `check` : bool - checks if there is more than 1 item
#      
#      :attr: `*args` : not used
#      
#      :attr: `**kwargs` : dict - possible filters
#        - submissionTime
#        - testStatus
#        - siteName 
#        - testID
#        - resourceName
#        - agentStatus 
#        - formerAgentStatus
#        - counter
#        - last    
#    '''
#       
#    items = self.rmDB.getHCTestList( *args, **kwargs )
#    
#    if check and len( items ) > 1:
#      
#      subject = 'More than 1 item: %s' % items
#      gLogger.exception( subject )
#      self._sendEmail( '[HCAgent]', subject )
#      return S_ERROR( 'Check Agent Break' )
#    
#    return S_OK( items )
#
#################################################################################           
#
#  def _updateTest( self, submitTime, *args, **kwargs ):
#    '''
#    Returns a list of test given a set of filters:
#    
#    :params:     
#      :attr: `*args` : not used
#      
#      :attr: `**kwargs` : dict - possible filters
#        - submissionTime
#        - testStatus
#        - siteName 
#        - testID
#        - resourceName
#        - agentStatus 
#        - formerAgentStatus
#        - counter
#    '''
#    
#    self.rmDB.updateHCTest( submitTime, *args, **kwargs)
#       
#    return S_OK()
#
#################################################################################           
#
#  def _getSites( self, granularity, name = None ):
#    '''
#    Returns the element(s) of a given granularity. If name is given,
#    it uses it as a filter.
#    
#    :params:
#      :attr: `granularity` : string - RSS valid granularity
#      
#      :attr: `name` : string - element name ( optional )
#    '''
#    
#    return self.rssDB.getStuffToCheck( granularity, name = name)     
#
#################################################################################           
#
#  def _getResources( self, *args, **kwargs ):
#    '''
#    Returns the resources matching the filters on the ResourceStatusDB
#    
#    :params:
#      :attr: `*args` : list - not used
#      
#      :attr: `**kwargs` : dict - filters for the search
#        - resourceName
#        - resourceType
#        - serviceType
#        - siteName
#        - gridSiteName
#        - status
#        - reason
#        - tokenOwner   
#    '''
#    #Given a resource, we cannot know to which site it belongs.
#    
#    return self.rssDB.getResources( *args, **kwargs)   
#
#################################################################################           
#
#  def _checkMaxCounterAlarm( self, siteName, counter, resourceName ):
#    '''
#    Given a counter, it checks if it exceeds the maximum. If it does,
#    it sends an email.
#    
#    :params:
#      :attr: `siteName` : string - name of the site
#      
#      :attr: `counter` : int - value of the counter
#      
#      :attr: `resourceName` : string - Not used. Name of the resource
#    '''
#
#    if counter > self.maxCounterAlarm :
#
#      subject = '[HCAgent][MAX COUNTER ALARM] at %s' % siteName
#      body    = 'You should take a look...'
#
#      self._sendEmail( subject, body )
#      
#      return S_OK('HCskipped')
#    
#    return S_OK()
#
#################################################################################           
#
#  def _checkErrorCounter( self ):
#    '''
#    Basically, if there is a failure, set the test as HCquarantine.
#    If there is any prevent test, set the new test as HCfailure, and the former
#    one to HCquarantine. If this happens more than MAX_COUNTER_ALARM ( 4 ),
#    notify admin.
#    '''
#
#    failures = self._getTestList( check = True, agentStatus = 'HCfailure', counter = 0 )
#    prevents = self._getTestList( check = True, agentStatus = 'HCprevent', counter = 0 )
#    
#    if not failures['OK']: return failures
#    else: 
#      failures = failures['Value']
#    
#    if not prevents['OK']: return prevents
#    else: 
#      prevents = prevents['Value']
#
#    if failures and prevents:
#      subject = 'Failures and prevents at the same time ! %s, %s' % ( failures, prevents )
#      gLogger.exception( subject )
#      self._sendEmail( '[HCAgent]', subject )
#      return S_ERROR( 'Check Error Counter' )
#    
#    now = datetime.now()
#    
#    agentStatus  = 'HCfailure'
#    counter      = 1
#    counterTime  = now  
#    
#    if failures:
#      
#      f           = failures[0]
#      counter     = f[9]
#      agentStatus = 'HCquarantine'
#      
#      
#    elif prevents:
#      
#      p                 = prevents[0]
#      site              = p[1]
#      resource          = p[2]
#      submissionTime    = p[6]
#      counter           = p[9]
#      agentStatus       = 'HCfailure'
#      formerAgentStatus = 'HCprevent'
#      counter           = counter + 1
#
#      agentStatus = ( 1 and self._checkMaxCounterAlarm( site, counter, resource )['Value'] ) or agentStatus
#        
#      #Update former test
#      self._updateTest( submissionTime, agentStatus = 'HCquarantine', formerAgentStatus = formerAgentStatus )     
#    
#    return S_OK( ( counter, agentStatus, counterTime ) )           
#
#################################################################################           
#
#  def _checkStartTime( self, siteName, resourceName ):
#    '''
#    Check if there is any HClastfinished test and take its counter and endtime. If not, counter is 0
#    and endtime = now.
#    Set next starttime for this site at endtime + 2 * counter * TEST_DURATION 
#    '''
#    
#    lastFinished = self._getTestList( check = True, agentStatus = 'HClastfinished', siteName = siteName, 
#                                      resourceName = resourceName, counter = 0 )
#        
#    if not lastFinished['OK']: return lastFinished
#    else: 
#      lastFinished = lastFinished['Value'] 
#      
#    counter     = 1 
#    agentStatus = 'HCscheduled'
#    starttime   = datetime.now()
#    
#    if lastFinished:
#      
#      lF = lastFinished[0]
#      
#      site        = lF[1]
#      resource    = lF[2]
##      endTime     = lF[8]
#      counter     = lF[9]
#      counterTime = lF[10]
#    
#      period    = 2 * counter * self.testDuration
#
#      starttime = counterTime + timedelta( hours = period )
#   
#      counter += 1 
# 
#      agentStatus = ( 1 and self._checkMaxCounterAlarm( site, counter, resource )['Value'] ) or agentStatus
#    
#    return S_OK( ( counter, agentStatus, starttime ) )                    
#
#################################################################################           
#           
#  def _checkFinishedTest( self, testID, siteName, resourceName ):         
#    '''
#    If there is any HClastfinished for this site, move its status to HCfinished,
#    and set the new test to HClastfinished.
#    '''           
#           
#    lastFinished = self._getTestList( check = True, agentStatus = 'HClastfinished', 
#                                      siteName = siteName, resourceName = resourceName )     
#    
#    if not lastFinished['OK']: return lastFinished
#    else: 
#      lastFinished = lastFinished['Value']
#        
#    formerAgentStatus = 'HClastfinished'       
#           
#    agentStatus = 'HClastfinished'       
#           
#    if lastFinished:
#      lastF        = lastFinished[0]
#      lastF_testID = lastF[0]
#      
#      now = datetime.now()
#           
#      self._updateTest( None, testID = lastF_testID, agentStatus = 'HCfinished', 
#                             formerAgentStatus = formerAgentStatus, counterTime = now )
#    return S_OK( agentStatus ) 
#
#################################################################################           
#              
#  def _detectErrors( self, result, time, site, resource, formerAgentStatus ):
#    '''
#    We know how errors coming from HC look like.
#    So, print something meaningful, send and email and set the site
#    as HCskipped. It puts the site on a corner for HC, but releases
#    the token, allowing the rest of policies to modify it.    
#    '''
#    
#    if result[0]:
#      return S_OK( False )
#    
#    if result[1]['type'] == 'SUBMISSION':
#      gLogger.info( '      submission ERROR : %s' % site )
#      subject = '[HCAgent][Submission]'
#      body = '%s' % result
#            
#    elif result[1]['type'] == 'MISSING':
#      gLogger.info( '      missing ERROR : %s' % site ) 
#      subject = '[HCAgent][Missing]'
#      body = '%s' % result
#            
#    elif result[1]['type'] == 'UNKNOWN':
#      gLogger.info( '      unknown ERROR : %s' % site )   
#      subject = '[HCAgent][Unknown]'
#      body = '%s is Unknown \n %s' % ( site, result )
#      
#    else:
#      gLogger.info( '      error : %s , no idea what happened' % site )       
#      subject = '[HCAgent][updateOngoing][NPI]'
#      body = '%s is NPI \n %s' % ( site, result )
#    
#    self._sendEmail( subject, body ) 
#    agentStatus = 'HCskipped'
#    self._updateTest( time, agentStatus = agentStatus, formerAgentStatus = formerAgentStatus )
#    
#    if resource == '':
#      self._unLockToken( 'Site', site )
#    else:
#      self._unLockToken( 'Resource', resource )
#    
#    return S_OK( True )            
#  
#################################################################################
## END AUXILIAR FUNCTIONS
#################################################################################
#
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  