from DIRAC import gLogger, S_OK, S_ERROR

from datetime import datetime, timedelta

class HCBaseMode( object ):
  
  __TIME_FORMAT_    = "%Y-%m-%d %H:%M:%S"
  # Lock token
  __LOCK_TOKEN_     = 'RS_Hold' 
  # Unlock token
  __UNLOCK_TOKEN_   = 'RS_SVC' 
  
  def __init__( self, setup, rssDB, rsaDB, hcClient, diracAdmin, maxCounterAlarm, testDuration, pollingTime ):
    
    self.setup           = setup
    self.rssDB           = rssDB
    self.rsaDB           = rsaDB
    self.hc              = hcClient
    self.da              = diracAdmin   
    self.maxCounterAlarm = maxCounterAlarm
    self.testDuration    = testDuration
    self.pollingTime     = pollingTime

  '''
  HCBaseMode getters

  a) _getTimeFormat           ( )
  b) _getLockToken            ( )
  c) _getUnLockToken          ( ) 
  '''

  def _getTimeFormat( self ):
    return S_OK( self.__TIME_FORMAT_ )           
  
  def _getLockToken( self ):
    return S_OK( self.__LOCK_TOKEN_ )
  
  def _getUnLockToken( self ):
    return S_OK( self.__UNLOCK_TOKEN_ )   

#############################################################################           

  '''      
  MAIN (BASE) LOOP FUNCTIONS
  
  a) updateScheduledTests     ( )
  b) updateOngoingTests       ( )
  c) updateLastFinishedTests  ( )
  d) checkQuarantine          ( )
  e) submitTests              ( )
  f) monitorTests             ( )
  g) run                      ( )
  '''              

  def updateScheduledTests( self ):
    print '1.- updateScheduledTests'
    return S_OK()
    
  def updateOngoingTests( self ):
    print '2.- updateOngoingTests'
    return S_OK()
  
  def updateLastFinishedTests( self ):
    print '3.- updateLastFinishedTests'
    return S_OK()
  
  def checkQuarantine( self ):
    print '4.- checkQuarantine'
    return S_OK( ( True, 'Default quarantine' ) )
  
  def submitTests( self ):
    print '5.- submitTests'
    return S_OK()

  def monitorTests( self ):
    print '6.- monitorTests'
    return S_OK()

  def run( self ):

    gLogger.info( '>>> UPDATING TESTS' )
    gLogger.info( '  + scheduled' )
    res = self.updateScheduledTests()
    if not res['OK']: return res
    
    gLogger.info( '  + ongoings' )
    res = self.updateOngoingTests()
    if not res['OK']: return res
    
    gLogger.info( '  + lastFinished' ) 
    res = self.updateLastFinishedTests()
    if not res['OK']: return res  
      
    gLogger.info( '>>> CHECKING QUARANTINE' )  
    res = self.checkQuarantine()     
    if not res['OK']: return res
    
    gLogger.info( '  '+res['Value'][1] )    
    if not res['Value'][0]:
      res = self.submitTests()
      if not res['OK']: return res
    else:
      res = self.monitorTests()
      if not res['OK']: return res  
  
    return S_OK()  
           
#############################################################################           

  '''      
  AUXILIAR FUNCTIONS
  
  a) _sendEmail               ( subject, body )
  b) _lockSite                ( siteName, endTime )
  c) _unLockSite              ( siteName )
  d) _addTest                 ( siteName, submitTime, agentStatus )
  e) _getTestList             ( check, *args, **kwargs )
  f) _updateTest              ( submitTime, *args, **kwargs )
  h) _getResources            ( granularity, name )
  
  i) _checkMaxCounterAlarm    ( siteName, counter ) 
  j) _checkErrorCounter       ( )
  k) _checkStartTime          ( siteName )
  l) _checkFinishedTest       ( testID, siteName )
  
  m) _detectErrors            ( result, time, site, formerAgentStatus )
  
  '''  
  
  def _sendEmail( self, subject, body ):
    '''
    Every time this agent sends an email, Chuck eats a kitten. 
    '''
    #sendMail = self.da.sendMail('aebeda3@gmail.com',subject,body)
    #if not sendMail['OK']:
    #  gLogger.error( 'Error sending email' )
      
    return S_OK()

  def _lockToken( self, granularity, name, endTime ):
    
    self.rssDB.setToken( granularity, name, self.__LOCK_TOKEN_, endTime )            
    gLogger.info( '        locked  : Ok, until %s' % endTime )
    
    return S_OK()

  def _unLockToken( self, granularity, name ):
    
    self.rssDB.setToken( granularity, name, self.__UNLOCK_TOKEN_, datetime(9999, 12, 31, 23, 59, 59) )            
    gLogger.info( '        unlocked  : Ok' )
    
    return S_OK()

  def _addTest( self, siteName, submitTime, agentStatus, resourceName ):
    
    self.rsaDB.addTest( siteName, submitTime, agentStatus, resourceName )
    
    return S_OK() 

  def _getTestList( self, check = False, *args, **kwargs ):
    
    items = self.rsaDB.getTestList( *args, **kwargs ) 
    
    if check and len( items ) > 1:
      subject = 'More than 1 item: %s' % item
      gLogger.exception( subject )
      self._sendEmail( '[HCAgent]', subject )
      return S_ERROR( 'Check Agent Break' )
    
    return S_OK( items )

  def _updateTest( self, submitTime, *args, **kwargs ):
    
    self.rsaDB.updateTest( submitTime, *args, **kwargs)
       
    return S_OK()

  def _getSites( self, granularity, name = None ):
    
    return self.rssDB.getStuffToCheck( granularity, name = name)     

  def _getResources( self, *args, **kwargs ):
    
    #Given a resource, we cannot know to which site it belongs.
    
    return self.rssDB.getResources( *args, **kwargs)   


  def _checkMaxCounterAlarm( self, siteName, counter, resourceName ):

    if counter > self.maxCounterAlarm :

      subject = '[HCAgent][MAX COUNTER ALARM] at %s' % siteName
      body    = 'You should take a look...'

      self._sendEmail( subject, body )
      
      return S_OK('HCskipped')
    
    return S_OK()

  def _checkErrorCounter( self ):
    '''
    Basically, if there is a failure, set the test as HCquarantine.
    If there is any prevent test, set the new test as HCfailure, and the former
    one to HCquarantine. If this happens more than MAX_COUNTER_ALARM ( 4 ),
    notify admin.
    '''

    failures = self._getTestList( check = True, agentStatus = 'HCfailure', counter = 0 )
    prevents = self._getTestList( check = True, agentStatus = 'HCprevent', counter = 0 )
    
    if not failures['OK']: return failures
    else: 
      failures = failures['Value']
    
    if not prevents['OK']: return prevents
    else: 
      prevents = prevents['Value']

    if failures and prevents:
      subject = 'Failures and prevents at the same time ! %s, %s' % ( failures, prevents )
      gLogger.exception( subject )
      self._sendEmail( '[HCAgent]', subject )
      return S_ERROR( 'Check Error Counter' )
    
    now = datetime.now()
    
    agentStatus  = 'HCfailure'
    counter      = 1
    counterTime  = now  
    
    if failures:
      
      f           = failures[0]
      counter     = f[9]
      agentStatus = 'HCquarantine'
      
      
    elif prevents:
      
      p                 = prevents[0]
      site              = p[1]
      resource          = p[2]
      submissionTime    = p[6]
      counter           = p[9]
      agentStatus       = 'HCfailure'
      formerAgentStatus = 'HCprevent'
      counter           = counter + 1

      agentStatus = ( 1 and self._checkMaxCounterAlarm( site, counter, resource )['Value'] ) or agentStatus
        
      #Update former test
      self._updateTest( submissionTime, agentStatus = 'HCquarantine', formerAgentStatus = formerAgentStatus )     
    
    return S_OK( ( counter, agentStatus, counterTime ) )           

  def _checkStartTime( self, siteName, resourceName ):
    '''
    Check if there is any HClastfinished test and take its counter and endtime. If not, counter is 0
    and endtime = now.
    Set next starttime for this site at endtime + 2 * counter * TEST_DURATION 
    '''
    
    lastFinished = self._getTestList( check = True, agentStatus = 'HClastfinished', siteName = siteName, 
                                      resourceName = resourceName, counter = 0 )
        
    if not lastFinished['OK']: return lastFinished
    else: 
      lastFinished = lastFinished['Value'] 
      
    counter     = 1 
    agentStatus = 'HCscheduled'
    starttime   = datetime.now()
    
    if lastFinished:
      
      lF = lastFinished[0]
      
      site        = lF[1]
      resource    = lF[2]
      endTime     = lF[8]
      counter     = lF[9]
      counterTime = lF[10]
    
      period    = 2 * counter * self.testDuration

      starttime = counterTime + timedelta( hours = period )
   
      counter += 1 
 
      agentStatus = ( 1 and self._checkMaxCounterAlarm( site, counter, resource )['Value'] ) or agentStatus
    
    return S_OK( ( counter, agentStatus, starttime ) )                    
           
  def _checkFinishedTest( self, testID, siteName, resourceName ):         
    '''
    If there is any HClastfinished for this site, move its status to HCfinished,
    and set the new test to HClastfinished.
    '''           
           
    lastFinished = self._getTestList( check = True, agentStatus = 'HClastfinished', 
                                      siteName = siteName, resourceName = resourceName )     
    
    if not lastFinished['OK']: return lastFinished
    else: 
      lastFinished = lastFinished['Value']
        
    formerAgentStatus = 'HClastfinished'       
           
    agentStatus = 'HClastfinished'       
           
    if lastFinished:
      lastF        = lastFinished[0]
      lastF_testID = lastF[0]
      
      now = datetime.now()
           
      self._updateTest( None, testID = lastF_testID, agentStatus = 'HCfinished', 
                             formerAgentStatus = formerAgentStatus, counterTime = now )
    return S_OK( agentStatus ) 
              
  def _detectErrors( self, result, time, site, resource, formerAgentStatus ):
    '''
    We know how errors coming from HC look like.
    So, print something meaningful, send and email and set the site
    as HCskipped. It puts the site on a corner for HC, but releases
    the token, allowing the rest of policies to modify it.    
    '''
    
    if result[0]:
      return S_OK( False )
    
    if result[1]['type'] == 'SUBMISSION':
      gLogger.info( '      submission ERROR : %s' % site )
      subject = '[HCAgent][Submission]'
      body = '%s' % result
            
    elif result[1]['type'] == 'MISSING':
      gLogger.info( '      missing ERROR : %s' % site ) 
      subject = '[HCAgent][Missing]'
      body = '%s' % result
            
    elif result[1]['type'] == 'UNKNOWN':
      gLogger.info( '      unknown ERROR : %s' % site )   
      subject = '[HCAgent][Unknown]'
      body = '%s is Unknown \n %s' % ( site, result )
      
    else:
      gLogger.info( '      error : %s , no idea what happened' % site )       
      subject = '[HCAgent][updateOngoing][NPI]'
      body = '%s is NPI \n %s' % ( site, result )
    
    self._sendEmail( subject, body ) 
    agentStatus = 'HCskipped'
    self._updateTest( time, agentStatus = agentStatus, formerAgentStatus = formerAgentStatus )
    
    if resource == '':
      self._unLockToken( 'Site', site )
    else:
      self._unLockToken( 'Resource', resource )
    
    return S_OK( True )            

#############################################################################  