########################################################################
# $HeadURL:  $
########################################################################

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

from DIRAC.ResourceStatusSystem.Utilities.Utils import where

from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB

from LHCbDIRAC.ResourceStatusSystem.DB.ResourceStatusAgentDB import ResourceStatusAgentDB
from LHCbDIRAC.ResourceStatusSystem.Client.HCClient import HCClient

from datetime import datetime, timedelta

import time

__RCSID__ = "$Id: $"

AGENT_NAME = 'ResourceStatus/HCAgent'

class HCAgent(AgentModule):
  """ Class HCAgent looks for sites on Probing state,
      with tokenOwner = RS_SVC and sends a test. After,
      'locks' the site with tokenOwner == RS_HOLD
  """

  # Test duation in hours
  TEST_DURATION     = 1.5
  TIME_FORMAT       = "%Y-%m-%d %H:%M:%S"
  MAX_COUNTER_ALARM = 4
  
#############################################################################

  def initialize(self):
    """ Standard constructor
    """
    
    try:
      self.rsDB  = ResourceStatusDB()
      self.rsaDB = ResourceStatusAgentDB()
      self.hc    = HCClient()
      
      self.diracAdmin = DiracAdmin()
        
      gLogger.info( "TEST_DURATION: %s hours" % self.TEST_DURATION )  
        
      return S_OK()

    except Exception:
      errorStr = "HCAgent initialization"
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################           

  '''      
  MAIN LOOP FUNCTIONS
  
  a) _updateScheduledTests
  b) _updateOngoingTests
  c) _updateLastFinished
  d) _checkQuarantine
  e) _submitTests
  '''  

#############################################################################           
           
  def _updateScheduledTests(self):
    '''
    This function processes all tests with status HCscheduled.
    If test.starttime < now or test.endtime < now, queries HC for
    fresh data. If the data is out of synch, we syncronize it.
    
    If test.endtime < now is bad. Means that test is sheduled, but
    test.endtime is on the past. Mark the test as failed, and unlock it.
    
    If test.starttime < now, means the test has already started running.
    We do not care here if crashed on HC or not, we just lock the site
    and set it as HCongoing. Next function will handle it.  
    '''
    scheduled = self.rsaDB.getTestList(reason = 'HCscheduled')
    
    if not scheduled:
      gLogger.info( '      no scheduled tests' )
      return S_OK()
             
    for sc in scheduled:
      
      testID    = sc[0]
      site      = sc[1]
      submitime = sc[4]
      starttime = sc[5] 
      endtime   = sc[6]
      
      gLogger.info( '      scheduled : on %s at %s' % ( site, starttime ) )
      
      now = datetime.now().replace( microsecond = 0 )
      
      if starttime < now or endtime < now:
         
        res = self.hc.getTest(testID)
        if not self.__detectErrors( res, submitime, site ):
          
          test       = res[1]['response']['test'] 
          testStatus = test['state']        
          startTime  = datetime.strptime(test['starttime'], self.TIME_FORMAT)
          endTime    = datetime.strptime(test['endtime'], self.TIME_FORMAT) 

          # IF it is different, it has been modified. Update.
          if endTime != endtime:
            gLogger.info( ' Updated endTime %s -> %s' % ( endtime, endTime ) )          
            self.rsaDB.updateTest( submitime, endTime = endTime )  
        
          # If endTime is in HC < now, there is a problem in HC
          elif endTime < now:
            ## ERROR 
            gLogger.info( '        endtime ( %s ) < now ( %s )' % ( endtime, now ) )
            gLogger.info( '        error   : test is scheduled, but endtime < now' )
            #self.rsDB.setToken( 'Site', site, 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59) )            
            #print '        unlock  : Ok'
            counter,reason,counterTime = self.__checkErrorCounter()       
            self.rsaDB.updateTest( submitime, reason = reason, counter = counter, counterTime = counterTime )          
        
          if starttime != startTime:
            gLogger.info( ' Updated startTime %s -> %s' % ( starttime, startTime ) )          
            self.rsaDB.updateTest( submitime, startTime = startTime )  
       
          elif testStatus not in ['tobescheduled','scheduled']:
            gLogger.info( '      starttime ( %s ) < now ( %s )' % ( starttime, now ) )
            self.rsDB.setToken( 'Site', site, 'RS_Hold', endtime )            
            gLogger.info( '        locked  : Ok' )
            gLogger.info( '        ongoing : Ok' )          
            # We move it to ongoing. UpdateOnGoingTests will deal with possible failures
            self.rsaDB.updateTest( submitime, reason = 'HCongoing' ) 
              
    return S_OK()             
           
#############################################################################           
           
  def _updateOngoingTests(self):
    '''
      First of all, check all tests registered as HCongoing on
      the HCAgent table.
      
      If there are no ongoing tests, nothing to do here.
      
      If there are tests ongoing, ask HammerCloud for an update.
        OPTIONS:
          o1: HammerCloud can return a NOK. 
            -> Still unclear how to handle it.
          o2: Test status is error.
            -> checkCounterError and update.
          o3: Test endTime is on the past, and test status reported by HC ! [completed,error]
            -> checkCounterError and update
          o4: Test status is completed
            -> checkFinishedTest and update
          o5: Test status is something else.
            -> update 
    '''
    
    ongoings     = self.rsaDB.getTestList(reason = 'HCongoing')
    
    if not ongoings:
      gLogger.info( '      no ongoing tests' )
      return S_OK()

    for og in ongoings:
      
      if not isinstance(og[0], long):
        gLogger.info( 'There is a NULL ID with status Ongoing' )
        subject = '[HCAgent][NULL HCongoing]'
        body = '%s is NULL HCongoing \n %s' % ( og[1], og )
        self.__sendEmail( subject, body )  
        continue
      
      res = self.hc.getTest(og[0])

      if self.__detectErrors( res, og[4], og[1] ):
        continue
          
      test       = res[1]['response']['test']
         
      testID     = test['id']
      testStatus = test['state']        
      site       = test['site']
      startTime  = datetime.strptime(test['starttime'], self.TIME_FORMAT)
      endTime    = datetime.strptime(test['endtime'], self.TIME_FORMAT)
      
      counter      = None
      reason       = 'HCongoing - HC updated'  
      counterTime  = endTime
        
      ## SANITY CHECK
      ## Site used in HC must be the same as we have on the HCAgent table
      if og[1] != site:
        gLogger.info( '      sanity check failed: test %d has %s (HC retrieved), %s (HCAgent table)' % ( testID, og[1], site ) )
        self.rsDB.setToken( 'Site', site, 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59) )
        gLogger.info( '      unlock : Ok ' )  
        testStatus = 'error'
        gLogger.info( '      marking as error, until problem is clarified' )
        
        '''
          We should kill the test in HC
        '''
      
      # We do not care about seconds
      if endTime.replace( second = 0 ) != og[6].replace( second = 0 ):
        gLogger.info( '      endTime modified : test %d has %s (HC retrieved), %s (HCAgent table)' % ( testID, og[6], endTime ) )
        self.rsDB.setToken( 'Site', site, 'RS_Hold', endTime )
        gLogger.info( '      lock : updated ' )
        
        
      if testStatus == 'error':
          
        counter,reason,counterTime = self.__checkErrorCounter()#, reason, datetime.now())
        reason += ' - running error.'
        gLogger.info( '      error : test %d, at %s (returned error)' % ( testID , site ) ) 
        self.rsDB.setToken( 'Site', site, 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59) )  
        gLogger.info( '      unlock : Ok ' )  
          
      # HC needs some time to end the test, depending on the number of jobs to be killed, etc..
      # We are giving half of the test duration time, or 15 min (the maximum)
      elif testStatus != 'completed' and datetime.now() - timedelta(hours = max( self.TEST_DURATION * 0.5, 0.25) ) > endTime:
          
        testStatus = 'error'  
        counter,reason,counterTime = self.__checkErrorCounter()#, reason, datetime.now())
        reason += ' - HC not updating status.'
        gLogger.info( '      error : test %d, at %s (timeout without completing)' % ( testID , site ) )
        self.rsDB.setToken( 'Site', site, 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59) )  
        gLogger.info( '      unlock : Ok ' )
        
      elif testStatus == 'completed':
        
        reason = self.__checkFinishedTest(testID,site)  
        gLogger.info( '      completed : test %d, at %s' % ( testID , site ) )
        self.rsDB.setToken( 'Site', site, 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59) )  
        gLogger.info( '      unlock : Ok ' )        

      else:
        gLogger.info( '      %s : test %d, at %s' % ( testStatus, testID, site ) )

      self.rsaDB.updateTest( None, testID = testID, testStatus = testStatus, startTime = startTime, 
                                 endTime = endTime, reason = reason, counter = counter, counterTime = counterTime )  
      
    return S_OK()   

#############################################################################

  def _updateLastFinishedTests(self):
    '''
      Check all tests registered as HClastfinished on
      the HCAgent table (max one per site).
      
      If there are no last finished tests, nothing to do here.
      
      If there are, check counterTime and counter. This means,
      if last counter update was at least 2 * counter * TEST_DURATION hours in
      the past, decrease counter ( min is zero).
      
      The HClastfinished status is used to prevent site overflood when tests
      are completing, but not successful according to policies. 
    '''
    
    lastFinished = self.rsaDB.getTestList(reason = 'HClastfinished', counter = 0)
    
    if not lastFinished:
      gLogger.info( '      no lastFinished' )
      return S_OK()
    
    now = datetime.now()
    
    for lastF in lastFinished:
      
      testID      = lastF[0]
      site        = lastF[1]
      counter     = lastF[7]
      counterTime = lastF[8]
      
      period      = 2 * counter * self.TEST_DURATION
      
      afterPeriod = counterTime + timedelta( hours = period )      
      
      gLogger.info( '      completed: test %s, at %s - counter %s, counterTime %s ' % ( testID, site, counter, counterTime ) )
      
      if now > afterPeriod:
        
        counter    -= 1
        counterTime = afterPeriod 
    
        gLogger.info( '        updating test %s counter to %s, with counterTime %s' % ( testID, counter, afterPeriod ) )
    
        reason = None
        if counter == 0:
          reason = 'HCfinished'
    
        self.rsaDB.updateTest( None, testID = testID, counter = counter, counterTime = counterTime, reason = reason )
    
    return S_OK()
            
#############################################################################

  def _checkQuarantine(self):
    '''
      There are two types of quarantine, blocking and preventive.
      
      Whenever a test fails, it's state is moved to HCfailure. This agent
      waits ( 1 + 2 * counter ) * TEST_TIME hours to submit again a test.
      
      After that period, the agent allows submission again, but if there is
      a failure, we will increment the counter and move to blocking mode.
      
    '''    
        
    gLogger.info( '>>> CHECKING QUARANTINE' )
    
    failures = self.rsaDB.getTestList(reason = 'HCfailure', counter = 0)
    prevents = self.rsaDB.getTestList(reason = 'HCprevent', counter = 0)
    
    now = datetime.now()

    if failures and len(failures) > 1:
      gLogger.exception('More than 1 failure: %s' % failures)
      return S_ERROR('Check Agent Break')
    
    if prevents and len(prevents) > 1:
      gLogger.exception('More than 1 failure: %s' % failures)
      return S_ERROR('Check Agent Break')

    if failures and prevents:
      gLogger.exception('Failures and prevents at the same time ! %s, %s' % ( failures, prevents ))
      return S_ERROR('Check Agent Break')

    #############################################################################
    ## FAILURES SECTION
    #############################################################################
    
    if failures:
      f = failures[0]   
      quarantine  = ( 1 + 2 * f[7] ) * self.TEST_DURATION                                     
      endQuarant  = f[8] + timedelta(hours = quarantine) 
      endPrevent  = endQuarant + timedelta(hours = quarantine)  

      if now < endQuarant:          
        return S_OK(value = (True, '  quarantine ( test %s ) until %s counter %s' % ( f[0], endQuarant, f[7] )))  
 
      elif now < endPrevent:
        self.rsaDB.updateTest(f[4], reason = 'HCprevent', counterTime = now)
        return S_OK(value = (False, '  prevent submission ( test %s ) until %s counter %s' % ( f[0], endPrevent, f[7] )))

      else:
        gLogger.exception('Failures > endPrevent')
        return S_ERROR('Check Agent Break')

    #############################################################################
    ## PREVENTS SECTION
    #############################################################################
    
    if prevents:
      
      p = prevents[0]
        
      prevention  = ( 1 + 2 * p[7] ) * self.TEST_DURATION                                     
      endPrevent  = p[8] + timedelta(hours = prevention) 
        
      pollingTime = self.am_getPollingTime()
      firstAfterP = endPrevent + timedelta(seconds=pollingTime)
        
      if now < endPrevent:
        return S_OK(value = (False, '  preventive submission ( test %s ) until %s counter %s.' % ( p[0], endPrevent, p[7] )))
        
      elif now < firstAfterP:
        counter = p[7] - 1
        reason = None
        messag = '  one more round of preventive submission.'
          
        if counter == 0:
          reason = 'HCquarantine'
          messag = '  end of quarantine and prevention time.'
            
        self.rsaDB.updateTest(p[4], counter = counter, reason = reason)  
        return S_OK(value = (False, messag))   
    
      else:
        gLogger.exception('Prevents > firstAfterP')
        return S_ERROR('Check Agent Break')
   
    return S_OK(value = (False,'+ No worries'))

#############################################################################

  def _submitTests(self):
    '''
      Try to submit a test for every Probing site with token RS_SVC.
    '''
    
    sites = self.rsDB.getStuffToCheck('Sites')
    
    gLogger.info( '>>> SUBMITTING TESTS' )
      
    siteFlag = False  
      
    for siteTuple in sites:

      if siteTuple[1] == 'Probing':
        if siteTuple[4] == 'RS_SVC':

          site = siteTuple[0]
          
          now  = datetime.now()
            
          if self.rsaDB.getTestList( siteName = site, reason = 'HCongoing' ):
            gLogger.info( '  + %s already has an ongoing test' % site )
            continue

          if self.rsaDB.getTestList( siteName = site, reason = 'HCscheduled' ):
            #print '  + %s already has an scheduled test'%site
            continue
          
          if self.rsaDB.getTestList( siteName = site, reason = 'HCskipped' ):
            gLogger.info( '  + %s is waiting for a Human action' % site )
            continue
            
          siteFlag = True  
            
          self.rsaDB.addTest( site, now, 'HCscheduled - On client creation')
          
          """
            Check if we have to set a different starttime for the test.
          """
          counter, reason, starttime = self.__checkStartTime(site)        

          """
             Duration must be an INT (put it into seconds)
          """
          durationSecs = int( self.TEST_DURATION * 3600 )

          res = self.hc.sendTest( siteTuple[0], starttime = starttime , duration = durationSecs, template = 2 )                         
            
          # If we detect any error, we stop submitting until next round  
          if self.__detectErrors( res, now, site ):
            return S_OK()  
            
          gLogger.info( '      submission : Ok' )
              
          resp = res[1]['response']    
              
          testStatus = resp['status']
          startTime  = resp['starttime']
          endTime    = resp['endtime']
          testID     = resp['id']
                           
          self.rsaDB.updateTest(now, testID = testID, testStatus = testStatus, startTime = startTime, 
                                   endTime = endTime, reason = reason, counterTime = endTime, counter = counter)
          
          # Do not bother excessively HC, and also ensure submissionTime is different !!
          time.sleep( 5 )
                            
    if not siteFlag:
      gLogger.info( '  + No submitted tests' )  
    
    return S_OK()    
    
#############################################################################           

  '''      
  AUXILIAR FUNCTIONS
  
  a) __checkErrorCounter
  b) __checkStartTime
  c) __checkFinishedTest
  d) __sendEmail
  e) __detectErrors
  '''  

#############################################################################     

  def __checkErrorCounter(self):
    '''
    Basically, if there is a failure, set the test as HCquarantine.
    If there is any prevent test, set the new test as HCfailure, and the former
    one to HCquarantine. If this happens more than MAX_COUNTER_ALARM ( 4 ),
    notify admin.
    '''

    failures = self.rsaDB.getTestList(reason = 'HCfailure', counter = 0)
    prevents = self.rsaDB.getTestList(reason = 'HCprevent', counter = 0)
    
    now = datetime.now()
    
    reason  = 'HCfailure'
    counter = 1
    counterTime = now  

    if len(failures) > 1:
      gLogger.exception('More than 1 failure: %s' % failures)
      return S_ERROR('Check Error Counter')
    
    if len(prevents) > 1:
      gLogger.exception('More than 1 failure: %s' % failures)
      return S_ERROR('Check Error Counter')

    if failures and prevents:
      gLogger.exception('Failures and prevents at the same time ! %s, %s' % ( failures, prevents ))
      return S_ERROR('Check Error Counter')
    
    if failures:
      
      f         = failures[0]
      reason    = 'HCquarantine'
      counter   = f[7]
      
    elif prevents:
      
      p      = prevents[0]
      reason  = 'HCfailure'
      counter = p[7] + 1

      if counter > self.MAX_COUNTER_ALARM:
        
        # After a considerable number of jumps, we put the site aside
        # and alert an administrator to investigate.
        
        reason  = 'HCskipped'
        
        subject = '[HCAgent][MAX COUNTER ALARM] at %s' % p[1]
        body    = 'You should take a look...'
        self.__sendEmail(subject,body)
        
      #Update former test
      self.rsaDB.updateTest(p[4], reason = 'HCquarantine')     
    
    return ( counter, reason, counterTime )           
           
#############################################################################

  def __checkStartTime(self, site):
    '''
    Check if there is any HClastfinished test and take its counter and endtime. If not, counter is 0
    and endtime = now.
    Set next starttime for this site at endtime + 2 * counter * TEST_DURATION 
    '''
    
    lastFinished = self.rsaDB.getTestList(reason = 'HClastfinished', siteName = site, counter = 0)
    
    if len(lastFinished) > 1:
      gLogger.info('More than 1 last finished: %s' % lastFinished)
      S_ERROR('MORE THAN 1 HClastfinished')
      
    counter   = 1 
    reason    = 'HCscheduled'
    starttime = datetime.now()
    
    if lastFinished:
      
      lF = lastFinished[0]
      
      counter   = lF[7]
      endTime   = lF[6]
    
      period    = 2 * counter * self.TEST_DURATION

      starttime = endTime + timedelta( hours = period )

      counter += 1 
      
      if counter > self.MAX_COUNTER_ALARM:
        
        # After a considerable number of jumps, we put the site aside
        # and alert an administrator to investigate.
        
        #reason  = 'HCskipped'
        
        subject = '[HCAgent][MAX COUNTER ALARM] at %s' % lF[1]
        body    = 'You should take a look...'
        self.__sendEmail(subject,body)
    
    return ( counter, reason, starttime )                    

#############################################################################
           
  def __checkFinishedTest(self,testID,site):         
    '''
    If there is any HClastfinished for this site, move its status to HCfinished,
    and set the new test to HClastfinished.
    '''           
           
    lastFinished = self.rsaDB.getTestList(reason = 'HClastfinished', siteName = site)         
           
    reason = 'HClastfinished'       
           
    if len(lastFinished) > 1:
      gLogger.exception('More than 1 last finished: %s' % lastFinished)
      return S_ERROR('Check Agent Break')
           
    if lastFinished:
      lastF        = lastFinished[0]
      lastF_testID = lastF[0]
      
      self.rsaDB.updateTest( None, testID = lastF_testID, reason = 'HCfinished')
           
    return reason
              
#############################################################################              
              
  def __sendEmail( self, subject, body ):
    '''
    Every time this agent sends an email, Chuck eats a kitten. 
    '''
    sendMail = self.diracAdmin.sendMail('aebeda3@gmail.com',subject,body)
    if not sendMail['OK']:
      gLogger.error( 'Error sending email' )

#############################################################################              
              
  def __detectErrors( self, res, time, site ):
    '''
    We know how errors coming from HC look like.
    So, print something meaningful, send and email and set the site
    as HCskipped. It puts the site on a corner for HC, but releases
    the token, allowing the rest of policies to modify it.    
    '''
    
    if res[0]:
      return False
    
    if res[1]['type'] == 'SUBMISSION':
      gLogger.info( '      submission ERROR : %s' % site )
      subject = '[HCAgent][Submission]'
      body = '%s' % res
      #self.__sendEmail( subject, body )
            
    elif res[1]['type'] == 'MISSING':
      gLogger.info( '      missing ERROR : %s' % site ) 
      subject = '[HCAgent][Missing]'
      body = '%s' % res
      #self.__sendEmail( subject, body )
            
    elif res[1]['type'] == 'UNKNOWN':
      gLogger.info( '      unknown ERROR : %s' % site )   
      subject = '[HCAgent][Unknown]'
      body = '%s is Unknown \n %s' % ( site, res )
      #self.__sendEmail( subject, body )  
      
    else:
      gLogger.info( '      error : %s , no idea what happened' % site )       
      subject = '[HCAgent][updateOngoing][NPI]'
      body = '%s is NPI \n %s' % ( site, res )
    
    self.__sendEmail( subject, body ) 
    reason = 'HCskipped'
    self.rsaDB.updateTest( time, reason = reason )
    
    # Release token
    self.rsDB.setToken( 'Site', site, 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59) )            
    gLogger.info( '        unlock  : Ok' )
    
    return True            
              
#############################################################################

  def execute(self):
    """
      The main HCAgent execution method.
    """
    
    try:

      #############################################################################
      # UPDATE
      #############################################################################
      
      """
        Inspect errors from HC, miss configurations MUST not lead to QUARANTINE
      """
      
      gLogger.info( '>>> UPDATING TESTS' )
      gLogger.info( '  + scheduled' )
      self._updateScheduledTests()
      gLogger.info( '  + ongoings' )
      self._updateOngoingTests()
      gLogger.info( '  + lastFinished' ) 
      self._updateLastFinishedTests()

      #############################################################################
      # CHECK IF WE ARE ON QUARANTINE
      #############################################################################
      
      res = self._checkQuarantine()
      
      if not res['OK']:
        return S_ERROR(res['Value'])
      
      gLogger.info( '  '+res['Value'][1] )
     
      #############################################################################
      # SUBMIT TESTS
      #############################################################################
     
      # If we are on quarantine, we do not send tests.
      if res['Value'][0]:
        return S_OK(res['Value'][1])  
      
      self._submitTests()
  
      return S_OK()    
      
    except Exception, x:
      errorStr = where(self, self.execute)
      gLogger.exception(errorStr, lException = x)
      return S_ERROR(errorStr)
  
  
#############################################################################