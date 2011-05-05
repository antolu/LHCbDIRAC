########################################################################
# $HeadURL:  $
########################################################################

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

from DIRAC.ResourceStatusSystem.Utilities.CS import getSetup
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

  TIME_FORMAT       = "%Y-%m-%d %H:%M:%S"
#  MAX_COUNTER_ALARM = 4
  
#############################################################################

  def initialize(self):
    """ Standard constructor
    """
    
    try:
      self.rsDB  = ResourceStatusDB()
      self.rsaDB = ResourceStatusAgentDB()
      self.hc    = HCClient()
      
      self.diracAdmin = DiracAdmin()
      self.setup = None

      # Duration is stored on minutes, but we transform it into minutes       
      self.TEST_DURATION     = self.am_getOption( 'testDurationMin', 120 ) / 60.  
      self.MAX_COUNTER_ALARM = self.am_getOption( 'maxCounterAlarm', 4 )
      
      setup = getSetup()
      if setup['OK']:
        self.setup = setup         
        
      gLogger.info( "TEST_DURATION: %s minutes" % self.TEST_DURATION )  
      gLogger.info( "MAX_COUNTER_ALARM: %s jumps" % self.MAX_COUNTER_ALARM )
        
      return S_OK()

    except Exception:
      errorStr = "HCAgent initialization"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

#############################################################################           

  '''      
  MAIN (MASTER) LOOP FUNCTIONS
  
  a) _updateScheduledTestsMaster
  b) _updateOngoingTestsMaster
  c) _updateLastFinishedTestsMaster
  d) _checkQuarantineMaster
  e) _submitTestsMaster
  '''
  
#############################################################################           
           
  def _updateScheduledTestsMaster(self):
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
    scheduled         = self.rsaDB.getTestList( agentStatus = 'HCscheduled' )
    formerAgentStatus = 'HCscheduled'
    
    if not scheduled:
      gLogger.info( '      no scheduled tests' )
      return S_OK()
             
    for sc in scheduled:
      
      testID    = sc[0]
      site      = sc[1]
      submitime = sc[5]
      starttime = sc[6].replace( second = 0 ) 
      endtime   = sc[7].replace( second = 0 )
      
      gLogger.info( '      scheduled : on %s at %s' % ( site, starttime ) )
      
      now = datetime.now().replace( microsecond = 0 ).replace( second = 0 )
      
      if starttime < now or endtime < now:
         
        res = self.hc.getTest(testID)
        if not self.__detectErrors( res, submitime, site, formerAgentStatus ):
          
          test       = res[1]['response']['test'] 
          testStatus = test['state']        
          startTime  = datetime.strptime( test['starttime'], self.TIME_FORMAT ).replace( second = 0 )
          endTime    = datetime.strptime( test['endtime'], self.TIME_FORMAT ).replace( second = 0 ) 

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
            counter, agentStatus, counterTime = self.__checkErrorCounter()       
            self.rsaDB.updateTest( submitime, agentStatus = agentStatus, formerAgentStatus = formerAgentStatus, 
                                   counter = counter, counterTime = counterTime )   
            subject = '[HCAgent][updateScheduled][Endtime]'
            body = '%s is scheduled but endtime ( %s ) < now ( %s )\n %s' % ( site, endtime, now )
    
            self.__sendEmail( subject, body ) 
               
            continue    
        
          if starttime != startTime:
            gLogger.info( ' Updated startTime %s -> %s' % ( starttime, startTime ) )          
            self.rsaDB.updateTest( submitime, startTime = startTime )  
       
          elif testStatus not in ['tobescheduled','scheduled']:
            gLogger.info( '        starttime ( %s ) < now ( %s )' % ( starttime, now ) )
            self.rsDB.setToken( 'Site', site, 'RS_Hold', endtime )            
            gLogger.info( '        locked  : Ok' )
            gLogger.info( '        ongoing : Ok' )          
            # We move it to ongoing. UpdateOnGoingTests will deal with possible failures
            self.rsaDB.updateTest( submitime, agentStatus = 'HCongoing', formerAgentStatus = formerAgentStatus ) 
              
    return S_OK()             
           
#############################################################################           
           
  def _updateOngoingTestsMaster(self):
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
    
    ongoings     = self.rsaDB.getTestList( agentStatus = 'HCongoing' )
    formerAgentStatus = 'HCongoing'
    
    if not ongoings:
      gLogger.info( '      no ongoing tests' )
      return S_OK()

    for og in ongoings:
      
      if not isinstance( og[0], long ):
        gLogger.info( 'There is a NULL ID with status Ongoing' )
        subject = '[HCAgent][NULL HCongoing]'
        body = '%s is NULL HCongoing \n %s' % ( og[1], og )
        self.__sendEmail( subject, body )  
        continue
      
      res = self.hc.getTest(og[0])

      if self.__detectErrors( res, og[5], og[1], formerAgentStatus ):
        continue
          
      test       = res[1]['response']['test']
         
      testID     = test['id']
      testStatus = test['state']        
      site       = test['site']
      startTime  = datetime.strptime(test['starttime'], self.TIME_FORMAT)
      endTime    = datetime.strptime(test['endtime'], self.TIME_FORMAT)
      
      counter      = None
      agentStatus  = 'HCongoing'  
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
      if endTime.replace( second = 0 ) != og[7].replace( second = 0 ):
        gLogger.info( '      endTime modified : test %d has %s (HC retrieved), %s (HCAgent table)' % ( testID, og[7], endTime ) )
        self.rsDB.setToken( 'Site', site, 'RS_Hold', endTime )
        gLogger.info( '      lock : updated ' )
        
        
      if testStatus == 'error':
          
        counter, agentStatus, counterTime = self.__checkErrorCounter()#, agentStatus, datetime.now())
        #agentStatus += ' - running error.'
        gLogger.info( '      error : test %d, at %s (returned error)' % ( testID , site ) ) 
        self.rsDB.setToken( 'Site', site, 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59) )  
        gLogger.info( '      unlock : Ok ' )  
          
      # HC needs some time to end the test, depending on the number of jobs to be killed, etc..
      # We are giving half of the test duration time, or 15 min (the maximum)
      elif testStatus != 'completed' and datetime.now() - timedelta(hours = max( self.TEST_DURATION * 0.5, 0.25) ) > endTime:
          
        testStatus = 'error'  
        counter, agentStatus, counterTime = self.__checkErrorCounter()#, agentStatus, datetime.now())
        #agentStatus += ' - HC not updating status.'
        gLogger.info( '      error : test %d, at %s (timeout without completing)' % ( testID , site ) )
        self.rsDB.setToken( 'Site', site, 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59) )  
        gLogger.info( '      unlock : Ok ' )
        
      elif testStatus == 'completed':
        
        agentStatus = self.__checkFinishedTest(testID,site)  
        gLogger.info( '      completed : test %d, at %s' % ( testID , site ) )
        self.rsDB.setToken( 'Site', site, 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59) )  
        gLogger.info( '      unlock : Ok ' )        

      else:
        # Nothing special happened
        gLogger.info( '      %s : test %d, at %s with start time %s' % ( testStatus, testID, site, startTime ) )
        formerAgentStatus = None
        
      self.rsaDB.updateTest( None, testID = testID, testStatus = testStatus, startTime = startTime, 
                                 endTime = endTime, agentStatus = agentStatus, formerAgentStatus = formerAgentStatus, 
                                 counter = counter, counterTime = counterTime )  
      
    return S_OK()   

#############################################################################

  def _updateLastFinishedTestsMaster(self):
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
    
    lastFinished      = self.rsaDB.getTestList( agentStatus = 'HClastfinished', counter = 0 )
    formerAgentStatus = 'HClastfinished'
    
    if not lastFinished:
      gLogger.info( '      no lastFinished' )
      return S_OK()
    
    now = datetime.now()
    
    for lastF in lastFinished:
      
      testID      = lastF[0]
      site        = lastF[1]
      endtime     = lastF[7]
      counter     = lastF[8]
      counterTime = lastF[9]
      
      period      = 2 * counter * self.TEST_DURATION
      
      afterPeriod = counterTime + timedelta( hours = period )      
      
      gLogger.info( '      completed: test %s, at %s at %s' % ( testID, site, endtime ) ) 
      gLogger.info( '        counter %s and counterTime %s ' % ( counter, counterTime ) )
      
      if now > afterPeriod:
        
        counter    -= 1
#        counterTime = afterPeriod 
        counterTime = now
    
        gLogger.info( '        updating test %s counter to %s, with counterTime %s' % ( testID, counter, afterPeriod ) )
    
        agentStatus = None
        if counter == 0:
          agentStatus = 'HCfinished'
    
        self.rsaDB.updateTest( None, testID = testID, counter = counter, counterTime = counterTime, 
                               agentStatus = agentStatus, formerAgentStatus = formerAgentStatus )
    
    return S_OK()
            
#############################################################################

  def _checkQuarantineMaster(self):
    '''
      There are two types of quarantine, blocking and preventive.
      
      Whenever a test fails, it's state is moved to HCfailure. This agent
      waits ( 1 + 2 * counter ) * TEST_TIME hours to submit again a test.
      
      After that period, the agent allows submission again, but if there is
      a failure, we will increment the counter and move to blocking mode.
      
    '''    
        
    gLogger.info( '>>> CHECKING QUARANTINE' )
    
    failures = self.rsaDB.getTestList( agentStatus = 'HCfailure', counter = 0 )
    prevents = self.rsaDB.getTestList( agentStatus = 'HCprevent', counter = 0 )
    
    now = datetime.now()

    if failures and len( failures ) > 1:
      gLogger.exception( 'More than 1 failure: %s' % failures )
      return S_ERROR( 'Check Agent Break' )
    
    if prevents and len( prevents ) > 1:
      gLogger.exception( 'More than 1 failure: %s' % failures )
      return S_ERROR( 'Check Agent Break' )

    if failures and prevents:
      gLogger.exception('Failures and prevents at the same time ! %s, %s' % ( failures, prevents ))
      return S_ERROR('Check Agent Break')

    #############################################################################
    ## FAILURES SECTION
    #############################################################################
    
    if failures:
      
      formerAgentStatus = 'HCfailure'
      
      f = failures[0]   
      quarantine  = ( 1 + 2 * f[8] ) * self.TEST_DURATION                                     
      endQuarant  = f[9] + timedelta( hours = quarantine ) 
      endPrevent  = endQuarant + timedelta( hours = quarantine )  

      if now < endQuarant:          
        return S_OK( value = ( True, '  Quarantine ( test %s ) until %s: \n    counter %s and counterTime %s' % ( f[0], endQuarant, f[8], f[9] )))  
 
      elif now < endPrevent:
        self.rsaDB.updateTest( f[5], agentStatus = 'HCprevent', formerAgentStatus = formerAgentStatus, counterTime = now )
        return S_OK( value = ( False, '  Preventive submission ( test %s ) until %s: \n    counter %s and counterTime %s' % ( f[0], endPrevent, f[8], f[9] )))

      else:
        gLogger.exception( 'Failures > endPrevent' )
        return S_ERROR( 'Check Agent Break' )

    #############################################################################
    ## PREVENTS SECTION
    #############################################################################
    
    if prevents:
      
      formerAgentStatus = 'HCprevent'
      
      p = prevents[0]
        
      prevention  = ( 1 + 2 * p[8] ) * self.TEST_DURATION                                     
      endPrevent  = p[9] + timedelta( hours = prevention ) 
        
      pollingTime = self.am_getPollingTime()
      firstAfterP = endPrevent + timedelta( seconds = pollingTime )
        
      if now < endPrevent:
        return S_OK( value = ( False, '  Preventive submission ( test %s ) until %s:\n    counter %s and counterTime %s' % ( p[0], endPrevent, p[8], p[9] )))
        
      elif now < firstAfterP:
        counter = p[8] - 1
        agentStatus = None
        messag = '  one more round of preventive submission.'
          
        if counter == 0:
          agentStatus = 'HCquarantine'
          messag = '  end of quarantine and prevention time.'
            
        self.rsaDB.updateTest( p[5], counter = counter, agentStatus = agentStatus, formerAgentStatus = formerAgentStatus )  
        return S_OK( value = ( False, messag ))   
    
      else:
        gLogger.exception( 'Prevents > firstAfterP' )
        return S_ERROR( 'Check Agent Break')
   
    return S_OK(value = (False,'+ No worries'))

#############################################################################

  def _submitTestsMaster(self):
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
            
          if self.rsaDB.getTestList( siteName = site, agentStatus = 'HCongoing' ):
            gLogger.info( '  + %s already has an ongoing test' % site )
            continue

          if self.rsaDB.getTestList( siteName = site, agentStatus = 'HCscheduled' ):
            #print '  + %s already has an scheduled test'%site
            continue
          
          if self.rsaDB.getTestList( siteName = site, agentStatus = 'HCskipped' ):
            gLogger.info( '  + %s is waiting for a Human action' % site )
            continue
            
          siteFlag = True  
            
          self.rsaDB.addTest( site, now, 'HCscheduled' )
          
          """
            Check if we have to set a different starttime for the test.
          """
          counter, agentStatus, starttime = self.__checkStartTime( site )        

          """
             Duration must be an INT (put it into seconds)
          """
          durationSecs = int( self.TEST_DURATION * 3600 )

          res = self.hc.sendTest( siteTuple[0], starttime = starttime , duration = durationSecs, template = 2 )                         
            
          # If we detect any error, we stop submitting until next round  
          if self.__detectErrors( res, now, site, 'HCscheduled' ):
            return S_OK()  
            
          gLogger.info( '      submission : Ok' )
              
          resp = res[1]['response']    
              
          testStatus = resp['status']
          startTime  = resp['starttime']
          endTime    = resp['endtime']
          testID     = resp['id']
                           
          self.rsaDB.updateTest(now, testID = testID, testStatus = testStatus, startTime = startTime, 
                                   endTime = endTime, agentStatus = agentStatus, formerAgentStatus = 'Undefined',
                                   counterTime = endTime, counter = counter)
          
          # Do not bother excessively HC, and also ensure submissionTime is different !!
          time.sleep( 5 )
                            
    if not siteFlag:
      gLogger.info( '  + No Probing & unlocked sites.' )  
    
    return S_OK()    
    
#############################################################################           

  '''      
  MAIN (SLAVE) LOOP FUNCTIONS
  
  a) _updateScheduledTestsSlave
  b) _updateOngoingTestsSlave
  c) _updateLastFinishedTestsSlave
  d) _checkQuarantineSlave
  e) _submitTestsSlave
  '''
  
#############################################################################        
    
  def _updateScheduledTestsSlave( self ):
    
    scheduled       = self.rsaDB.getTestList( agentStatus = 'HCscheduled' )
    formerScheduled = self.rsaDB.getTestList( formerAgentStatus = 'HCscheduled' )
    
    if not scheduled:
      gLogger.info( '      no scheduled tests' )
       
    now = datetime.now()   
       
    for sc in scheduled:
      
      testID    = sc[0]
      site      = sc[1]
      starttime = sc[6] 
      
      gLogger.info( '      scheduled : test %s on %s at %s' % ( testID, site, starttime ) )
      
      if starttime + timedelta( hours = max( self.TEST_DURATION * 0.5, 0.25) ) < now :
        gLogger.error( 'It seems that the MASTER agent is not running' )
        subject = '[HCAgentSlave][updateScheduled]'
        body = '( %s - %s ). MASTER seems to not run' % ( testID, site )
        self.__sendEmail( subject, body )  
        return S_OK()
             
    for fs in formerScheduled:
      
      testID      = fs[0]
      site        = fs[1]
      agentStatus = fs[3]
      starttime   = fs[6] 
      endtime     = fs[7]
 
      if agentStatus == 'HCongoing':
        
        rssSite = self.rsDB.getStuffToCheck( 'Sites', name = site )
        if rssSite:
          rssSite = rssSite[0]
          token   = rssSite[4]
          if token == 'RS_SVC':
      
            gLogger.info( '      scheduled : test %s on %s at %s' % ( testID, site, starttime ) )
        
            self.rsDB.setToken( 'Site', site, 'RS_Hold', endtime )            
            gLogger.info( '        locked  : Ok' )
            gLogger.info( '        ongoing : Ok' )               
 
          #else:
          #  
          #  gLogger.error( '%s on %s, should not be locked' % ( token, site ))
 
      else:
        
        gLogger.info( '      scheduled : test %s on %s at %s' % ( testID, site, starttime ))
        gLogger.info( '        error : %s' % agentStatus )   
    
    return S_OK()

#############################################################################

  def _updateOngoingTestsSlave( self ):    
    
    ongoing       = self.rsaDB.getTestList( agentStatus = 'HCongoing' )
    formerOngoing = self.rsaDB.getTestList( formerAgentStatus = 'HCongoing' )
    
    if not ongoing:
      gLogger.info(  '      no ongoing tests'  )
  
    now = datetime.now() 
  
    for og in ongoing:
      
      testID      = og[0]
      site        = og[1]
      testStatus  = og[2]
      agentStatus = og[3]
      endtime     = og[7] 
  
      if not isinstance( og[0], long ):
        continue
  
      if endtime + timedelta( hours = max( self.TEST_DURATION * 0.5, 0.25) ) < now :
        gLogger.error( 'It seems that the MASTER agent is not running' )
        subject = '[HCAgentSlave][UpdateOngoing]'
        body = '( %s - %s ). MASTER seems to not run' % ( testID, site )
        self.__sendEmail( subject, body )  
        return S_OK()
  
      gLogger.info( '      %s : test %d, at %s' % ( testStatus, testID, site ) ) 
  
    for fo in formerOngoing:
      
      testID      = fo[0]
      site        = fo[1]
      testStatus  = fo[2]
      agentStatus = fo[3]
  
      if testStatus == 'completed' or testStatus == 'error':
        rssSite = self.rsDB.getStuffToCheck( 'Sites', name = site )
        if rssSite:
          rssSite = rssSite[0]
          token   = rssSite[4]
          if token == 'RS_Hold':
            gLogger.info( '      %s : test %d, at %s' % ( testStatus, testID , site ) )
            self.rsDB.setToken( 'Site', site, 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59) )  
            gLogger.info( '      unlock : Ok ' )      
        
      #elif testStatus == 'error':  
      #  gLogger.info( '      error : test %d, at %s (returned error)' % ( testID , site ) ) 
      #  self.rsDB.setToken( 'Site', site, 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59) )  
      #  gLogger.info( '      unlock : Ok ' )  
  
      else:
        
        gLogger.info( 'updateOngoingSlave, no idea what happened' )
  
    return S_OK()
  
#############################################################################  
  
  def _updateLastFinishedTestsSlave( self ):
    '''
      Seems it will not be needed
    '''
    pass
  
#############################################################################  
  
  def _checkQuarantineSlave( self ):
    
    gLogger.info( '>>> CHECKING QUARANTINE' )
    
    failures = self.rsaDB.getTestList( agentStatus = 'HCfailure', counter = 0 )
    prevents = self.rsaDB.getTestList( agentStatus = 'HCprevent', counter = 0 )

    if failures:
           
      f = failures[0]   
      quarantine  = ( 1 + 2 * f[8] ) * self.TEST_DURATION                                     
      endQuarant  = f[9] + timedelta( hours = quarantine ) 

      return S_OK( value = ( True, '  Quarantine ( test %s ) until %s counter %s' % ( f[0], endQuarant, f[7] )))  

    #############################################################################
    ## PREVENTS SECTION
    #############################################################################
    
    if prevents:
      
      formerAgentStatus = 'HCprevent'
      
      p = prevents[0]
        
      prevention  = ( 1 + 2 * p[8] ) * self.TEST_DURATION                                     
      endPrevent  = p[9] + timedelta( hours = prevention ) 
        
      return S_OK( value = ( False, '  Preventive submission ( test %s ) until %s counter %s.' % ( p[0], endPrevent, p[8] )))
   
    return S_OK(value = (False,'+ No worries'))
    
#############################################################################  
  
  def _submitTestsSlave( self ):
    '''
      Try to submit a test for every Probing site with token RS_SVC.
    '''
    
    sites = self.rsDB.getStuffToCheck('Sites')
    
    title = '>>> SUBMITTING TESTS'
    if self.setup == 'LHCb-Production':
      title += ' ( QUARANTINE )'
    gLogger.info( title )
      
#    siteFlag = False  
      
    for siteTuple in sites:

      if siteTuple[1] == 'Probing':
        if siteTuple[4] == 'RS_SVC':

          site = siteTuple[0]
                  
          gLogger.info( '  + %s is Probing and has token RS_SVC' % site )  
            
          if self.rsaDB.getTestList( siteName = site, agentStatus = 'HCongoing' ):
            gLogger.info( '    already has an ongoing test' )
#            siteFlag = True
            #continue

#          elif self.rsaDB.getTestList( siteName = site, agentStatus = 'HCscheduled' ):
            #print '  + %s already has an scheduled test'%site
#            siteFlag = True
            #continue
          
          elif self.rsaDB.getTestList( siteName = site, agentStatus = 'HCskipped' ):
            gLogger.info( '    is waiting for a Human action' )
            #continue
            
          else:
            gLogger.info( '    is waiting for a test' )  
                            
#    if not siteFlag:
#      gLogger.info( '  + No Probing & unlocked sites.' )  
    
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

    failures = self.rsaDB.getTestList( agentStatus = 'HCfailure', counter = 0 )
    prevents = self.rsaDB.getTestList( agentStatus = 'HCprevent', counter = 0 )
    
    now = datetime.now()
    
    agentStatus  = 'HCfailure'
    counter      = 1
    counterTime  = now  

    if len( failures ) > 1:
      gLogger.exception( 'More than 1 failure: %s' % failures )
      return S_ERROR( 'Check Error Counter' )
    
    if len( prevents ) > 1:
      gLogger.exception( 'More than 1 failure: %s' % failures )
      return S_ERROR( 'Check Error Counter' )

    if failures and prevents:
      gLogger.exception( 'Failures and prevents at the same time ! %s, %s' % ( failures, prevents ))
      return S_ERROR( 'Check Error Counter' )
    
    if failures:
      
      f           = failures[0]
      agentStatus = 'HCquarantine'
      counter     = f[8]
      
    elif prevents:
      
      p                 = prevents[0]
      agentStatus       = 'HCfailure'
      formerAgentStatus = 'HCprevent'
      counter           = p[8] + 1

      if counter > self.MAX_COUNTER_ALARM:
        
        # After a considerable number of jumps, we put the site aside
        # and alert an administrator to investigate.
        
        agentStatus  = 'HCskipped'
        
        subject = '[HCAgent][MAX COUNTER ALARM] at %s' % p[1]
        body    = 'You should take a look...'
        self.__sendEmail( subject, body )
        
      #Update former test
      self.rsaDB.updateTest( p[5], agentStatus = 'HCquarantine', formerAgentStatus = formerAgentStatus )     
    
    return ( counter, agentStatus, counterTime )           
           
#############################################################################

  def __checkStartTime(self, site):
    '''
    Check if there is any HClastfinished test and take its counter and endtime. If not, counter is 0
    and endtime = now.
    Set next starttime for this site at endtime + 2 * counter * TEST_DURATION 
    '''
    
    lastFinished = self.rsaDB.getTestList( agentStatus = 'HClastfinished', siteName = site, counter = 0 )
    
    if len( lastFinished ) > 1:
      gLogger.info( 'More than 1 last finished: %s' % lastFinished )
      S_ERROR( 'MORE THAN 1 HClastfinished' )
      
    counter     = 1 
    agentStatus = 'HCscheduled'
    starttime   = datetime.now()
    
    if lastFinished:
      
      lF = lastFinished[0]
      
      counter     = lF[8]
      endTime     = lF[7]
      counterTime = lF[9]
    
      period    = 2 * counter * self.TEST_DURATION

#      starttime = endTime + timedelta( hours = period )
      starttime = counterTime + timedelta( hours = period )
   
      counter += 1 
      
      if counter > self.MAX_COUNTER_ALARM:
        
        # After a considerable number of jumps, we put the site aside
        # and alert an administrator to investigate.
        
        #agentStatus  = 'HCskipped'
        
        subject = '[HCAgent][MAX COUNTER ALARM] at %s' % lF[1]
        body    = 'You should take a look...'
        self.__sendEmail(subject,body)
    
    return ( counter, agentStatus, starttime )                    

#############################################################################
           
  def __checkFinishedTest(self,testID,site):         
    '''
    If there is any HClastfinished for this site, move its status to HCfinished,
    and set the new test to HClastfinished.
    '''           
           
    lastFinished = self.rsaDB.getTestList( agentStatus = 'HClastfinished', siteName = site )         
    formerAgentStatus = 'HClastfinished'       
           
    agentStatus = 'HClastfinished'       
           
    if len( lastFinished ) > 1:
      gLogger.exception( 'More than 1 last finished: %s' % lastFinished )
      return S_ERROR( 'Check Agent Break' )
           
    if lastFinished:
      lastF        = lastFinished[0]
      lastF_testID = lastF[0]
      
      now = datetime.now()
      
      self.rsaDB.updateTest( None, testID = lastF_testID, agentStatus = 'HCfinished', 
                             formerAgentStatus = formerAgentStatus, counterTime = now )
           
    return agentStatus
              
#############################################################################              
              
  def __sendEmail( self, subject, body ):
    '''
    Every time this agent sends an email, Chuck eats a kitten. 
    '''
    sendMail = self.diracAdmin.sendMail('aebeda3@gmail.com',subject,body)
    if not sendMail['OK']:
      gLogger.error( 'Error sending email' )

#############################################################################              
              
  def __detectErrors( self, res, time, site, formerAgentStatus ):
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
    agentStatus = 'HCskipped'
    self.rsaDB.updateTest( time, agentStatus = agentStatus, formerAgentStatus = formerAgentStatus )
    
    # Release token
    self.rsDB.setToken( 'Site', site, 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59) )            
    gLogger.info( '        unlock  : Ok' )
    
    return True            

#############################################################################
                            
  '''      
  EXECUTE FUNCTIONS
  
  a) execute
  b) _executeMaster
  c) _executeSlave
  '''  

#############################################################################
              
  def execute( self ):
    """
      The main HCAgent execution method.
      The agent shows a completely different behavior depending on the setup.
      If it is LHCb-Production, we submit tests. If not, it just prints some
        information. 
    """
    
    if self.setup == 'LHCb-Production':
      gLogger.info( 'MASTER EXECUTION' )
      self._executeMaster()
      
    else:
      gLogger.info( 'SLAVE EXECUTION' )
      self._executeSlave()     
      
    return S_OK()             
              
#############################################################################

  def _executeMaster( self ):
    """
      The master HCAgent execution method.
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
      self._updateScheduledTestsMaster()
      gLogger.info( '  + ongoings' )
      self._updateOngoingTestsMaster()
      gLogger.info( '  + lastFinished' ) 
      self._updateLastFinishedTestsMaster()

      #############################################################################
      # CHECK IF WE ARE ON QUARANTINE
      #############################################################################
      
      res = self._checkQuarantineMaster()
      
      if not res['OK']:
        return S_ERROR(res['Value'])
      
      gLogger.info( '  '+res['Value'][1] )
     
      #############################################################################
      # SUBMIT TESTS
      #############################################################################
     
      # If we are on quarantine, we do not send tests.
      if res['Value'][0]:
      #  return S_OK(res['Value'][1])  
        self._submitTestsSlave()
      else:
        self._submitTestsMaster()
  
      return S_OK()    
      
    except Exception, x:
      errorStr = where(self, self.execute)
      gLogger.exception(errorStr, lException = x)
  
#############################################################################

  def _executeSlave( self ):
    """
      The slave HCAgent execution method.
    """
        
    try:

      #############################################################################
      # UPDATE
      #############################################################################
      
      gLogger.info( '>>> UPDATING TESTS' )
      gLogger.info( '  + scheduled' )
      self._updateScheduledTestsSlave()
      gLogger.info( '  + ongoings' )
      self._updateOngoingTestsSlave()
      #gLogger.info( '  + lastFinished' ) 
      #self._updateLastFinishedTestsSlave()

      #############################################################################
      # CHECK IF WE ARE ON QUARANTINE
      #############################################################################
      
      #res = self._checkQuarantineSlave()
      #
      #if not res['OK']:
      #  return S_ERROR(res['Value'])
      #
      #gLogger.info( '  '+res['Value'][1] )
     
      #############################################################################
      # SUBMIT TESTS
      #############################################################################
     
      ## If we are on quarantine, we do not send tests.
      #if res['Value'][0]:
      #  return S_OK(res['Value'][1])  
      #
      #self._submitTestsSlave()
      # 
      return S_OK()    
      
    except Exception, x:
      errorStr = where(self, self.execute)
      gLogger.exception(errorStr, lException = x)

#############################################################################