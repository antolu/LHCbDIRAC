#"""
#LHCbDIRAC/ResourceStatusSystem/Agent/HCModes/HCMasterMode.py
#"""
#
#__RCSID__ = "$Id$"
#
## First, pythonic stuff
#import time
#from datetime import datetime, timedelta
#
## Second, DIRAC stuff
#from DIRAC import gLogger, S_OK, S_ERROR
#
## Third, LHCbDIRAC stuff
#from LHCbDIRAC.ResourceStatusSystem.Agent.HCModes.HCBaseMode import HCBaseMode
#
#class HCMasterMode( HCBaseMode ):
#  
#  def __init__( self, *args, **kwargs ):
#    super( HCMasterMode, self ).__init__( *args, **kwargs )
#    
#    self.TIME_FORMAT   = self._getTimeFormat()['Value'] 
#    self.LOCK_TOKEN    = self._getLockToken()['Value']
#    self.UNLOCK_TOKEN  = self._getUnLockToken()['Value']
#  
#################################################################################  
#  
#  def updateScheduledTests( self ):
#    '''
#    This function processes all tests with status HCscheduled.
#    If test.starttime < now or test.endtime < now, queries HC for
#    fresh data. If the data is out of synch, we syncronize it.
#    
#    If test.endtime < now is bad. Means that test is sheduled, but
#    test.endtime is on the past. Mark the test as failed, and unlock it.
#    
#    If test.starttime < now, means the test has already started running.
#    We do not care here if crashed on HC or not, we just lock the site
#    and set it as HCongoing. Next function will handle it.  
#    '''
#    
#    scheduled         = self._getTestList( agentStatus = 'HCscheduled' )['Value']
#    formerAgentStatus = 'HCscheduled'
#    
#    if not scheduled:
#      gLogger.info( '      no scheduled tests' )
#      return S_OK()
#             
#    for sc in scheduled:
#      
#      testID    = sc[0]
#      site      = sc[1]
#      resource  = sc[2]
#      submitime = sc[6]     
#      starttime = sc[7] 
#      endtime   = sc[8]  
#           
#      resource_log = ''
#      if resource:
#        resource_log = '( %s )' % resource     
#           
#      if not isinstance( testID, long ):
#        gLogger.info( 'There is a NULL ID with status Ongoing' )
#        subject = '[HCAgent][NULL HCscheduled]'
#                 
#        body = '%s %s is NULL HCscheduled \n %s' % ( site, resource_log, sc )
#        self._sendEmail( subject, body )  
#        continue     
#
#      starttime = starttime.replace( second = 0 ) 
#      endtime   = endtime.replace( second = 0 )
#           
#      gLogger.info( '      scheduled : on %s %s at %s' % ( site, resource_log, starttime ) )
#      
#      now = datetime.now().replace( microsecond = 0 ).replace( second = 0 )
#      
#      if starttime < now or endtime < now:
#         
#        res = self.hc.getTest( testID )
#        if not self._detectErrors( res, submitime, site, resource, formerAgentStatus )['Value']:
#          
#          test       = res[1]['response']['test'] 
#          testStatus = test['state']        
#          startTime  = datetime.strptime( test['starttime'], self.TIME_FORMAT ).replace( second = 0 )
#          endTime    = datetime.strptime( test['endtime'], self.TIME_FORMAT ).replace( second = 0 ) 
#
#          # IF it is different, it has been modified. Update.
#          if endTime != endtime:
#            gLogger.info( ' Updated endTime %s -> %s' % ( endtime, endTime ) )          
#            self._updateTest( submitime, endTime = endTime )  
#        
#          # If endTime is in HC < now, there is a problem in HC
#          elif endTime < now:
#            gLogger.error( '        endtime ( %s ) < now ( %s )' % ( endtime, now ) )
#            gLogger.error( '        error   : test is scheduled, but endtime < now' )
#
#            ec = self._checkErrorCounter( )
#            
#            if ec['OK']: 
#              counter, agentStatus, counterTime = ec['Value']
#            else: 
#              agentStatus = 'HCquarantine'
#                     
#            self._updateTest( submitime, agentStatus = agentStatus, formerAgentStatus = formerAgentStatus, 
#                                   counter = counter, counterTime = counterTime )   
#            subject = '[HCAgent][updateScheduled][Endtime]'
#            body = '%s ( %s ) is scheduled but endtime ( %s ) < now ( %s )' % ( site, resource, endtime, now )
#    
#            self._sendEmail( subject, body ) 
#               
#            continue    
#        
#          if starttime != startTime:
#            gLogger.info( ' Updated startTime %s -> %s' % ( starttime, startTime ) )          
#            self._updateTest( submitime, startTime = startTime )  
#       
#          elif testStatus not in ['tobescheduled','scheduled']:
#            gLogger.info( '        starttime ( %s ) < now ( %s )' % ( starttime, now ) )
#            
#            if resource == '':
#              self._lockToken( 'Site', site, endtime )
#            else:
#              self._lockToken( 'Resource', resource, endtime)
#                            
#            gLogger.info( '        ongoing : Ok' )          
#            # We move it to ongoing. UpdateOnGoingTests will deal with possible failures
#            self._updateTest( submitime, agentStatus = 'HCongoing', formerAgentStatus = formerAgentStatus ) 
#              
#    return S_OK()  
#
#################################################################################           
#           
#  def updateOngoingTests(self):
#    '''
#      First of all, check all tests registered as HCongoing on
#      the HCAgent table.
#      
#      If there are no ongoing tests, nothing to do here.
#      
#      If there are tests ongoing, ask HammerCloud for an update.
#        OPTIONS:
#          o1: HammerCloud can return a NOK. 
#            -> Still unclear how to handle it.
#          o2: Test status is error.
#            -> checkCounterError and update.
#          o3: Test endTime is on the past, and test status reported by HC ! [completed,error]
#            -> checkCounterError and update
#          o4: Test status is completed
#            -> checkFinishedTest and update
#          o5: Test status is something else.
#            -> update 
#    '''
#    
#    ongoings     = self._getTestList( agentStatus = 'HCongoing' )['Value']
#    formerAgentStatus = 'HCongoing'
#    
#    if not ongoings:
#      gLogger.info( '      no ongoing tests' )
#      return S_OK()
#
#    for og in ongoings:
#      
#      testID_db         = og[0]
#      site_db           = og[1]
#      resource_db       = og[2]
#      submissionTime_db = og[6]
#      endTime_db        = og[8]
#      
#      resource_log = ''
#      if resource_db:
#        resource_log = '( %s )' % resource_db
#      
#      if not isinstance( testID_db, long ):
#        gLogger.info( 'There is a NULL ID with status Ongoing' )
#        subject = '[HCAgent][NULL HCongoing]'
#        body = '%s ( %s ) is NULL HCongoing \n %s' % ( site_db, resource_log, og )
#        self._sendEmail( subject, body )  
#        continue
#      
#      res = self.hc.getTest( testID_db )
#
#      if self._detectErrors( res, submissionTime_db, site_db, resource_db, formerAgentStatus )['Value']:
#        continue
#          
#      test       = res[1]['response']['test']
#         
#      testID     = test['id']
#      testStatus = test['state']        
#      site       = test['site']
#      startTime  = datetime.strptime(test['starttime'], self.TIME_FORMAT)
#      endTime    = datetime.strptime(test['endtime'], self.TIME_FORMAT)
#      
#      counter      = None
#      agentStatus  = 'HCongoing'  
#      counterTime  = endTime
#        
#      ## SANITY CHECK
#      ## Site used in HC must be the same as we have on the HCAgent table
#      if site_db != site:
#        gLogger.info( '      sanity check failed: test %d has %s (HC retrieved),%s (HCAgent table)' % ( testID, site_db, site ) )
#        if resource_db == '':
#          self._unLockToken( 'Site', site )
#        else:
#          self._unLockToken( 'Resource', resource_db )
#            
#        testStatus = 'error'
#        gLogger.info( '      marking as error, until problem is clarified' )
#        
#        '''
#          We should kill the test in HC
#        '''
#      
#      # We do not care about seconds
#      if endTime.replace( second = 0 ) != endTime_db.replace( second = 0 ):
#        gLogger.warn( '      endTime modified : test %d has %s (HC retrieved),\
#                        %s (HCAgent table)' % ( testID, endTime_db, endTime ) )
#        #self._lockSite( site, endTime )
#        
#      if testStatus == 'error':
#          
#        gLogger.info( '      error : test %d, at %s %s (returned error)' % ( testID , site, resource_log ) )  
#        if resource_db == '':
#          self._unLockToken( 'Site', site )
#        else:
#          self._unLockToken( 'Resource', resource_db )     
#        
#        ec = self._checkErrorCounter( )
#            
#        if ec['OK']: 
#          counter, agentStatus, counterTime = ec['Value']
#        else: 
#          # IF there is something bad, put it into quarantine
#          agentStatus = 'HCquarantine'  
#          
#      # HC needs some time to end the test, depending on the number of jobs to be killed, etc..
#      # We are giving half of the test duration time, or 15 min (the maximum)
#      elif testStatus != 'completed' and datetime.now() - timedelta(hours = max( self.testDuration * 0.5, 0.25) ) > endTime:
#          
#        testStatus = 'error'  
#        
#        gLogger.info( '      error : test %d, at %s %s (timeout without completing)' % ( testID , site, resource_log ) )
#        
#        if resource_db == '':
#          self._unLockToken( 'Site', site )
#        else:
#          self._unLockToken( 'Resource', resource_db )
#        
#        ec = self._checkErrorCounter( )
#            
#        if ec['OK']: 
#          counter, agentStatus, counterTime = ec['Value']
#        else: 
#          # IF there is something wrong, put it into quarantine
#          agentStatus = 'HCquarantine'  
#        
#      elif testStatus == 'completed':
#        
#        ft = self._checkFinishedTest( testID, site, resource_db )
#        
#        if ft['OK']:
#          agentStatus = ft['Value']
#        else:
#          # This will increase the problem. Hopefully someone will read the alert email.
#          # Otherwise, good luck Mr. Gorsky
#          agentStatus = 'HClastfinished'
#             
#        gLogger.info( '      completed : test %d, at %s %s' % ( testID , site, resource_log ) )    
#        if resource_db == '':
#          self._unLockToken( 'Site', site )
#        else:
#          self._unLockToken( 'Resource', resource_db )
#
#      else:
#        # Nothing special happened
#        gLogger.info( '      %s : test %d, at %s %s with start time %s' % ( testStatus, testID, site, resource_log, startTime ) )
#        formerAgentStatus = None
#        
#      self._updateTest( None, testID = testID, testStatus = testStatus, startTime = startTime, 
#                        endTime = endTime, agentStatus = agentStatus, formerAgentStatus = formerAgentStatus, 
#                        counter = counter, counterTime = counterTime )  
#      
#    return S_OK()   
#
################################################################################# 
#
#  def updateLastFinishedTests( self ):
#    '''
#      Check all tests registered as HClastfinished on
#      the HCAgent table (max one per site).
#      
#      If there are no last finished tests, nothing to do here.
#      
#      If there are, check counterTime and counter. This means,
#      if last counter update was at least 2 * counter * testDuration hours in
#      the past, decrease counter ( min is zero).
#      
#      The HClastfinished status is used to prevent site overflood when tests
#      are completing, but not successful according to policies. 
#    '''
#    
#    lastFinished      = self._getTestList( agentStatus = 'HClastfinished', counter = 0 )['Value']
#    formerAgentStatus = 'HClastfinished'
#    
#    if not lastFinished:
#      gLogger.info( '      no lastFinished' )
#      return S_OK()
#    
#    now = datetime.now()
#    
#    for lastF in lastFinished:
#      
#      testID      = lastF[0]
#      site        = lastF[1]
#      resource    = lastF[2]
#      endtime     = lastF[8]
#      counter     = lastF[9]
#      counterTime = lastF[10]
#      
#      resource_log = ''
#      if resource:
#        resource_log = '( %s )' % resource
#      
#      period      = 2 * counter * self.testDuration
#      
#      afterPeriod = counterTime + timedelta( hours = period )      
#      
#      gLogger.info( '      completed: test %s, at %s %s at %s' % ( testID, site, resource_log, endtime ) ) 
#      gLogger.info( '        counter %s and counterTime %s ' % ( counter, counterTime ) )
#      
#      if now > afterPeriod:
#        
#        counter    -= 1
#        counterTime = now
#    
#        gLogger.info( '        updating test %s counter to %s, with counterTime %s' % ( testID, counter, afterPeriod ) )
#    
#        agentStatus = None
#        if counter == 0:
#          agentStatus = 'HCfinished'
#    
#        self._updateTest( None, testID = testID, counter = counter, counterTime = counterTime, 
#                               agentStatus = agentStatus, formerAgentStatus = formerAgentStatus )
#    
#    return S_OK()
#            
#################################################################################
#
#  def checkQuarantine( self ):
#    '''
#      There are two types of quarantine, blocking and preventive.
#      
#      Whenever a test fails, it's state is moved to HCfailure. This agent
#      waits ( 1 + 2 * counter ) * TEST_TIME hours to submit again a test.
#      
#      After that period, the agent allows submission again, but if there is
#      a failure, we will increment the counter and move to blocking mode.
#      
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
#      body = 'Failures and prevents at the same time ! %s, %s' % ( failures, prevents )
#      gLogger.exception( body )
#      self._sendEmail( '[HCAgent]', body )
#      return S_ERROR( 'Check Agent Break' )
#      
#    now = datetime.now()
#
#    ############################################################################
#    ## FAILURES SECTION
#    ############################################################################
#    
#    if failures:
#      
#      formerAgentStatus = 'HCfailure'
#      
#      f = failures[0]   
#      
#      testID         = f[0]
#      submissionTime = f[6]
#      counter        = f[9]
#      counterTime    = f[10]
#      
#      quarantine  = ( 1 + 2 * counter ) * self.testDuration                                     
#      endQuarant  = counterTime + timedelta( hours = quarantine ) 
#      endPrevent  = endQuarant + timedelta( hours = quarantine )  
#
#      if now < endQuarant:          
#        return S_OK( ( True, '  Quarantine ( test %s ) until %s: \n    counter %s and counterTime %s' % ( testID, endQuarant, counter, counterTime )))  
# 
#      elif now < endPrevent:
#        self._updateTest( submissionTime, agentStatus = 'HCprevent', formerAgentStatus = formerAgentStatus, counterTime = now )
#        return S_OK( ( False, '  Preventive submission ( test %s ) until %s: \n    counter %s and counterTime %s' % ( testID, endPrevent, counter, counterTime )))
#
#      else:
#        gLogger.error( 'Failures > endPrevent' )
#        return S_ERROR( 'Check Agent Break' )
#
#    ############################################################################
#    ## PREVENTS SECTION
#    ############################################################################
#    
#    if prevents:
#      
#      formerAgentStatus = 'HCprevent'
#      
#      p = prevents[0]
#      
#      testID         = p[0]
#      submissionTime = p[6]
#      counter        = p[9]
#      counterTime    = p[10]
#        
#      prevention  = ( 1 + 2 * counter ) * self.testDuration                                     
#      endPrevent  = counterTime + timedelta( hours = prevention ) 
#        
#      pollingTime = self.am_getPollingTime()
#      firstAfterP = endPrevent + timedelta( seconds = pollingTime )
#        
#      if now < endPrevent:
#        return S_OK( value = ( False, '  Preventive submission ( test %s ) until %s:\n    counter %s and counterTime %s' % ( testID, endPrevent, counter, counterTime )))
#        
#      elif now < firstAfterP:
#        counter = counter - 1
#        agentStatus = None
#        messag = '  one more round of preventive submission.'
#          
#        if counter == 0:
#          agentStatus = 'HCquarantine'
#          messag = '  end of quarantine and prevention time.'
#            
#        self._updateTest( submissionTime, counter = counter, agentStatus = agentStatus, formerAgentStatus = formerAgentStatus )  
#        return S_OK( ( False, messag ))   
#    
#      else:
#        gLogger.error( 'Prevents > firstAfterP' )
#        return S_ERROR( 'Check Agent Break')
#   
#    return S_OK( ( False,'+ No worries' ) )
#
#################################################################################
#
#  def submitTests( self ):
#
#    gLogger.info( '>>> SUBMITTING TESTS' )
#
#    toSite = self.submitTestsToSite()
#    if toSite['OK']:
#      return self.submitTestsToCE()
#    return toSite  
#
#################################################################################
#
#  def submitTestsToSite( self ):
#    '''
#      Try to submit a test for every Probing site with token RS_SVC.
#    '''
#    
#    gLogger.info( '>>> SITES' )
#    
#    sites = self._getSites('Sites')
#      
#    siteFlag = False  
#      
#    for siteTuple in sites:
#
#      if siteTuple[1] == 'Probing':
#        if siteTuple[4] == self.UNLOCK_TOKEN :
#
#          site = siteTuple[0]
#          
#          now  = datetime.now()
#            
#          if self._getTestList( siteName = site, resourceName = '', agentStatus = 'HCongoing' )['Value']:
#            gLogger.info( '  + %s\n      already has an ongoing test' % site )
#            continue
#
#          if self._getTestList( siteName = site, resourceName = '', agentStatus = 'HCscheduled' )['Value']:
#            continue
#          
#          if self._getTestList( siteName = site, resourceName = '', agentStatus = 'HCskipped' )['Value']:
#            gLogger.info( '  + %s\n      is waiting for a Human action' % site )
#            continue  
#
#          """
#            Check if we have to set a different starttime for the test.
#          """
#          
#          st = self._checkStartTime( site, '' )
#          
#          if st['OK']: 
#            counter, agentStatus, starttime = st['Value']
#          else: 
#            # IF there is something bad, put it into skipped
#            agentStatus = 'HCskipped'     
#
#          if not agentStatus == 'HCscheduled':
#            continue
#            
#          self._addTest( site, now, agentStatus, '' )
#                   
#          siteFlag = True
#          
#          """
#             Duration must be an INT (put it into seconds)
#          """
#          durationSecs = int( self.testDuration * 3600 )
#
#          res = self.hc.sendTest( siteTuple[0], starttime = starttime , duration = durationSecs, template = 2 )                         
#            
#          # If we detect any error, we stop submitting until next round  
#          if self._detectErrors( res, now, site, '', 'HCscheduled' )['Value']:
#            return S_ERROR()  
#            
#          gLogger.info( '      submission : Ok' )
#              
#          resp = res[1]['response']    
#              
#          testStatus = resp['status']
#          startTime  = resp['starttime']
#          endTime    = resp['endtime']
#          testID     = resp['id']
#                           
#          self._updateTest(now, testID = testID, testStatus = testStatus, startTime = startTime, 
#                                   endTime = endTime, agentStatus = agentStatus, formerAgentStatus = 'Unspecified',
#                                   counterTime = endTime, counter = counter)
#          
#          # Do not bother excessively HC, and also ensure submissionTime is different !!
#          time.sleep( 5 )
#                            
#    if not siteFlag:
#      gLogger.info( '  + No Probing & unlocked sites.' )  
#    
#    return S_OK()    
#  
#################################################################################
#
#  def submitTestsToCE( self ):
#    '''
#      Try to submit a test for every Probing site with token RS_SVC.
#    '''
#    
#    gLogger.info( '>>> CEs' )
#    
#    ces = self._getResources( resourceType = 'CE', status = 'Probing', tokenOwner = self.UNLOCK_TOKEN )
#    
#    ceFlag = False
#    
#    for ce in ces:
#      
#      resourceName = ce[0]
#      resourceType = ce[1]
#      serviceType  = ce[2]
#      siteName     = ce[3]
#      gridSiteName = ce[4]
#      status       = ce[5]
#      reason       = ce[6]
#      tokenOwner   = ce[7]
#       
#      gLogger.info( '  + %s ( %s )' % ( siteName, resourceName ))   
#
#      now  = datetime.now()
#            
#      if self._getTestList( siteName = siteName, resourceName = resourceName, agentStatus = 'HCongoing' )['Value']:
#        gLogger.info( '      already has a ongoing test on this CE' )
#        continue
#
#      if self._getTestList( siteName = siteName, resourceName = resourceName, agentStatus = 'HCscheduled' )['Value']:
#        continue
#
#      if self._getTestList( siteName = siteName, resourceName = resourceName, agentStatus = 'HCskipped' )['Value']:
#        gLogger.info( '      this CE is waiting for a Human action' )
#        continue  
#
#      st = self._checkStartTime( siteName, resourceName )
#      if st['OK']: 
#        counter, agentStatus, starttime = st['Value']
#      else: 
#        # IF there is something bad, put it into skipped
#        agentStatus = 'HCskipped'     
#
#      if not agentStatus == 'HCscheduled':
#        gLogger.info( agentStatus )
#        continue
#          
#      self._addTest( siteName, now, agentStatus, resourceName )
#      
#      ceFlag = True
#
#      """
#        Duration must be an INT (put it into seconds)
#      """
#      durationSecs = int( self.testDuration * 3600 )
#
#      extraargs = "-o[defaults_Dirac]diracOpts=j._addJDLParameter('GridRequiredCEs','%s')" % resourceName 
#
#      res = self.hc.sendTest( siteName, starttime = starttime , duration = durationSecs, 
#                              template = 2, extraargs = extraargs )                         
#            
#      # If we detect any error, we stop submitting until next round  
#
#      if self._detectErrors( res, now, siteName, resourceName, 'HCscheduled' )['Value']:
#        return S_ERROR()  
#            
#      gLogger.info( '      submission : Ok' )
#              
#      resp = res[1]['response']    
#            
#      testStatus = resp['status']
#      startTime  = resp['starttime']
#      endTime    = resp['endtime']
#      testID     = resp['id']
#                           
#      self._updateTest( now, testID = testID, testStatus = testStatus, startTime = startTime, 
#                       endTime = endTime, agentStatus = agentStatus, formerAgentStatus = 'Undefined',
#                       counterTime = endTime, counter = counter)
#          
#      # Do not bother excessively HC, and also ensure submissionTime is different !!
#      time.sleep( 5 )
#
#
#    if not ceFlag:
#      gLogger.info( '  + No Probing & unlocked CEs.' )  
#    
#    return S_OK()      
#
#################################################################################
#
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF