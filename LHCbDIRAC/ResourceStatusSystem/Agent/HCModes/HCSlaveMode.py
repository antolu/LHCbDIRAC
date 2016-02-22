#from DIRAC import gLogger, S_OK, S_ERROR
#from LHCbDIRAC.ResourceStatusSystem.Agent.HCModes.HCBaseMode import HCBaseMode
#
#from datetime import datetime, timedelta
#
##import time
#
#class HCSlaveMode( HCBaseMode ):
#
#  def __init__( self, *args, **kwargs ):
#    super( HCSlaveMode, self ).__init__( *args, **kwargs )
#
#    self.TIME_FORMAT   = self._getTimeFormat()['Value']
#    self.LOCK_TOKEN    = self._getLockToken()['Value']
#    self.UNLOCK_TOKEN  = self._getUnLockToken()['Value']
#
##############################################################################
#
#  def updateScheduledTests( self ):
#
#    scheduled         = self._getTestList( agentStatus = 'HCscheduled' )['Value']
#    formerScheduled   = self._getTestList( formerAgentStatus = 'HCscheduled' )['Value']
#
#    formerAgentStatus = 'HCscheduled'
#
#    if not scheduled:
#      gLogger.info( '      no scheduled tests' )
#
#    now = datetime.now()
#
#    for sc in scheduled:
#
#      testID    = sc[0]
#      site      = sc[1]
#      starttime = sc[6]
#
#      gLogger.info( '      scheduled : test %s on %s at %s' % ( testID, site, starttime ) )
#
#
##      if starttime + timedelta( hours = max( self.testDuration * 0.5, 0.25) ) < now :
##        gLogger.error( 'It seems that the MASTER agent is not running' )
##        subject = '[HCAgentSlave][updateScheduled]'
##        body = '( %s - %s ). MASTER seems to not run' % ( testID, site )
##        self._sendEmail( subject, body )
##        return S_ERROR()
#
#    for fs in formerScheduled:
#
#      testID      = fs[0]
#      site        = fs[1]
#      agentStatus = fs[3]
#      starttime   = fs[6]
#      endtime     = fs[7]
#      counterTime = fs[9]
#
#      if not isinstance( fs[0], long ):
#        continue
#
#      if agentStatus == 'HCongoing':
#
#        rssSite = self._getResources( 'Sites', name = site )
#        if rssSite:
#          rssSite = rssSite[0]
#          token   = rssSite[4]
#          if token == self.UNLOCK_TOKEN:
#
#            gLogger.info( '      scheduled : test %s on %s at %s' % ( testID, site, starttime ) )
#
##            self._lockSite( site, endtime )
#              #self.rsDB.setToken( 'Site', site, 'RS_Hold', endtime )
#              #gLogger.info( '        locked  : Ok' )
#            gLogger.info( '        ongoing : Ok' )
#
#          #else:
#          #
#          #  gLogger.error( '%s on %s, should not be locked' % ( token, site ))
#
#      else:
#
#        ignoreAfter = counterTime + timedelta( seconds = self.pollingTime )
#
#        if now < ignoreAfter :
#
#          body = 'scheduled : test %s on %s at %s, error %s' % ( testID, site, starttime, agentStatus )
#          gLogger.exception( '      ' + body )
#          self._sendEmail( '[HCAgent]', body )
#
#    return S_OK()
#
##############################################################################
#
#  def updateOngoingTests( self ):
#
#    ongoing         = self._getTestList( agentStatus = 'HCongoing' )['Value']
#    formerOngoing   = self._getTestList( formerAgentStatus = 'HCongoing' )['Value']
#
#    if not ongoing:
#      gLogger.info(  '      no ongoing tests'  )
#
#    now = datetime.now()
#
#    for og in ongoing:
#
#      testID      = og[0]
#      site        = og[1]
#      testStatus  = og[2]
#      agentStatus = og[3]
#      startTime   = og[6]
#      endtime     = og[7]
#
#      if not isinstance( og[0], long ):
#        continue
#
#      gLogger.info( '      %s : test %d, at %s with start time %s' % ( testStatus, testID, site, startTime ) )
#
##      if endtime + timedelta( hours = max( self.testDuration * 0.5, 0.25) ) < now :
##        gLogger.error( 'It seems that the MASTER agent is not running' )
##        subject = '[HCAgentSlave][UpdateOngoing]'
##        body = '( %s - %s ). MASTER seems to not run' % ( testID, site )
##        self._sendEmail( subject, body )
##        return S_ERROR()
#
#      #gLogger.info( '      %s : test %d, at %s' % ( testStatus, testID, site ) )
#
#    for fo in formerOngoing:
#
#      testID      = fo[0]
#      site        = fo[1]
#      testStatus  = fo[2]
#      agentStatus = fo[3]
#      counterTime = fo[9]
#
#      if testStatus == 'completed' or testStatus == 'error':
#        rssSite = self._getResources( 'Sites', name = site )
#        if rssSite:
#          rssSite = rssSite[0]
#          token   = rssSite[4]
#          if token == self.UNLOCK_TOKEN:
#            gLogger.info( '      %s : test %d, at %s' % ( testStatus, testID , site ) )
#            self._unLockSite( site )
#            #self.rsDB.setToken( 'Site', site, 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59) )
#            #gLogger.info( '      unlock : Ok ' )
#
#      #elif testStatus == 'error':
#      #  gLogger.info( '      error : test %d, at %s (returned error)' % ( testID , site ) )
#      #  self.rsDB.setToken( 'Site', site, 'RS_SVC', datetime(9999, 12, 31, 23, 59, 59) )
#      #  gLogger.info( '      unlock : Ok ' )
#
#      else:
#
#        ignoreAfter = counterTime + timedelta( seconds = self.pollingTime )
#
#        if now < ignoreAfter :
#
#          #subject = 'scheduled : test %s on %s at %s, error %s' % ( testID, site, starttime, agentStatus )
#          body = 'updateOngoing, no idea what happened with %s' % str(fo)
#          gLogger.exception( '      ' + body )
#          self._sendEmail( '[HCAgent]', body )
#
#          self._unLockSite( site )
#
#        #gLogger.info( 'updateOngoingSlave, no idea what happened' )
#
#    return S_OK()
#
##############################################################################
#
#  def updateLastFinishedTests( self ):
#    return S_OK()
#
##############################################################################
#
#  def checkQuarantine( self ):
#
#    #gLogger.info( '>>> CHECKING QUARANTINE' )
#
#    failures = self._getTestList( agentStatus = 'HCfailure', counter = 0 )['Value']
#    prevents = self._getTestList( agentStatus = 'HCprevent', counter = 0 )['Value']
#
#    if failures:
#
#      f = failures[0]
#      quarantine  = ( 1 + 2 * f[8] ) * self.testDuration
#      endQuarant  = f[9] + timedelta( hours = quarantine )
#
#      return S_OK( ( True, '  Quarantine ( test %s ) until %s counter %s' % ( f[0], endQuarant, f[7] )))
#
#    #############################################################################
#    ## PREVENTS SECTION
#    #############################################################################
#
#    if prevents:
#
#      formerAgentStatus = 'HCprevent'
#
#      p = prevents[0]
#
#      prevention  = ( 1 + 2 * p[8] ) * self.testDuration
#      endPrevent  = p[9] + timedelta( hours = prevention )
#
#      return S_OK( ( False, '  Preventive submission ( test %s ) until %s counter %s.' % ( p[0], endPrevent, p[8] )))
#
#    return S_OK( (False,'+ No worries') )
#
##############################################################################
#
#  def submitTests( self ):
#    '''
#      Try to submit a test for every Probing site with token RS_SVC.
#    '''
#
#    sites = self._getResources('Sites')
#
#    for siteTuple in sites:
#
#      if siteTuple[1] == 'Probing':
#        if siteTuple[4] == self.UNLOCK_TOKEN:
#
#          site = siteTuple[0]
#
#          gLogger.info( '  + %s ' % site )# is Probing and has token RS_SVC' % site )
#
#          if self._getTestList( siteName = site, agentStatus = 'HCongoing' ):
#            gLogger.info( '     already has an ongoing test' )
##            siteFlag = True
#            #continue
#
##          elif self.rsaDB.getTestList( siteName = site, agentStatus = 'HCscheduled' ):
#            #print '  + %s already has an scheduled test'%site
##            siteFlag = True
#            #continue
#
#          elif self._getTestList( siteName = site, agentStatus = 'HCskipped' ):
#            gLogger.info( '     is waiting for a Human action' )
#            #continue
#
#          else:
#            gLogger.info( '     is waiting for a test' )
#
##    if not siteFlag:
##      gLogger.info( '  + No Probing & unlocked sites.' )
#
#    return S_OK()