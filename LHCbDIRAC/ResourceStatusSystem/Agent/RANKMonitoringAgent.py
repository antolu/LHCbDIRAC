########################################################################
# $HeadURL$
########################################################################

__RCSID__ = "$Id$"

""" Queries BDII to pick out information about RANK.
"""

from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule                              import AgentModule
from DIRAC.Core.Utilities.ldapsearchBDII import ldapCEState
from DIRAC  import gMonitor

import sys, os

AGENT_NAME = "ResourceStatusSystem/RankMonitoringAgent"

class RankMonitoringAgent(AgentModule):

  def initialize( self ):

    self.pollingTime = self.am_getOption('PollingTime',60*10) # Every 10 minuts
    gLogger.info("PollingTime %d minutes" %(int(self.pollingTime)/60))

    self.useProxies = self.am_getOption('UseProxies','True').lower() in ( "y", "yes", "true" )
    self.proxyLocation = self.am_getOption('ProxyLocation', '' )
    if not self.proxyLocation:
      self.proxyLocation = False

    cesqueues = self._getCEsQueues()
    if not cesqueues:
      return cesqueues
    self.cesqueues = cesqueues['Value']
    
    cesqueues = self.cesqueues
    for ce in cesqueues:
      for queue in cesqueues[ce]:
        queuename = "%s:2119/%s"%(ce,queue)
        rankname = "RANK-%s"%ce
#        rankname = "RANK"
        gLogger.info( queuename, rankname )
        gMonitor.registerActivity(queuename,queuename,rankname,"",gMonitor.OP_MEAN,60*10)
      
    return S_OK()

  def execute( self ):

    cestates = ldapCEState("")
    if not cestates['OK']:
      gLogger.warn("cestate",cestates['Message'])
      return S_ERROR(cestates['Message'])
    cestates = cestates['Value']
    cestatedict = {}
    for cestate in cestates:
      try:
        cestatedict[cestate['GlueCEUniqueID']] = cestate
      except:
        continue
    
    
    cesqueues = self.cesqueues
    for ce in cesqueues:
      for queue in self.cesqueues[ce]:
        queuename = "%s:2119/%s"%(ce,queue)
#        cestate = ldapCEState(queuename)
#        if not cestate['OK']:
#          gLogger.warn("cestate",cestate['Message'])
#          continue
#        cestate = cestate['Value']
#        if len(cestate) != 1:
#          gLogger.warn("cestate",cestate)
#          continue
#        cestate = cestate[0]
        
        try:
          cestate = cestatedict[queuename]
        except:
          continue

        gLogger.debug(queuename,cestate)
        try:
          waitingJobs = int(cestate['GlueCEStateWaitingJobs'])
          runningJobs = int(cestate['GlueCEStateRunningJobs'])
          gLogger.debug('(waitingJobs,runningJobs)',queuename+str((waitingJobs,runningJobs)))
          if waitingJobs == 0:
            totalCPUs = int(cestate['GlueCEInfoTotalCPUs'])
            freeCPUs = int(cestate['GlueCEStateFreeCPUs'])
            gLogger.debug('(totalCPUs,freeCPUs)',queuename+str((totalCPUs,freeCPUs)))
            rank = freeCPUs * 10 / totalCPUs 
          else:  
            rank = -waitingJobs * 10 / ( runningJobs + 1 ) - 1
        except Exception,x:
          gLogger.warn(queuename,x)
          continue
        if rank>100:
          rank = 100
        if rank<-100:
          rank = -100
          
        gLogger.info(queuename,rank)
        gMonitor.addMark(queuename,rank)
           
    return S_OK()

 
  def _getCEsQueues(self):
    sites = gConfig.getSections('/Resources/Sites/LCG')
    if sites['OK']:
      sites = sites['Value']
    else:
      return sites
       
    cesqueues = {}
    for site in sites:
#      if site[-2:]!='uk':
#        continue        
      gLogger.debug("Site",site)
      ces = gConfig.getSections('/Resources/Sites/LCG/%s/CEs'%site)
      if not ces['OK']:
        continue
      ces = ces['Value']
      for ce in ces:
        queues = gConfig.getSections('/Resources/Sites/LCG/%s/CEs/%s/Queues'%(site,ce))
        if queues['OK']:
          cesqueues[ce] = queues['Value']

    return S_OK(cesqueues)
