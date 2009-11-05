########################################################################
# $HeadURL$
########################################################################

""" Production requests agent perform all periodic task with
    requests. Nothing for now...
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Agent          import Agent
from DIRAC.Core.DISET.RPCClient     import RPCClient
from DIRAC.Core.Utilities.Shifter   import setupShifterProxyInEnv
from DIRAC                          import S_OK, S_ERROR, gConfig, gMonitor, gLogger

import os, time, string

def bkProductionProgress(id,setup):
# AZ: have to use setup since it's not automatically picked up... WHY ???
  RPC = RPCClient('Bookkeeping/BookkeepingManager',setup=setup)
  result = RPC.getProductionInformations(id)
  if not result['OK']:
    return result
  info = result['Value']
  if not 'Number of events' in info:
    return S_OK(0)
  allevents = info['Number of events']
  if len(allevents) == 0:
    return S_OK(0)
  if len(allevents) > 1:
    return S_ERROR('More than one output file type. Unsupported.')
  return S_OK(allevents[0][1])


AGENT_NAME = 'ProductionManagement/RequestTrackingAgent'

class RequestTrackingAgent(Agent):
  def __init__(self):
    """ Standard constructor for Agent"""
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    """Sets defaults"""
    result = Agent.initialize(self)
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',120)
    self.setup       = gConfig.getValue('/DIRAC/Setup','LHCb-Production')
    return result

  def getTrackedProductions(self):
    RPC   = RPCClient( "ProductionManagement/ProductionRequest", setup=self.setup )
    return RPC.getTrackedProductions();

  def updateTrackedProductions(self,update):
    RPC   = RPCClient( "ProductionManagement/ProductionRequest", setup=self.setup )
    return RPC.updateTrackedProductions(update);

  def execute(self):
    """The RequestTrackingAgent execution method.
    """
    gLogger.info('Request Tracking execute is started')
    result = self.getTrackedProductions()
    update = []
    if result['OK']:
      for productionID in result['Value']:
        result = bkProductionProgress(long(productionID),self.setup)
        if result['OK']:
          update.append({'ProductionID':productionID,'BkEvents':result['Value']})
        else:
          gLogger.error('Progress of %s is not updated: %s' %
                        (productionID,result['Message']))
    else:
      gLogger.error('Request service: %s' % result['Message'])

    if update:
      result = self.updateTrackedProductions(update)
      if not result['OK']:
        gLogger.error(result['Message'])

    gLogger.info('Request Tracking execute is ended')
    return S_OK('Request Tracking information updated')
