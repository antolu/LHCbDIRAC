########################################################################
# $Header: /local/reps/dirac/DIRAC3/DIRAC/ProductionManagementSystem/Agent/RequestTrackingAgent.py,v 1.2 2009/02/11 13:21:59 atsareg Exp $
########################################################################

""" Production requests agent perform all periodic task with
    requests.
    Currently it update the number of Input Events for processing
    productions and the number of Output Events for all productions.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.AgentModule    import AgentModule
from DIRAC.Core.DISET.RPCClient     import RPCClient
from DIRAC                          import S_OK, S_ERROR, gConfig, gMonitor, gLogger

import os, time, string

def bkProductionProgress(id,setup):
  RPC = RPCClient('Bookkeeping/BookkeepingManager',setup=setup)
  return RPC.getProcessedEvents(int(id))

def bkInputNumberOfEvents(r,setup):
  """ Extrim dirty way... But I DO NOT KNOW OTHER !!! """
  try:
    v = {
      'ProcessingPass' : str(r['inProPass']),
      'FileType' : str(r['inFileType']),
      'EventType' : str(r['EventType']),
      'ConfigName' : str(r['configName']),
      'ConfigVersion' : str(r['configVersion']),
      'DataQualityFlag' : str(r['inDataQualityFlag'])
      }
    if r['condType'] == 'Run':
      v['DataTakingConditions'] = str(r['SimCondition'])
    else:
      v['SimulationConditions'] = str(r['SimCondition'])
    if str(r['inProductionID']) != '0':
      v['ProductionID'] = [int(x) for x in str(r['inProductionID']).split(',')]
  except Exception,e:
    return S_ERROR("Can not parse the request: %s" % str(e))
  RPC = RPCClient('Bookkeeping/BookkeepingManager',setup=setup)
  v['NbOfEvents'] = True
  result = RPC.getFilesWithGivenDataSets(v)
  if not result['OK']:
    return result
  if not result['Value'][0]:
    return S_OK(0)
  try:
    sum = long(result['Value'][0])
  except Exception,e:
    return S_ERROR("Can not convert result from BK call: %s" % str(e))
  return S_OK(sum)

AGENT_NAME = 'ProductionManagement/RequestTrackingAgent'

class RequestTrackingAgent(AgentModule):

  def initialize(self):
    """Sets defaults"""
    self.pollingTime = self.am_getOption('PollingTime',1200)
    self.setup       = self.am_getOption('Setup','')
    return S_OK()

  def getTrackedProductions(self):
    RPC   = RPCClient( "ProductionManagement/ProductionRequest", setup=self.setup )
    return RPC.getTrackedProductions();

  def updateTrackedProductions(self,update):
    RPC   = RPCClient( "ProductionManagement/ProductionRequest", setup=self.setup )
    return RPC.updateTrackedProductions(update);

  def getTrackedInput(self):
    RPC   = RPCClient( "ProductionManagement/ProductionRequest", setup=self.setup )
    return RPC.getTrackedInput();

  def updateTrackedInput(self,update):
    RPC   = RPCClient( "ProductionManagement/ProductionRequest", setup=self.setup )
    return RPC.updateTrackedInput(update);

  def execute(self):
    """The RequestTrackingAgent execution method.
    """
    gLogger.info('Request Tracking execute is started')

    result = self.getTrackedInput()
    update = []
    if result['OK']:
      for request in result['Value']:
        result = bkInputNumberOfEvents(request,self.setup)
        if result['OK']:
          update.append({'RequestID':request['RequestID'],
                         'RealNumberOfEvents':result['Value']})
        else:
          gLogger.error('Input of %s is not updated: %s' %
                        (str(request['RequestID']),result['Message']))
    else:
      gLogger.error('Request service: %s' % result['Message'])
    if update:
      result = self.updateTrackedInput(update)
      if not result['OK']:
        gLogger.error(result['Message'])

    result = self.getTrackedProductions()
    update = []
    if result['OK']:
      for productionID in result['Value']:
        result = bkProductionProgress(int(productionID),self.setup)
        if result['OK']:
          if result['Value']:
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
