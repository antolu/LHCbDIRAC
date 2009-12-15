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
    for x in allevents:
      if x[0] == 'DST':
        return S_OK(x[1])
    return S_ERROR('More than one output file type and no DST. Unsupported.')
  return S_OK(allevents[0][1])

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
    if long(r['inProductionID']):
      v['ProductionID'] = str(r['inProductionID'])
  except Exception,e:
    return S_ERROR("Can not parse the request: %s" % str(e))
  RPC = RPCClient('Bookkeeping/BookkeepingManager',setup=setup)
  result = RPC.getFilesWithGivenDataSets(v)
  if not result['OK']:
    return result
  result = RPC.getFilesInformations(result['Value'])
  if not result['OK']:
    return result
  try:
    evstat=sum([long(x['EventStat']) for x in result['Value'].values()])
  except:
    return S_ERROR("Bad files information for %s" % r['RequestID'])
  return S_OK(evstat)

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
