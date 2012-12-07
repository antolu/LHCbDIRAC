''' Production requests agent perform all periodic task with
    requests.
    Currently it update the number of Input Events for processing
    productions and the number of Output Events for all productions.
'''

from DIRAC                                                import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.AgentModule                          import AgentModule
from DIRAC.Core.DISET.RPCClient                           import RPCClient
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient


__RCSID__ = "$Id$"

AGENT_NAME = 'ProductionManagement/RequestTrackingAgent'

class RequestTrackingAgent( AgentModule ):

  def __init__( self, agentName, loadName, baseAgentName = False, properties = dict() ):
    ''' c'tor

    :param self: self reference
    :param str agentName: name of agent
    :param bool baseAgentName: whatever
    :param dict properties: whatever else
    '''
    AgentModule.__init__( self, agentName, loadName, baseAgentName, properties )

    self.bkClient = BookkeepingClient()
    self.prodReq = RPCClient( "ProductionManagement/ProductionRequest" )

  def initialize( self ):
    '''Sets defaults
    '''
    self.pollingTime = self.am_getOption( 'PollingTime', 1200 )
    return S_OK()

  def execute( self ):
    '''The RequestTrackingAgent execution method.
    '''
    gLogger.info( 'Request Tracking execute is started' )

    result = self.prodReq.getTrackedInput()
    update = []
    if result['OK']:
      for request in result['Value']:
        result = self.bkInputNumberOfEvents( request )
        if result['OK']:
          update.append( {'RequestID':request['RequestID'],
                         'RealNumberOfEvents':result['Value']} )
        else:
          gLogger.error( 'Input of %s is not updated: %s' %
                        ( str( request['RequestID'] ), result['Message'] ) )
    else:
      gLogger.error( 'Request service: %s' % result['Message'] )
    if update:
      result = self.prodReq.updateTrackedInput( update )
      if not result['OK']:
        gLogger.error( result['Message'] )

    result = self.self.prodReq.getTrackedProductions()
    update = []
    if result['OK']:
      for productionID in result['Value']:
        result = self.bkClient.getProductionProcessedEvents( productionID )
        if result['OK']:
          if result['Value']:
            update.append( {'ProductionID':productionID, 'BkEvents':result['Value']} )
        else:
          gLogger.error( 'Progress of %s is not updated: %s' %
                        ( productionID, result['Message'] ) )
    else:
      gLogger.error( 'Request service: %s' % result['Message'] )

    if update:
      result = self.prodReq.updateTrackedProductions( update )
      if not result['OK']:
        gLogger.error( result['Message'] )

    gLogger.info( 'Request Tracking execute is ended' )
    return S_OK( 'Request Tracking information updated' )

  def bkInputNumberOfEvents( self, request ):
    ''' Extremely dirty way...
    '''

    try:
      v = {
        'ProcessingPass' : str( request['inProPass'] ),
        'FileType' : str( request['inFileType'] ),
        'EventType' : str( request['EventType'] ),
        'ConfigName' : str( request['configName'] ),
        'ConfigVersion' : str( request['configVersion'] ),
        'DataQualityFlag' : str( request['inDataQualityFlag'] )
        }
      if request['condType'] == 'Run':
        v['DataTakingConditions'] = str( request['SimCondition'] )
      else:
        v['SimulationConditions'] = str( request['SimCondition'] )
      if str( request['inProductionID'] ) != '0':
        v['Production'] = [int( x ) for x in str( request['inProductionID'] ).split( ',' )]
      if 'inTCKs' in request and str( request['inTCKs'] ) != '':
        v['TCK'] = [str( x ) for x in str( request['inTCKs'] ).split( ',' )]
    except Exception, e:
      return S_ERROR( "Can not parse the request: %s" % str( e ) )
    v['NbOfEvents'] = True
    result = self.bkClient.getFiles( v )
    if not result['OK']:
      return result
    if not result['Value'][0]:
      return S_OK( 0 )
    try:
      sum_nr = long( result['Value'][0] )
    except Exception, e:
      return S_ERROR( "Can not convert result from BK call: %s" % str( e ) )
    return S_OK( sum_nr )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
