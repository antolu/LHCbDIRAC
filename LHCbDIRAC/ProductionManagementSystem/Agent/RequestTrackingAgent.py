""" Production requests agent perform all periodic task with requests.
    Currently it updates the number of Input Events for processing
    productions and the number of Output Events for all productions.
"""

from DIRAC                                                import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.AgentModule                          import AgentModule
from DIRAC.Core.DISET.RPCClient                           import RPCClient
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient


__RCSID__ = "$Id$"

AGENT_NAME = 'ProductionManagement/RequestTrackingAgent'

class RequestTrackingAgent( AgentModule ):

  def __init__( self, *args, **kwargs ):
    """ c'tor
    """
    AgentModule.__init__( self, *args, **kwargs )

    self.bkClient = None
    self.prodReq = None

  def initialize( self ):
    """ Just initializing the clients
    """
    self.bkClient = BookkeepingClient()
    self.prodReq = RPCClient( "ProductionManagement/ProductionRequest" )

    return S_OK()

  def execute( self ):
    """The RequestTrackingAgent execution method.
    """
    result = self.prodReq.getTrackedInput()
    update = []
    if result['OK']:
      gLogger.verbose( "Requests tracked: %s" % ( ','.join( [str( req['RequestID'] ) for req in result['Value'] ] ) ) )
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

    result = self.prodReq.getTrackedProductions()
    update = []
    if result['OK']:
      gLogger.verbose( "Productions tracked: %s" % ( ','.join( [str( prod ) for prod in result['Value'] ] ) ) )
      for productionID in result['Value']:
        result = self.bkClient.getProductionProcessedEvents( productionID )
        if result['OK']:
          if result['Value']:
            gLogger.verbose( "Updating production %d, with BkEvents %d" % ( int( productionID ),
                                                                            int( result['Value'] ) ) )
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

    return S_OK( 'Request Tracking information updated' )

  def bkInputNumberOfEvents( self, request ):
    """ Extremely dirty way...
    """
    condition = {'ProcessingPass'  : str( request['inProPass'] ).replace( ' ', '' ),
                 'FileType'        : str( request['inFileType'] ).replace( ' ', '' ).split( ',' ),
                 'EventType'       : str( request['EventType'] ).replace( ' ', '' ),
                 'ConfigName'      : str( request['configName'] ).replace( ' ', '' ),
                 'ConfigVersion'   : str( request['configVersion'] ).replace( ' ', '' ),
                 'DataQualityFlag' : str( request['inDataQualityFlag'] ).replace( ' ', '' )}
    if request['condType'] == 'Run':
      condition['DataTakingConditions'] = str( request['SimCondition'] )
    else:
      condition['SimulationConditions'] = str( request['SimCondition'] )
    if str( request['inProductionID'] ) != '0':
      condition['Production'] = [int( x ) for x in str( request['inProductionID'] ).split( ',' )]
    if 'inTCKs' in request and str( request['inTCKs'] ) != '':
      condition['TCK'] = [str( x ) for x in str( request['inTCKs'] ).split( ',' )]
    condition['NbOfEvents'] = True
    
    gLogger.debug( "Requesting: ", str( condition ) )
    result = self.bkClient.getFiles( condition )
    if not result['OK']:
      return result
    if not result['Value'][0]:
      return S_OK( 0 )
    try:
      sum_nr = long( result['Value'][0] )
    except Exception, e:
      return S_ERROR( "Can not convert result from BK call: %s" % str( e ) )
    return S_OK( sum_nr )

#...............................................................................
#EOF
