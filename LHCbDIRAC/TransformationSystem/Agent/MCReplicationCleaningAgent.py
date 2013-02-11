""" :mod: MCReplicationCleaningAgent
    ================================

  .. module: MCReplicationCleaningAgent
  :synopsis: Clean up of completed all MC Replication transformations which
  have enough replicas for Done requests.

"""

__RCSID__ = "$Id $"

from DIRAC                                                        import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                                  import AgentModule
from DIRAC.Core.DISET.RPCClient                                   import RPCClient
from DIRAC.DataManagementSystem.Client.ReplicaManager             import ReplicaManager
from LHCbDIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient

AGENT_NAME = 'Transformation/MCReplicationCleaningAgent'

class MCReplicationCleaningAgent( AgentModule ):
  """ .. class:: MCReplicationCleaningAgent

  :param TransformationClient transClient: TransformationClient instance
  :param ReplicaManager replicaManager: ReplicaManager instance
  :param RPCClient requestClient: ProductionRequest rpc client
  :param int reqARCHIVE: nb of requested archive replicas
  :param int reqDST: nb of requested dst replicas
  """

  #############################################################################

  def __init__( self, *args, **kwargs ):
    """ c'tor

    :param self: self reference
    :param str agentName: name of agent
    :param bool baseAgentName: whatever
    :param dict properties: whatever else
    """
    AgentModule.__init__( self, *args, **kwargs )

    self.transClient = TransformationClient()
    self.replicaManager = ReplicaManager()
    self.requestClient = RPCClient( 'ProductionManagement/ProductionRequest', timeout = 120 )

    self.reqARCHIVE = 1
    self.reqDST = 3

  #############################################################################

  def initialize( self ):
    """Sets defaults """

    # This sets the Default Proxy
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    self.reqARCHIVE = self.am_getOption( "requestedARCHIVE", self.reqARCHIVE )
    self.reqDST = self.am_getOption( "requestedDST", self.reqDST )

    return S_OK()

  #############################################################################
  def execute( self ):
    """ The MCReplicationCleaningAgent execution method.
    """

    res = self.requestClient.getProductionRequestSummary( 'Done', 'Simulation' )
    if not res['OK']:
      self.log.error( 'Can not get requests', res['Message'] )
      return S_OK()
    reqs = res['Value']

    masreqs = []
    for req, reqDict in reqs.iteritems():
      master = reqDict['master']
      if master:
        if not master in masreqs:
          masreqs.append( master )
      else:
        masreqs.append( req )

    res = self.transClient.getTransformations( {'Status':'Active', 'Type':'Replication'} )
    if not res['OK']:
      self.log.error( 'Can not get transformations', res['Message'] )
      return S_OK()
    trans = res["Value"]

    transfdone = []
    for tran in trans:
      tid = int( tran['TransformationID'] )
      tfamily = int( tran['TransformationFamily'] )
      if tfamily in masreqs:
        transfdone.append( tid )

    # failedreplicas = 0

    for tran in transfdone:
      res = self.checkReplicas( tran )
      if not res['OK']:
        self.log.error( res["Message"] )
      else:
        res = self.transClient.setTransformationParameter( tran, 'Status', 'Completed' )
        if res['OK']:
          self.log.info( "Completed transformation %d" % tran )
        else:
          self.log.error( "Completing transformation %d" % tran, res['Message'] )

    return S_OK()

  def checkReplicas( self, transformationID ):
    """ check replicas """

    res = self.transClient.getTransformationFiles( {'TransformationID':transformationID} )
    if not res['OK']:
      return S_ERROR( "getTransformationFiles for %d: %s" % ( transformationID, res["Message"] ) )

    lfns = [lfn['LFN'] for lfn in res['Value']]

    res = self.replicaManager.getReplicas( lfns )
    if not res['OK']:
      return S_ERROR( "getReplicas for %d: %s" % ( transformationID, res['Message'] ) )

    failedReplicas = res['Value']['Failed']
    if failedReplicas:
      return S_ERROR( "failedReplicas for %d: %s " % ( transformationID, failedReplicas.keys() ) )

    replicas = res['Value']['Successful']

    badlfnsFailover = []
    badlfnsArchive = []
    badlfnsDST = []

    for lfn in replicas.iterkeys():
      ses = replicas[lfn].keys()
      sedict = { "FAILOVER":[], "ARCHIVE":[], "DST":[] }
      for se in ses:
        if se.endswith( "-FAILOVER" ):
          sedict["FAILOVER"].append( se )
        if se.endswith( "-ARCHIVE" ):
          sedict["ARCHIVE"].append( se )
        if se.endswith( "-DST" ):
          sedict["DST"].append( se )

      if sedict["FAILOVER"]:
        badlfnsFailover.append( lfn )

      if len( sedict["ARCHIVE"] ) < self.reqARCHIVE :
        badlfnsArchive.append( lfn )

      if len( sedict["DST"] ) < self.reqDST :
        badlfnsDST.append( lfn )
    message = ""
    if badlfnsFailover:
      message += "\nFAILOVER replica for LFN(s):\n%s" % ( "\n".join( badlfnsFailover ) )
    if badlfnsArchive:
      message += "\ninsufficient ARCHIVE replicas for LFN(s):\n%s" % ( "\n".join( badlfnsArchive ) )
    if badlfnsDST:
      message += "\ninsufficient DST replicas for LFN(s):\n%s" % ( "\n".join( badlfnsDST ) )

    if  badlfnsFailover or badlfnsArchive or badlfnsDST:
      return S_ERROR( "checkReplicas:  Transformation %d has problem%s" % ( transformationID, message ) )

    return S_OK()
