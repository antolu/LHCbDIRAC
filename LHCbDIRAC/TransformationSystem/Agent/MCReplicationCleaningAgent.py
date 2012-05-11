"""
MCReplicationCleaningAgent Completed all MC Replication transformations which have enough replicas for Done requests. 
"""

from DIRAC                                                          import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.AgentModule                                    import AgentModule
from LHCbDIRAC.TransformationSystem.Client.TransformationClient     import TransformationClient
from DIRAC.DataManagementSystem.Client.ReplicaManager               import ReplicaManager
from DIRAC.Core.DISET.RPCClient                                     import RPCClient

AGENT_NAME = 'Transformation/MCReplicationCleaningAgent'

class MCReplicationCleaningAgent( AgentModule ):

  #############################################################################
  def initialize( self ):
    """Sets defaults """

    self.transClient = TransformationClient()
    self.replicaManager = ReplicaManager()
    #self.requestClient = RequestClient()
    self.requestClient = RPCClient( 'ProductionManagement/ProductionRequest', timeout = 120 )

    # This sets the Default Proxy
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    self.reqARCHIVE = self.am_getOption( "requestedARCHIVE", 1 )
    self.reqDST = self.am_getOption( "requestedDST"    , 3 )

    return S_OK()

  #############################################################################
  def execute( self ):
    """ The MCReplicationCleaningAgent execution method.
    """

    res = self.requestClient.getProductionRequestSummary( 'Done', 'Simulation' )
    if not res['OK']:
      gLogger.error( 'Can not get requests', res['Message'] )
      return S_OK()
    reqs = res['Value']

    masreqs = []
    for r, d in reqs.iteritems():
      master = d['master']
      if master:
        if not master in masreqs:
          masreqs.append( master )
      else:
        masreqs.append( r )

    res = self.transClient.getTransformations( {'Status':'Active', 'Type':'Replication'} )
    if not res['OK']:
      gLogger.error( 'Can not get transformations', res['Message'] )
      return S_OK()
    trans = res["Value"]

    transfdone = []
    for t in trans:
      tid = int( t['TransformationID'] )
      tfamily = int( t['TransformationFamily'] )
      if tfamily in masreqs:
        transfdone.append( tid )

    failedreplicas = 0

    for t in transfdone:
      res = self.checkReplicas( t )
      if not res['OK']:
        gLogger.error( res["Message"] )
      else:
        res = self.transClient.setTransformationParameter( t, 'Status', 'Completed' )
        if res['OK']:
          gLogger.info( "Completed transformation %d" % t )
        else:
          gLogger.error( "Completing transformation %d" % t, res['Message'] )

    return S_OK()


  def checkReplicas( self, transformationID ):

    res = self.transClient.getTransformationFiles( {'TransformationID':transformationID} )
    if not res['OK']:
      return S_ERROR( "getTransformationFiles for %d: %s" % ( transformationID, res["Message"] ) )

    lfns = res["LFNs"]

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
      sedict = {"FAILOVER":[], "ARCHIVE":[], "DST":[]}
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
