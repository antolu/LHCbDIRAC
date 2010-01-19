# $Id: ReplicationPlacementDB.py 19467 2009-12-14 15:01:49Z acsmith $

""" DIRAC ReplicationPlacementDB class is a front-end to the LHCb data specific database """

__RCSID__ = "$Revision: 1.65 $"

from DIRAC                                                  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.DB                                     import DB
from LHCbDIRAC.TransformationSystem.DB.TransformationDB     import TransformationDB

class ReplicationPlacementDB(TransformationDB):

  def __init__( self, maxQueueSize=10 ):
    """ Constructor
    """
    TransformationDB.__init__(self,'ReplicationPlacementDB', 'DataManagement/ReplicationPlacement', maxQueueSize)