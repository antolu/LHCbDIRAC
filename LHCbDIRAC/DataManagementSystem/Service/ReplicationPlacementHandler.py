""" ReplicationPlacementHandler is the implementation of the data Transformation service """
# $Id: ReplicationPlacementHandler.py 19253 2009-12-07 10:52:48Z acsmith $
__RCSID__ = "$Revision: 1.56 $"

from DIRAC                                                              import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                                    import RequestHandler
from LHCbDIRAC.DataManagementSystem.DB.ReplicationPlacementDB           import ReplicationPlacementDB
from LHCbDIRAC.TransformationSystem.Service.TransformationHandler       import TransformationHandler
from types import *

# This is a global instance of the TransformationDB class
replicationDB = False

def initializeTransformationManagerHandler( serviceInfo ):
  global replicationDB
  replicationDB = ReplicationPlacementDB()
  return S_OK()

class TransformationManagerHandler(TransformationHandler):

  def __init__(self,*args,**kargs):
    self.setDatabase(replicationDB)
    TransformationHandler.__init__(self, *args,**kargs)