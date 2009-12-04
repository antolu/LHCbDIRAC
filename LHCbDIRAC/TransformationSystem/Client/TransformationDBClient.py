""" Class that contains client access to the transformation DB handler. """
########################################################################
# $Id: TransformationDBClient.py 18427 2009-11-20 10:28:53Z acsmith $
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/LHCbDIRAC/TransformationSystem/Client/TransformationDBClient.py $
########################################################################
__RCSID__ = "$Id: TransformationDBClient.py 19179 2009-12-04 09:54:19Z acsmith $"

from DIRAC                                                      import gLogger, gConfig, S_OK, S_ERROR
import DIRAC.TransformationSystem.Client.TransformationDBClient as     DIRACTransformationDBClient  
import types
    
class TransformationDBClient(DIRACTransformationDBClient.TransformationDBClient):

  """ Exposes the functionality available in the LHCbDIRAC/TransformationHandler

      This inherits the DIRAC base Client for direct execution of server functionality.
      The following methods are available (although not visible here).

      BK query manipulation

          addBookkeepingQuery(queryDict)
          getBookkeepingQueryForTransformation(transName)
          getBookkeepingQuery(queryID)
          deleteBookkeepingQuery(queryID)
          setTransformationQuery(transName, queryID)
          createTransformationQuery(transName,queryDict)      
  """
  def __init__(self):
    TransformationDBClient.__init__()