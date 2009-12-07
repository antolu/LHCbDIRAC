""" Class that contains client access to the transformation DB handler. """
########################################################################
# $Id: TransformationDBClient.py 18427 2009-11-20 10:28:53Z acsmith $
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/LHCbDIRAC/TransformationSystem/Client/TransformationDBClient.py $
########################################################################
__RCSID__ = "$Id: TransformationDBClient.py 19179 2009-12-04 09:54:19Z acsmith $"

from DIRAC                                                      import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.TransformationSystem.Client.TransformationDBClient   import TransformationDBClient as DIRACTransformationDBClient  
import types
    
class TransformationDBClient(DIRACTransformationDBClient):

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
  def __init__(self, *args, **kargs):
    DIRACTransformationDBClient.__init__(self, *args, **kargs)
    
  def addTransformation(self,transName,description,longDescription,type,plugin,agentType,fileMask,
                                    transformationGroup = 'General',
                                    groupSize           = 1,
                                    inheritedFrom       = 0,
                                    body                = '',
                                    maxJobs             = 0,
                                    eventsPerJob        = 0,
                                    addFiles            = True,
                                    bkQuery             = {},
                                    rpc                 = False,
                                    url                 = '',
                                    timeout             = 120):
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    res = rpcClient.addTransformation(transName,description,longDescription,type,plugin,agentType,fileMask,transformationGroup,groupSize,inheritedFrom,body,maxJobs,eventsPerJob,addFiles)
    if not res['OK']:
      return res
    transID = res['Value']
    if bkQuery:
      res = rpcClient.createTransformationQuery(transID,bkQuery)
      if not res['OK']:
        gLogger.error("Failed to publish BKQuery for transformation","%s %s" % (transID,res['Message']))
    return S_OK(transID)
