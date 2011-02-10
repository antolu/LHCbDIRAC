""" Class that contains client access to the transformation DB handler. """
########################################################################
# $Id$
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/LHCbDIRAC/TransformationSystem/Client/TransformationClient.py $
########################################################################
__RCSID__ = "$Id$"

from DIRAC                                                    import S_OK, gLogger
from DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient as DIRACTransformationClient  
    
class TransformationClient(DIRACTransformationClient):

  """ Exposes the functionality available in the LHCbDIRAC/TransformationHandler

      This inherits the DIRAC base Client for direct execution of server functionality.
      The following methods are available (although not visible here).

      BK query manipulation
          deleteBookkeepingQuery(queryID)
          deleteTransformationBookkeepingQuery(transName)
          createTransformationQuery(transName,queryDict)      
          getBookkeepingQueryForTransformation(transName)
  """
    
  def addTransformation(self,transName,description,longDescription,type,plugin,agentType,fileMask,
                                    transformationGroup = 'General',
                                    groupSize           = 1,
                                    inheritedFrom       = 0,
                                    body                = '',
                                    maxTasks            = 0,
                                    eventsPerTask       = 0,
                                    addFiles            = True,
                                    bkQuery             = {},
                                    rpc                 = False,
                                    url                 = '',
                                    timeout             = 120):
    rpcClient = self._getRPC(rpc=rpc,url=url,timeout=timeout)
    res = rpcClient.addTransformation(transName,description,longDescription,type,plugin,agentType,fileMask,transformationGroup,groupSize,inheritedFrom,body,maxTasks,eventsPerTask,addFiles)
    if not res['OK']:
      return res
    transID = res['Value']
    if bkQuery:
      res = rpcClient.createTransformationQuery(transID,bkQuery)
      if not res['OK']:
        gLogger.error("Failed to publish BKQuery for transformation","%s %s" % (transID,res['Message']))
    return S_OK(transID)
