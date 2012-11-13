""" Class that contains client access to the transformation DB handler. """

__RCSID__ = "$Id$"

from DIRAC                                                    import S_OK, gLogger
from DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient as DIRACTransformationClient

class TransformationClient( DIRACTransformationClient ):

  """ Exposes the functionality available in the LHCbDIRAC/TransformationHandler

      This inherits the DIRAC base Client for direct execution of server functionality.
      The following methods are available (although not visible here).

      BK query manipulation
          deleteBookkeepingQuery(queryID)
          deleteTransformationBookkeepingQuery(transName)
          createTransformationQuery(transName,queryDict)      
          getBookkeepingQueryForTransformation(transName)
  """

  def addTransformation( self, transName, description, longDescription, transfType, plugin, agentType, fileMask,
                         transformationGroup = 'General',
                         groupSize = 1,
                         inheritedFrom = 0,
                         body = '',
                         maxTasks = 0,
                         eventsPerTask = 0,
                         addFiles = True,
                         bkQuery = {},
                         rpc = None,
                         url = '',
                         timeout = 120 ):
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    res = rpcClient.addTransformation( transName, description, longDescription, transfType, plugin, agentType,
                                       fileMask, transformationGroup, groupSize, inheritedFrom, body,
                                       maxTasks, eventsPerTask, addFiles )
    if not res['OK']:
      return res
    transID = res['Value']
    if bkQuery:
      res = rpcClient.createTransformationQuery( transID, bkQuery )
      if not res['OK']:
        gLogger.error( "Failed to publish BKQuery for transformation", "%s %s" % ( transID, res['Message'] ) )
    return S_OK( transID )

  def moveFilesToDerivedTransformation( self, transDict, resetUnused=True ):
    prod = transDict['TransformationID']
    parentProd = int( transDict.get( 'InheritedFrom', 0 ) )
    movedFiles = {}
    if not parentProd:
      gLogger.warn( "Transformation %d was not derived..." % prod )
      return S_OK( ( parentProd, movedFiles ) )
    statusToMove = [ 'Unused', 'MaxReset' ]
    selectDict = {'TransformationID': parentProd, 'Status': statusToMove}
    res = self.getTransformationFiles( selectDict )
    if not res['OK']:
      gLogger.error( "Error getting Unused files from transformation %s:" % parentProd, res['Message'] )
      return res
    parentFiles = res['Value']
    lfns = [lfnDict['LFN'] for lfnDict in parentFiles]
    if not lfns:
      gLogger.info( "No files found to be moved from transformation %d to %d" % ( parentProd, prod ) )
      return res
    selectDict = { 'TransformationID': prod, 'LFN': lfns}
    res = self.getTransformationFiles( selectDict )
    if not res['OK']:
      gLogger.error( "Error getting files from derived transformation %s" % prod, res['Message'] )
      return res
    derivedFiles = res['Value']
    suffix = '-%d' % parentProd
    errorFiles = {}
    for parentDict in parentFiles:
      lfn = parentDict['LFN']
      status = parentDict['Status']
      force = False
      if resetUnused and status == 'MaxReset':
        status = 'Unused'
        force = True
      derivedStatus = None
      for derivedDict in derivedFiles:
        if derivedDict['LFN'] == lfn:
          derivedStatus = derivedDict['Status']
          break
      if derivedStatus:
        if derivedStatus.endswith( suffix ):
          res = self.setFileStatusForTransformation( parentProd, 'MovedTo-%d' % prod, [lfn] )
          if not res['OK']:
            gLogger.error( "Error setting status for %s in transformation %d to %s" % ( lfn, parentProd, 'MovedTo-%d' % prod ), res['Message'] )
            continue
          res = self.setFileStatusForTransformation( prod, status, [lfn], force=force )
          if not res['OK']:
            gLogger.error( "Error setting status for %s in transformation %d to %s" % ( lfn, prod, status ), res['Message'] )
            self.setFileStatusForTransformation( parentProd, status , [lfn] )
            continue
          if force:
            status = 'Unused from MaxReset'
          movedFiles[status] = movedFiles.setdefault( status, 0 ) + 1
        else:
          errorFiles[derivedStatus] = errorFiles.setdefault( derivedStatus, 0 ) + 1
    if errorFiles:
      gLogger.error( "Some files didn't have the expected status in derived transformation %d" % prod )
      for err, val in errorFiles.items():
        gLogger.error( "\t%d files were in status %s" % ( val, err ) )

    return S_OK( ( parentProd, movedFiles ) )
