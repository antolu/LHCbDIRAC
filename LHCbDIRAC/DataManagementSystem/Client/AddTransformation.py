"""
Actual engine for adding a DM transformation, called by dirac-dms-add-Transformation
"""
__RCSID__ = "$Id$"

import os

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.List import breakListIntoChunks

from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import getProcessingPasses, BKQuery
from LHCbDIRAC.TransformationSystem.Utilities.PluginUtilities import getRemovalPlugins, getReplicationPlugins
from LHCbDIRAC.DataManagementSystem.Utilities.FCUtilities import chown
from LHCbDIRAC.DataManagementSystem.Client.DMScript import ProgressBar


def executeAddTransformation(pluginScript):
  """
  Method for actually adding a DM transformation
  It takes its options and argument values from pluginScript
  """
  test = False
  start = False
  force = False
  invisible = False
  fcCheck = True
  unique = False
  bkQuery = None
  depth = None
  userGroup = None
  listProcessingPasses = False
  nameOption = None

  switches = Script.getUnprocessedSwitches()
  for opt, val in switches:
    if opt in ('s', 'Start'):
      start = True
    elif opt == 'Test':
      test = True
    elif opt == "Force":
      force = True
    elif opt == "SetInvisible":
      invisible = True
    elif opt == "NoFCCheck":
      fcCheck = False
    elif opt == "Unique":
      unique = True
    elif opt == 'Chown':
      userGroup = val.split('/')
      if len(userGroup) != 2 or not userGroup[1].startswith('lhcb_'):
        gLogger.fatal("Wrong user/group")
        DIRAC.exit(2)
    elif opt == "Depth":
      try:
        depth = int(val)
      except ValueError:
        gLogger.fatal("Illegal integer depth:", val)
        DIRAC.exit(2)
    elif opt == 'ListProcessingPasses':
      listProcessingPasses = True
    elif opt == 'Name':
      nameOption = val

  if userGroup:
    from DIRAC.Core.Security.ProxyInfo import getProxyInfo
    res = getProxyInfo()
    if not res['OK']:
      gLogger.fatal("Can't get proxy info", res['Message'])
      exit(1)
    properties = res['Value'].get('groupProperties', [])
    if not 'FileCatalogManagement' in properties:
      gLogger.error("You need to use a proxy from a group with FileCatalogManagement")
      exit(5)

  plugin = pluginScript.getOption('Plugin')
  if not plugin and not listProcessingPasses:
    gLogger.fatal("ERROR: No plugin supplied...")
    Script.showHelp()
    DIRAC.exit(0)
  prods = pluginScript.getOption('Productions')
  requestID = pluginScript.getOption('RequestID')
  fileType = pluginScript.getOption('FileType')
  pluginParams = pluginScript.getPluginParameters()
  pluginSEParams = pluginScript.getPluginSEParameters()
  requestedLFNs = pluginScript.getOption('LFNs')

  transType = None
  if plugin in getRemovalPlugins():
    transType = "Removal"
  elif plugin in getReplicationPlugins():
    transType = "Replication"
  elif not listProcessingPasses:
    gLogger.notice("This script can only create Removal or Replication plugins")
    gLogger.notice("Replication :", str(getReplicationPlugins()))
    gLogger.notice("Removal     :", str(getRemovalPlugins()))
    gLogger.notice("If needed, ask for adding %s to the known list of plugins" % plugin)
    DIRAC.exit(2)

  bk = BookkeepingClient()
  tr = TransformationClient()

  if plugin in ("DestroyDataset", 'DestroyDatasetWhenProcessed') or prods:
    # visible = 'All'
    fcCheck = False

  transBKQuery = {}
  # If no BK queries are given, set to None to go once in the loop
  bkQueries = [None]
  if not requestedLFNs:
    bkQuery = pluginScript.getBKQuery()
    if not bkQuery and not force:
      gLogger.fatal("No LFNs and no BK query were given...")
      Script.showHelp()
      DIRAC.exit(2)
    if bkQuery:
      processingPass = bkQuery.getProcessingPass()
      if '...' in processingPass or '*' in processingPass:
        bkPath = pluginScript.getOption('BKPath').replace('RealData', 'Real Data')
        if listProcessingPasses:
          gLogger.notice("List of processing passes for BK path", bkPath)
        processingPasses = getProcessingPasses(bkQuery, depth=depth)
        if processingPasses:
          if not listProcessingPasses:
            gLogger.notice("Transformations will be launched for the following list of processing passes:")
          gLogger.notice('\n'.join([''] + processingPasses))
        else:
          gLogger.notice("No processing passes matching the BK path")
          DIRAC.exit(0)
        if listProcessingPasses:
          DIRAC.exit(0)
        # Create a list of BK queries, taking into account visibility and if needed excluded file types
        exceptTypes = bkQuery.getExceptFileTypes()
        visible = bkQuery.isVisible()
        bkQueries = []
        for pp in processingPasses:
          query = BKQuery(bkPath.replace(processingPass, pp), visible=visible)
          query.setExceptFileTypes(exceptTypes)
          bkQueries.append(query)
      else:
        # Single BK query
        bkQueries = [bkQuery]

  reqID = pluginScript.getRequestID()
  if not requestID and reqID:
    requestID = reqID

  transGroup = plugin
  for bkQuery in bkQueries:
    if len(bkQueries) > 1:
      gLogger.notice("**************************************")
    # Create the transformation
    transformation = Transformation()
    transformation.setType(transType)
    transName = transType

    # In case there is a loop on processing passes
    if bkQuery:
      transBKQuery = bkQuery.getQueryDict()
    if requestedLFNs:
      longName = transGroup + " for %d LFNs" % len(requestedLFNs)
      transName += '-LFNs'
    elif prods:
      if not fileType:
        fileType = ["All"]
      prodsStr = ','.join(str(p) for p in prods)
      fileStr = ','.join(fileType)
      longName = transGroup + " of " + fileStr + " for productions %s " % prodsStr
      if len(prods) > 5:
        prodsStr = '%d-productions' % len(prods)
      transName += '-' + fileStr + '-' + prodsStr
    elif transBKQuery and 'BKPath' not in pluginScript.getOptions():
      if isinstance(transBKQuery['FileType'], list):
        strQuery = ','.join(transBKQuery['FileType'])
      else:
        strQuery = str(transBKQuery['FileType'])
      longName = transGroup + " for fileType " + strQuery
      transName += '-' + str(transBKQuery['FileType'])
    elif bkQuery:
      queryPath = bkQuery.getPath()
      longName = transGroup + " for BKQuery " + queryPath
      transName += '-' + queryPath
    else:
      transName = ''

    dqFlag = transBKQuery.get('DataQuality', [])
    if 'BAD' in dqFlag:
      dqFlag = ' (DQ: %s)' % ','.join(dqFlag)
      transName += dqFlag
      longName += dqFlag

    if requestID:
      transName += '-Request%s' % (requestID)
    # If a name is given in the options, use it if LFNs are given or forced
    if nameOption and (requestedLFNs or force):
      transName = nameOption
      longName = transGroup + ' - ' + transName
    if not transName:
      gLogger.fatal("Didn't manage to find a name for this transformation, check options")
      DIRAC.exit(1)
    # Check if transformation exists
    if unique:
      res = tr.getTransformation(transName)
      if not res['OK']:
        res = tr.getTransformation(transName + '/')
        if not res['OK']:
          res = tr.getTransformation(transName.replace('-/', '-'))
      if res['OK']:
        gLogger.notice("Transformation %s already exists with ID %d, status %s" % (transName,
                                                                                   res['Value']['TransformationID'],
                                                                                   res['Value']['Status']))
        continue
    transformation.setTransformationName(transName)
    transformation.setTransformationGroup(transGroup)
    transformation.setDescription(longName)
    transformation.setLongDescription(longName)
    transformation.setType(transType)
    transBody = None
    if transType == "Removal":
      if plugin == "DestroyDataset":
        transBody = "removal;RemoveFile"
      elif plugin == "DestroyDatasetWhenProcessed":
        plugin = "DeleteReplicasWhenProcessed"
        transBody = "removal;RemoveFile"
        # Set the polling period to 0 if not defined
        pluginParams.setdefault('Period', 0)
      else:
        transBody = "removal;RemoveReplica"
      transformation.setBody(transBody)

    if pluginSEParams:
      for key, val in pluginSEParams.iteritems():
        res = transformation.setSEParam(key, val)
        if not res['OK']:
          gLogger.error('Error setting SE parameter', res['Message'])
          DIRAC.exit(1)
    if pluginParams:
      for key, val in pluginParams.iteritems():
        res = transformation.setAdditionalParam(key, val)
        if not res['OK']:
          gLogger.error('Error setting additional parameter', res['Message'])
          DIRAC.exit(1)

    transformation.setPlugin(plugin)

    if test:
      gLogger.notice("Transformation type:", transType)
      gLogger.notice("Transformation Name:", transName)
      gLogger.notice("Transformation group:", transGroup)
      gLogger.notice("Long description:", longName)
      gLogger.notice("Transformation body:", transBody)
      if transBKQuery:
        gLogger.notice("BK Query:", transBKQuery)
      elif requestedLFNs:
        gLogger.notice("List of %d LFNs" % len(requestedLFNs))
      else:
        # Should not happen here, but who knows ;-)
        gLogger.error("No BK query provided...")
        Script.showHelp()
        DIRAC.exit(0)

    if force:
      lfns = []
    elif transBKQuery:
      progressBar = ProgressBar(1, title="Executing the BK query: %s" % bkQuery)
      lfns = bkQuery.getLFNs(printSEUsage=(transType == 'Removal' and
                                           not pluginScript.getOption('Runs')),
                             printOutput=test)
      progressBar.endLoop(message=("found %d files" % len(lfns)))
    else:
      lfns = requestedLFNs
    nfiles = len(lfns)

    if test:
      gLogger.notice("Plugin:", plugin)
      gLogger.notice("Parameters:", pluginParams)
      gLogger.notice("RequestID:", requestID)
      continue

    if not force and nfiles == 0:
      gLogger.notice("No files found from BK query %s" % str(bkQuery))
      gLogger.notice("If you anyway want to submit the transformation, use option --Force")
      continue

    if userGroup:
      directories = set(os.path.dirname(lfn) for lfn in lfns)
      res = chown(directories, user=userGroup[0], group=userGroup[1])
      if not res['OK']:
        gLogger.fatal("Error changing ownership", res['Message'])
        DIRAC.exit(3)
      gLogger.notice("Successfully changed owner/group for %d directories" % res['Value'])
    # If the transformation is a removal transformation,
    #  check all files are in the FC. If not, remove their replica flag
    if fcCheck and transType == 'Removal':
      from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
      fc = FileCatalog()
      success = 0
      missingLFNs = set()
      chunkSize = 1000
      progressBar = ProgressBar(len(lfns), title="Checking %d files in FC" % len(lfns), chunk=chunkSize)
      for lfnChunk in breakListIntoChunks(lfns, chunkSize):
        progressBar.loop()
        res = fc.exists(lfnChunk)
        if res['OK']:
          success += len(res['Value']['Successful'])
          missingLFNs |= set(res['Value']['Failed'])
        else:
          gLogger.fatal("\nError checking files in the FC", res['Message'])
          DIRAC.exit(2)
      progressBar.endLoop(message='all found' if not missingLFNs else None)
      if missingLFNs:
        gLogger.notice('%d are in the FC, %d are not. Attempting to remove GotReplica' % (success, len(missingLFNs)))
        res = bk.removeFiles(list(missingLFNs))
        if res['OK']:
          gLogger.notice("Replica flag successfully removed in BK")
        else:
          gLogger.fatal("Error removing BK flag", res['Message'])
          DIRAC.exit(2)

    # Prepare the transformation
    if transBKQuery:
      # !!!! Hack in order to remain compatible with hte BKQuery table...
      for transKey, bkKey in (('ProductionID', 'Production'),
                              ('RunNumbers', 'RunNumber'),
                              ('DataQualityFlag', 'DataQuality')):
        if bkKey in transBKQuery:
          transBKQuery[transKey] = transBKQuery.pop(bkKey)
      transformation.setBkQuery(transBKQuery)

    # If the transformation uses the RemoveDatasetFromDisk plugin, set the files invisible in the BK...
    # Try and let them visible such that users can see they are archived...
    # It was:
    # setInvisiblePlugins = ("RemoveDatasetFromDisk", )
    setInvisiblePlugins = tuple()
    if invisible or plugin in setInvisiblePlugins:
      chunkSize = 1000
      progressBar = ProgressBar(len(lfns), title='Setting %d files invisible' % len(lfns), chunk=chunkSize)
      okFiles = 0
      for lfnChunk in breakListIntoChunks(lfns, chunkSize):
        progressBar.loop()
        res = bk.setFilesInvisible(lfnChunk)
        if res['OK']:
          okFiles += len(lfnChunk)
      if okFiles == len(lfns):
        msg = "all files successfully set invisible in BK"
      else:
        msg = "%d files successfully set invisible in BK" % okFiles
      progressBar.endLoop(message=msg)
      if res['OK']:
        if transBKQuery:
          savedVisi = transBKQuery.get('Visible')
          transBKQuery['Visible'] = 'All'
          transformation.setBkQuery(transBKQuery.copy())
          if savedVisi:
            transBKQuery['Visible'] = savedVisi
          else:
            transBKQuery.pop('Visible')
      else:
        gLogger.error("Failed to set files invisible: ", res['Message'])

    trial = 0
    errMsg = ''
    while True:
      result = transformation.addTransformation()
      if not result['OK']:
        if not unique and result['Message'].find("already exists") >= 0:
          trial += 1
          tName = transName + "-" + str(trial)
          transformation.setTransformationName(tName)
          continue
        else:
          errMsg = "Couldn't create transformation:\n%s" % result['Message']
          break
      result = transformation.getTransformationID()
      if result['OK']:
        transID = result['Value']
      else:
        errMsg = "Error getting transformationID: %s" % res['Message']
        break
      if requestedLFNs:
        from LHCbDIRAC.TransformationSystem.Utilities.PluginUtilities import addFilesToTransformation
        res = addFilesToTransformation(transID, requestedLFNs, addRunInfo=True)
        if not res['OK']:
          errMsg = "Could not add %d files to transformation: %s" % (len(requestedLFNs), res['Message'])
          break
        gLogger.notice("%d files successfully added to transformation" % len(res['Value']))
      if requestID:
        transformation.setTransformationFamily(requestID)
      if start:
        transformation.setStatus('Active')
        transformation.setAgentType('Automatic')
      gLogger.notice("Transformation %d created" % transID)
      gLogger.notice("Name: %s, Description:%s" % (transName, longName))
      gLogger.notice("Transformation body:", transBody)
      gLogger.notice("Plugin:", plugin)
      if pluginParams:
        gLogger.notice("Additional parameters:", pluginParams)
      if requestID:
        gLogger.notice("RequestID:", requestID)
      break
    if errMsg:
      gLogger.notice(errMsg)

  DIRAC.exit(0)
