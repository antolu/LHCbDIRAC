"""
Database interface
"""

__RCSID__ = "$Id$"

class IBookkeepingDatabaseClient(object):
  """stores a Entity manager and expose its method"""
  #############################################################################
  def __init__(self, databaseManager):
    """initialize a manager"""
    self.databaseManager_ = databaseManager

  #############################################################################
  def getManager(self):
    """current manager"""
    return self.databaseManager_

  #############################################################################
  def getAvailableSteps(self, in_dict):
    "more info in the BookkeepingClient.py"
    return self.getManager().getAvailableSteps(in_dict)

  #############################################################################
  def getAvailableFileTypes(self):
    "more info in the BookkeepingClient.py"
    return self.getManager().getAvailableFileTypes()

  #############################################################################
  def insertFileTypes(self, ftype, desc, fileType):
    "more info in the BookkeepingClient.py"
    return self.getManager().insertFileTypes(ftype, desc, fileType)

  #############################################################################
  def insertStep(self, in_dict):
    "more info in the BookkeepingClient.py"
    return self.getManager().insertStep(in_dict)

  #############################################################################
  def deleteStep(self, stepid):
    "more info in the BookkeepingClient.py"
    return self.getManager().deleteStep(stepid)

  #############################################################################
  def updateStep(self, in_dict):
    "more info in the BookkeepingClient.py"
    return self.getManager().updateStep(in_dict)

  #############################################################################
  def getStepInputFiles(self, stepId):
    "more info in the BookkeepingClient.py"
    return self.getManager().getStepInputFiles(stepId)

  #############################################################################
  def setStepInputFiles(self, stepid, files):
    "more info in the BookkeepingClient.py"
    return self.getManager().setStepInputFiles(stepid, files)

  #############################################################################
  def setStepOutputFiles(self, stepid, files):
    "more info in the BookkeepingClient.py"
    return self.getManager().setStepOutputFiles(stepid, files)

  #############################################################################
  def getStepOutputFiles(self, stepId):
    "more info in the BookkeepingClient.py"
    return self.getManager().getStepOutputFiles(stepId)

  #############################################################################
  def getAvailableConfigNames(self):
    "more info in the BookkeepingClient.py"
    return self.getManager().getAvailableConfigNames()

  #############################################################################
  def getConfigVersions(self, configname):
    "more info in the BookkeepingClient.py"
    return self.getManager().getConfigVersions(configname)
  #############################################################################
  def getConditions(self, configName, configVersion, evt):
    "more info in the BookkeepingClient.py"
    return self.getManager().getConditions(configName, configVersion, evt)

  #############################################################################
  def getProcessingPass(self, configName, configVersion, conddescription, runnumber, prod, evt, path):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProcessingPass(configName, configVersion, conddescription, runnumber, prod, evt, path)

  #############################################################################
  def getProductions(self, configName, configVersion, conddescription, processing, evt, visible, ftype, replicaFlag):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductions(configName, configVersion, conddescription, processing, evt, visible, ftype, replicaFlag)

  #############################################################################
  def getFileTypes(self, configName, configVersion, conddescription, processing, evt, runnb, production, visible):
    "more info in the BookkeepingClient.py"
    return self.getManager().getFileTypes(configName, configVersion,
                                          conddescription, processing,
                                          evt, runnb, production, visible)

  #############################################################################
  def getFilesWithMetadata( self, configName, configVersion, conddescription,
                           processing, evt, production, filetype, quality, visible,
                           replicaflag, startDate, endDate,
                           runnumbers, startRunID, endRunID, tcks ):
    "more info in the BookkeepingClient.py"
    return self.getManager().getFilesWithMetadata( configName, configVersion,
                                                  conddescription, processing,
                                                  evt, production, filetype,
                                                  quality, visible, replicaflag,
                                                  startDate, endDate,
                                                  runnumbers , startRunID, endRunID, tcks )

  #############################################################################
  def getFilesSummary( self, configName, configVersion,
                      conddescription, processing, evt,
                      production, filetype, quality, runnb,
                      startrun, endrun, visible, startDate,
                      endDate, runnumbers, replicaflag, tcks ):
    "more info in the BookkeepingClient.py"
    return self.getManager().getFilesSummary( configName, configVersion, conddescription,
                                             processing, evt, production, filetype, quality,
                                             runnb, startrun, endrun, visible, startDate,
                                             endDate, runnumbers, replicaflag, tcks )

  #############################################################################
  def getLimitedFiles(self, configName, configVersion, conddescription,
                      processing, evt, production, filetype, quality,
                      runnb, startitem, maxiten):
    "more info in the BookkeepingClient.py"
    return self.getManager().getLimitedFiles(configName, configVersion, conddescription,
                                             processing, evt, production, filetype, quality,
                                             runnb, startitem, maxiten)

  #############################################################################
  def getAvailableDataQuality(self):
    "more info in the BookkeepingClient.py"
    return self.getManager().getAvailableDataQuality()

  #############################################################################
  def getAvailableProductions(self):
    "more info in the BookkeepingClient.py"
    return self.getManager().getAvailableProductions()

  #############################################################################
  def getAvailableRuns(self):
    "more info in the BookkeepingClient.py"
    return self.getManager().getAvailableRuns()

  #############################################################################
  def getAvailableEventTypes(self):
    "more info in the BookkeepingClient.py"
    return self.getManager().getAvailableEventTypes()

  #############################################################################
  def getMoreProductionInformations(self, prodid):
    "more info in the BookkeepingClient.py"
    return self.getManager().getMoreProductionInformations(prodid)

  #############################################################################
  def getJobInfo(self, lfn):
    "more info in the BookkeepingClient.py"
    return self.getManager().getJobInfo(lfn)

  #############################################################################
  def bulkJobInfo(self, lfns):
    "more info in the BookkeepingClient.py"
    return self.getManager().bulkJobInfo(lfns)

  #############################################################################
  def getJobInformation(self, in_dict):
    "more info in the BookkeepingClient.py"
    return self.getManager().getJobInformation(in_dict)

  #############################################################################
  def getRunNumber(self, lfn):
    "more info in the BookkeepingClient.py"
    return self.getManager().getRunNumber(lfn)

  #############################################################################
  def getProductionFiles(self, prod, ftype, gotreplica='ALL'):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductionFiles(prod, ftype, gotreplica)

  #############################################################################
  def getRunFiles(self, runid):
    "more info in the BookkeepingClient.py"
    return self.getManager().getRunFiles(runid)

  #############################################################################
  def updateFileMetaData(self, filename, filesAttr):
    "more info in the BookkeepingClient.py"
    return self.getManager().updateFileMetaData(filename, filesAttr)

  #############################################################################
  def renameFile(self, oldLFN, newLFN):
    "more info in the BookkeepingClient.py"
    return self.getManager().renameFile(oldLFN, newLFN)

  #############################################################################
  def insertTag(self, name, tag):
    "more info in the BookkeepingClient.py"
    return self.getManager().insertTag(name, tag)

  #############################################################################
  def setRunAndProcessingPassDataQuality(self, runNB, procpass, flag):
    "more info in the BookkeepingClient.py"
    return self.getManager().setRunAndProcessingPassDataQuality(runNB, procpass, flag)

  #############################################################################
  def setRunDataQuality(self, runNb, flag):
    "more info in the BookkeepingClient.py"
    return self.getManager().setRunDataQuality(runNb, flag)

  #############################################################################
  def setProductionDataQuality(self, prod, flag):
    "more info in the BookkeepingClient.py"
    return self.getManager().setProductionDataQuality(prod, flag)

  #############################################################################
  def getFileAncestors(self, lfn, depth, replica):
    "more info in the BookkeepingClient.py"
    return self.getManager().getFileAncestors(lfn, depth, replica)

  #############################################################################
  def getFileDescendents(self, lfn, depth, production, checkreplica):
    "more info in the BookkeepingClient.py"
    return self.getManager().getFileDescendents(lfn, depth, production, checkreplica)

  #############################################################################
  def checkfile(self, fileName): #file
    "more info in the BookkeepingClient.py"
    return self.getManager().checkfile(fileName)

  #############################################################################
  def checkFileTypeAndVersion(self, filetype, version): #fileTypeAndFileTypeVersion(self, type, version):
    "more info in the BookkeepingClient.py"
    return self.getManager().checkFileTypeAndVersion(filetype, version)

  #############################################################################
  def checkEventType(self, eventTypeId):  #eventType(self, eventTypeId):
    "more info in the BookkeepingClient.py"
    return self.getManager().checkEventType(eventTypeId)

  #############################################################################
  def insertJob(self, job):
    "more info in the BookkeepingClient.py"
    return self.getManager().insertJob(job)

  #############################################################################
  def insertInputFile(self, jobID, fileId):
    "more info in the BookkeepingClient.py"
    return self.getManager().insertInputFile(jobID, fileId)

  #############################################################################
  def insertOutputFile(self, fileName):
    "more info in the BookkeepingClient.py"
    return self.getManager().insertOutputFile(fileName)

  #############################################################################
  def updateReplicaRow(self, fileID, replica):
    "more info in the BookkeepingClient.py"
    return self.getManager().updateReplicaRow(fileID, replica)

  #############################################################################
  def deleteJob(self, job):
    "more info in the BookkeepingClient.py"
    return self.getManager().deleteJob(job)

  #############################################################################
  def deleteInputFiles(self, jobid):
    "more info in the BookkeepingClient.py"
    return self.getManager().deleteInputFiles(jobid)

  #############################################################################
  def deleteFiles(self, lfns):
    "more info in the BookkeepingClient.py"
    return self.getManager().deleteFiles(lfns)

  #############################################################################
  def insertSimConditions(self, in_dict):
    "more info in the BookkeepingClient.py"
    return self.getManager().insertSimConditions(in_dict)

  #############################################################################
  def getSimConditions(self):
    "more info in the BookkeepingClient.py"
    return self.getManager().getSimConditions()

  #############################################################################
  def removeReplica(self, fileName):
    "more info in the BookkeepingClient.py"
    return self.getManager().removeReplica(fileName)

  #############################################################################
  def getFileMetadata(self, lfns):
    "more info in the BookkeepingClient.py"
    return self.getManager().getFileMetadata(lfns)

  #############################################################################
  def getFileMetaDataForWeb(self, lfns):
    "more info in the BookkeepingClient.py"
    return self.getManager().getFileMetaDataForWeb(lfns)

  #############################################################################
  def getProductionFilesForWeb(self, prod, ftypeDict, sortDict, startItem, maxitems):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductionFilesForWeb(prod, ftypeDict, sortDict, startItem, maxitems)

  #############################################################################
  def exists(self, lfns):
    "more info in the BookkeepingClient.py"
    return self.getManager().exists(lfns)

  #############################################################################
  def addReplica(self, fileName):
    "more info in the BookkeepingClient.py"
    return self.getManager().addReplica(fileName)

  #############################################################################
  def getRunInformations(self, runnb):
    "more info in the BookkeepingClient.py"
    return self.getManager().getRunInformations(runnb)

  #############################################################################
  def getRunInformation(self, runnb):
    "more info in the BookkeepingClient.py"
    return self.getManager().getRunInformation(runnb)

  #############################################################################
  def getFileCreationLog(self, lfn):
    "more info in the BookkeepingClient.py"
    return self.getManager().getFileCreationLog(lfn)

  #############################################################################
  def updateEventType(self, evid, desc, primary):
    "more info in the BookkeepingClient.py"
    return self.getManager().updateEventType(evid, desc, primary)

  #############################################################################
  def getProductionSummary(self, cName, cVersion, simdesc, pgroup, production, ftype, evttype):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductionSummary(cName, cVersion, simdesc, pgroup, production, ftype, evttype)

  #############################################################################
  def getFileHistory(self, lfn):
    "more info in the BookkeepingClient.py"
    return self.getManager().getFileHistory(lfn)

  #############################################################################
  def getProductionInformationsFromView(self, prodid):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductionInformationsFromView(prodid)

  #############################################################################
  def getProductionNbOfJobs(self, prodid):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductionNbOfJobs(prodid)

  #############################################################################
  def getProductionNbOfEvents(self, prodid):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductionNbOfEvents(prodid)

  #############################################################################
  def getProductionSizeOfFiles(self, prodid):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductionSizeOfFiles(prodid)

  #############################################################################
  def getProductionNbOfFiles(self, prodid):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductionNbOfFiles(prodid)

  #############################################################################
  def getProductionInformation(self, prodid):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductionInformation(prodid)

  #############################################################################
  def getSteps(self, prodid):
    "more info in the BookkeepingClient.py"
    return self.getManager().getSteps(prodid)

  #############################################################################
  def getNbOfJobsBySites(self, prodid):
    "more info in the BookkeepingClient.py"
    return self.getManager().getNbOfJobsBySites(prodid)

  #############################################################################
  def getConfigsAndEvtType(self, prodid):
    "more info in the BookkeepingClient.py"
    return self.getManager().getConfigsAndEvtType(prodid)

  #############################################################################
  def getAvailableTags(self):
    "more info in the BookkeepingClient.py"
    return self.getManager().getAvailableTags()

  #############################################################################
  def getProductionProcessedEvents(self, prodid):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductionProcessedEvents(prodid)

  #############################################################################
  def getRunsForAGivenPeriod(self, in_dict):
    "more info in the BookkeepingClient.py"
    return self.getManager().getRunsForAGivenPeriod(in_dict)

  #############################################################################
  def getProductionsFromView(self, in_dict):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductionsFromView(in_dict)

  #############################################################################
  def getRunFilesDataQuality(self, runs):
    "more info in the BookkeepingClient.py"
    return self.getManager().getRunFilesDataQuality(runs)

  #############################################################################
  def setFilesInvisible(self, lfns):
    "more info in the BookkeepingClient.py"
    return self.getManager().setFilesInvisible(lfns)

  #############################################################################
  def setFilesVisible(self, lfns):
    "more info in the BookkeepingClient.py"
    return self.getManager().setFilesVisible(lfns)

  #############################################################################
  def getTotalProcessingPass(self, prod):
    "more info in the BookkeepingClient.py"
    return self.getManager().getTotalProcessingPass(prod)

  #############################################################################
  def getRunAndProcessingPassDataQuality(self, runnb, processing):
    "more info in the BookkeepingClient.py"
    return self.getManager().getRunAndProcessingPassDataQuality(runnb, processing)

  #############################################################################
  def getAvailableConfigurations(self):
    "more info in the BookkeepingClient.py"
    return self.getManager().getAvailableConfigurations()

  #############################################################################
  def getProductionProcessingPassID(self, prodid):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductionProcessingPassID(prodid)

  #############################################################################
  def getProductionProcessingPass(self, prodid):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductionProcessingPass(prodid)

  #############################################################################
  def getRunProcessingPass(self, runnumber):
    "more info in the BookkeepingClient.py"
    return self.getManager().getRunProcessingPass(runnumber)

  #############################################################################
  def getProductionFilesStatus(self, productionid, lfns):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductionFilesStatus(productionid, lfns)

  #############################################################################
  def getProductionSimulationCond(self, prod):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductionSimulationCond(prod)

  #############################################################################
  def getFiles(self, simdesc, datataking, procPass, ftype, evt, configName,
               configVersion, production, flag, startDate,
               endDate, nbofEvents, startRunID, endRunID,
               runnumbers, replicaFlag, visible, filesize, tck):
    "more info in the BookkeepingClient.py"
    return self.getManager().getFiles(simdesc, datataking, procPass,
                                      ftype, evt, configName, configVersion, production,
                                      flag, startDate, endDate, nbofEvents, startRunID,
                                      endRunID, runnumbers, replicaFlag, visible, filesize, tck)

  #############################################################################
  def getVisibleFilesWithMetadata(self, simdesc, datataking, procPass, ftype, evt, configName,
                                  configVersion, production, flag, startDate, endDate,
                                  nbofEvents, startRunID, endRunID, runnumbers, replicaFlag, tcks):
    "more info in the BookkeepingClient.py"
    return self.getManager().getVisibleFilesWithMetadata(simdesc, datataking, procPass,
                                                         ftype, evt, configName, configVersion,
                                                         production, flag,startDate,
                                                         endDate, nbofEvents, startRunID,
                                                         endRunID, runnumbers,
                                                         replicaFlag, tcks)

  #############################################################################
  def getDataTakingCondId(self, condition):
    "more info in the BookkeepingClient.py"
    return self.getManager().getDataTakingCondId(condition)

  #############################################################################
  def getStepIdandName(self, programName, programVersion):
    "more info in the BookkeepingClient.py"
    return self.getManager().getStepIdandName(programName, programVersion)

  #############################################################################
  def addProduction(self, production, simcond, daq, steps, inputproc):
    "more info in the BookkeepingClient.py"
    return self.getManager().addProduction(production, simcond, daq, steps, inputproc)

  #############################################################################
  def checkProcessingPassAndSimCond(self, production):
    "more info in the BookkeepingClient.py"
    return self.getManager().checkProcessingPassAndSimCond(production)

  #############################################################################
  def getEventTypes(self, configName, configVersion, production):
    "more info in the BookkeepingClient.py"
    return self.getManager().getEventTypes(configName, configVersion, production)

  #############################################################################
  def getProcessingPassSteps(self, procpass, cond, stepname):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProcessingPassSteps(procpass, cond, stepname)

  #############################################################################
  def getProductionProcessingPassSteps(self, prod):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductionProcessingPassSteps(prod)

  #############################################################################
  def getStepIdandNameForRUN(self, programName, programVersion, conddb, dddb):
    "more info in the BookkeepingClient.py"
    return self.getManager().getStepIdandNameForRUN(programName, programVersion, conddb, dddb)

  #############################################################################
  def getDataTakingCondDesc(self, condition):
    "more info in the BookkeepingClient.py"
    return self.getManager().getDataTakingCondDesc(condition)

  #############################################################################
  def getProductionOutputFileTypes(self, prod):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductionOutputFileTypes(prod)

  #############################################################################
  def existsTag(self, name, value):
    "more info in the BookkeepingClient.py"
    return self.getManager().existsTag(name, value)

  #############################################################################
  def getRunWithProcessingPassAndDataQuality(self, procpass, flag):
    "more info in the BookkeepingClient.py"
    return self.getManager().getRunWithProcessingPassAndDataQuality(procpass, flag)

  #############################################################################
  def insertDataTakingCond(self, conditions):
    "more info in the BookkeepingClient.py"
    return self.getManager().insertDataTakingCond(conditions)

  #############################################################################
  def deleteSetpContiner(self, prod):
    "more info in the BookkeepingClient.py"
    return self.getManager().deleteSetpContiner(prod)

  #############################################################################
  def getRunNbAndTck(self, lfn):
    "more info in the BookkeepingClient.py"
    return self.getManager().getRunNbAndTck(lfn)

  #############################################################################
  def deleteProductionsContiner(self, prod):
    "more info in the BookkeepingClient.py"
    return self.getManager().deleteProductionsContiner(prod)

  #############################################################################
  def insertEventTypes(self, evid, desc, primary):
    "more info in the BookkeepingClient.py"
    return self.getManager().insertEventTypes(evid, desc, primary)

  #############################################################################
  def getRuns(self, cName, cVersion):
    "more info in the BookkeepingClient.py"
    return self.getManager().getRuns(cName, cVersion)

  #############################################################################
  def getRunAndProcessingPass(self, runnb):
    "more info in the BookkeepingClient.py"
    return self.getManager().getRunAndProcessingPass(runnb)

  #############################################################################
  def getProcessingPassId(self, fullpath):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProcessingPassId(fullpath)

  #############################################################################
  def getNbOfRawFiles( self, runid, eventtype, replicaFlag, visible, isFinished ):
    "more info in the BookkeepingClient.py"
    return self.getManager().getNbOfRawFiles( runid, eventtype, replicaFlag, visible, isFinished )

  #############################################################################
  def getFileTypeVersion(self, lfn):
    "more info in the BookkeepingClient.py"
    return self.getManager().getFileTypeVersion(lfn)

  #############################################################################
  def insertRuntimeProject(self, projectid, runtimeprojectid):
    "more info in the BookkeepingClient.py"
    return self.getManager().insertRuntimeProject(projectid, runtimeprojectid)

  #############################################################################
  def getRuntimeProjects(self, in_dict):
    "more info in the BookkeepingClient.py"
    return self.getManager().getRuntimeProjects(in_dict)

  #############################################################################
  def updateRuntimeProject(self, projectid, runtimeprojectid):
    "more info in the BookkeepingClient.py"
    return self.getManager().updateRuntimeProject(projectid, runtimeprojectid)

  #############################################################################
  def removeRuntimeProject(self, stepid):
    "more info in the BookkeepingClient.py"
    return self.getManager().removeRuntimeProject(stepid)

  #############################################################################
  def getTCKs(self, configName, configVersion, conddescription, processing,
              evt, production, filetype, quality, runnb):
    "more info in the BookkeepingClient.py"
    return self.getManager().getTCKs(configName, configVersion,
                                     conddescription, processing,
                                     evt, production, filetype,
                                     quality, runnb)

  #############################################################################
  def getStepsMetadata(self, configName, configVersion, cond, procpass, evt,
                        production, filetype, runnb):
    "more info in the BookkeepingClient.py"
    return self.getManager().getStepsMetadata(configName, configVersion,
                                              cond, procpass, evt,
                                              production, filetype, runnb)

  #############################################################################
  def getDirectoryMetadata(self, lfn):
    "more info in the BookkeepingClient.py"
    return self.getManager().getDirectoryMetadata(lfn)

  #############################################################################
  def getDirectoryMetadata_new(self, lfn):
    "more info in the BookkeepingClient.py"
    return self.getManager().getDirectoryMetadata_new(lfn)

  #############################################################################
  def getFilesForGUID(self, guid):
    "more info in the BookkeepingClient.py"
    return self.getManager().getFilesForGUID(guid)

  #############################################################################
  def setFileDataQuality(self, lfns, flag):
    "more info in the BookkeepingClient.py"
    return self.getManager().setFileDataQuality(lfns, flag)

  #############################################################################
  def getRunsGroupedByDataTaking(self):
    "more info in the BookkeepingClient.py"
    return self.getManager().getRunsGroupedByDataTaking()

  #############################################################################
  def getListOfFills(self, configName, configVersion, conddescription):
    "more info in the BookkeepingClient.py"
    return self.getManager().getListOfFills(configName, configVersion, conddescription)

  #############################################################################
  def getRunsForFill(self, fillid):
    "more info in the BookkeepingClient.py"
    return self.getManager().getRunsForFill(fillid)

  #############################################################################
  def getListOfRuns(self, configName, configVersion, conddescription, processing, evt, quality):
    "more info in the BookkeepingClient.py"
    return self.getManager().getListOfRuns(configName, configVersion, conddescription, processing, evt, quality)

  #############################################################################
  def getSimulationConditions(self, in_dict):
    "more info in the BookkeepingClient.py"
    return self.getManager().getSimulationConditions(in_dict)

  #############################################################################
  def updateSimulationConditions(self, in_dict):
    "more info in the BookkeepingClient.py"
    return self.getManager().updateSimulationConditions(in_dict)

  #############################################################################
  def deleteSimulationConditions(self, simid):
    "more info in the BookkeepingClient.py"
    return self.getManager().deleteSimulationConditions(simid)

  #############################################################################
  def getProductionSummaryFromView(self, in_dict):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductionSummaryFromView(in_dict)

  #############################################################################
  def getJobInputOutputFiles(self, diracjobids):
    "more info in the BookkeepingClient.py"
    return self.getManager().getJobInputOutputFiles(diracjobids)
  
  #############################################################################
  def insertRunStatus( self, runnumber, jobId, isFinished ):
    "more info in the BookkeepingClient.py"
    return self.getManager().insertRunStatus( runnumber, jobId, isFinished )
  
  #############################################################################
  def setRunStatusFinished( self, runnumber, isFinished ):
    "more info in the BookkeepingClient.py"
    return self.getManager().setRunStatusFinished( runnumber, isFinished )
  
  #############################################################################
  def getRunStatus( self, runnumbers ):
    "more info in the BookkeepingClient.py"
    return self.getManager().getRunStatus( runnumbers )
  
  #############################################################################
  def bulkupdateFileMetaData( self, lfnswithmeta ):
    "more info in the BookkeepingClient.py"
    return self.getManager().bulkupdateFileMetaData( lfnswithmeta )
  
  #############################################################################
  def fixRunLuminosity( self, runnumbers ):
    "more info in the BookkeepingClient.py"
    return self.getManager().fixRunLuminosity( runnumbers )
  
  #############################################################################
  def getProductionProducedEvents( self, prodid ):
    "more info in the BookkeepingClient.py"
    return self.getManager().getProductionProducedEvents( prodid )
  
