########################################################################
# $Id$
########################################################################

"""
dict = {'EventTypeId': 93000000,
        'ConfigVersion': 'Collision10',
        'ProcessingPass': '/Real Data',
        'ConfigName': 'LHCb',
        'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
        'Production':7421
         }
"""
import DIRAC
from DIRAC                           import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient      import RPCClient
from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC.Core.Utilities            import DEncode
import types, cPickle, os, tempfile, time


__RCSID__ = "$Id$"

class BookkeepingClient:

  def __init__(self, rpcClient=None):
    self.rpcClient = rpcClient

  def __getServer(self, timeout=3600):
    retVal = None
    if self.rpcClient:
      retVal = self.rpcClient
    else:
      retVal = RPCClient('Bookkeeping/BookkeepingManager', timeout=timeout)

    if not retVal.ping()['OK']:
      wait = 10
      while not retVal.ping()['OK'] and wait <= 60:
        time.sleep(wait)
        wait += 10
    return retVal

  #############################################################################
  def echo(self, string):
    server = self.__getServer()
    res = server.echo(string)
    print res

  #############################################################################
  def sendXMLBookkeepingReport(self, xml):
    """
    This method is used to upload an xml report which is produced after when the job successfully finished.
    The input parameter 'xml' is a string which contains various information (metadata) about the finished job in the Grid in an XML format.
    """
    server = self.__getServer()
    result = server.sendXMLBookkeepingReport(xml)
    return result

  #############################################################################
  def getAvailableSteps(self, dict={}):
    """
    It returns all the available steps which corresponds to a given conditions. The dict contains the following conditions: StartDate, StepId, InputFileTypes, OutputFileTypes,
    ApplicationName, ApplicationVersion, OptionFiles, DDDB, CONDDB, ExtraPackages, Visible, ProcessingPass, Usable, RuntimeProjects, DQTag, OptionsFormat, StartItem, MaxItem
    """
    server = self.__getServer()
    return server.getAvailableSteps(dict)

  #############################################################################
  def getRuntimeProjects(self, dict={}):
    """
    It returns a runtime project for a given step. The input parameter is a dictionary which has only one key StepId
    """
    server = self.__getServer()
    return server.getRuntimeProjects(dict)

  #############################################################################
  def getAvailableFileTypes(self):
    """
    It returns all the available files which are registered to the bkk.
    """
    result = S_ERROR()
    server = self.__getServer()
    retVal = server.getAvailableFileTypes()
    if retVal['OK']:
      records = []
      parameters = ["FileType", "Description"]
      for record in retVal['Value']:
        records += [list(record)]
      result = S_OK({'ParameterNames':parameters, 'Records':records, 'TotalRecords':len(records)})
    else:
      result = retVal
    return result

  #############################################################################
  def insertFileTypes(self, ftype, desc, fileType):
    """
    It is used to register a file type. It has the following input parameters:
      -ftype: file type; for example: COOL.DST
      -desc: a short description which describes the file content
      -fileType: the file format such as ROOT, POOL_ROOT, etc.
    """
    server = self.__getServer()
    return server.insertFileTypes(ftype, desc, fileType)

  #############################################################################
  def insertStep(self, dict):
    """
    It used to insert a step to the Bookkeeping Metadata Catalogue. The imput parameter is a dictionary which contains the steps attributes.
    For example: Dictionary format: {'Step': {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': '', 'ApplicationVersion': 'v29r1', 'ext-comp-1273': 'CHARM.MDST (Charm micro dst)', 'ExtraPackages': '', 'StepName': 'davinci prb2', 'ProcessingPass': 'WG-Coool', 'ext-comp-1264': 'CHARM.DST (Charm stream)', 'Visible': 'Y', 'DDDB': '', 'OptionFiles': '', 'CONDDB': ''}, 'OutputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.MDST'}], 'InputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.DST'}],'RuntimeProjects':[{StepId:13878}]}
    """
    server = self.__getServer()
    return server.insertStep(dict)

  #############################################################################
  def deleteStep(self, stepid):
    """
    It used to delete a given step.
    """
    server = self.__getServer()
    return server.deleteStep(int(stepid))

  #############################################################################
  def updateStep(self, dict):
    """
    It is used to modify the step attributes.
    """
    server = self.__getServer()
    return server.updateStep(dict)

  #############################################################################
  def getStepInputFiles(self, StepId):
    """
    It returns the input files for a given step.
    """
    server = self.__getServer()
    return server.getStepInputFiles(int(StepId))

  #############################################################################
  def setStepInputFiles(self, stepid, files):
    """
     It is used to set input file types to a Step.
    """
    server = self.__getServer()
    return server.setStepInputFiles(stepid, files)

  #############################################################################
  def setStepOutputFiles(self, stepid, files):
    """
    It is used to set output file types to a Step.
    """
    server = self.__getServer()
    return server.setStepOutputFiles(stepid, files)

  #############################################################################
  def getStepOutputFiles(self, StepId):
    """
    It returns the output file types for a given Step.
    """
    server = self.__getServer()
    return server.getStepOutputFiles(int(StepId))

  #############################################################################
  def getAvailableConfigNames(self):
    """
    It returns all the available configuration names which are used.
    """
    server = self.__getServer()
    return server.getAvailableConfigNames()

  #############################################################################
  def getConfigVersions(self, dict):
    """
    It returns all the available configuration version for a given condition.
    Input parameter is a dictionary which has the following key: 'ConfigName'
    For example: dict = {'ConfigName':'MC'}
    """
    server = self.__getServer()
    return server.getConfigVersions(dict)

  #############################################################################
  def getConditions(self, dict):
    """
    It returns all the available conditions for a given conditions.
    Input parameter is a dictionary which has the following keys: 'ConfigName', 'ConfigVersion', 'EventType'
    For example: dict = {'ConfigName':'MC','ConfigVersion':'MC10'}
    """
    server = self.__getServer()
    return server.getConditions(dict)

  #############################################################################
  def getProcessingPass(self, dict, path='/'):
    """
    It returns the processing pass for a given conditions.
    Input parameter is a dictionary and a path (string) which has the following keys: 'ConfigName', 'ConfigVersion', 'ConditionDescription','Production', 'RunNumber', 'EventType'
    This method is used to recursively browse the processing pass. To start the browsing you have to define the path as a root: path = '/'
    Note: it returns a list with two dictionary. First dictionary contains the processing passes while the second dictionary contains the event types.
    """
    server = self.__getServer()
    return server.getProcessingPass(dict, path)

  #############################################################################
  def getProcessingPassId(self, fullpath):
    """
    It returns the ProcessingPassId for a given path. this method should not used!
    """
    server = self.__getServer()
    return server.getProcessingPassId(fullpath)

  #############################################################################
  def getProductions(self, dict):
    """
    It returns the productions for a given conditions.
    Input parameter is a dictionary which has the following keys: 'ConfigName', 'ConfigVersion', 'ConditionDescription', 'EventType','ProcessingPass'
    """
    server = self.__getServer()
    return server.getProductions(dict)

  #############################################################################
  def getFileTypes(self, dict):
    """
    It returns the file types for a given conditions.
    Input parameter is a dictionary which has the following keys: 'ConfigName', 'ConfigVersion', 'ConditionDescription', 'EventType','ProcessingPass','Production','RunNumber'
    """
    server = self.__getServer()
    return server.getFileTypes(dict)

  #############################################################################
  def getFilesWithMetadata(self, dict):
    """
    It returns the files for a given conditions.
    Input parameter is a dictionary which has the following keys: 'ConfigName', 'ConfigVersion', 'ConditionDescription', 'EventType','ProcessingPass','Production','RunNumber', 'FileType', DataQuality
    """
    result = S_ERROR()
    bkk = TransferClient('Bookkeeping/BookkeepingManager')
    s = cPickle.dumps(dict)
    file = tempfile.NamedTemporaryFile()
    params = str(s)
    retVal = bkk.receiveFile(file.name, params)
    if not retVal['OK']:
      result = retVal
    else:
      value = cPickle.load(open(file.name))
      file.close()
      result = S_OK(value)
    return result

  #############################################################################
  def getFilesSummary(self, dict):
    """
    It returns sumary for a given data set.
    Input parameter is a dictionary which has the following keys: 'ConfigName', 'ConfigVersion', 'ConditionDescription', 'EventType','ProcessingPass','Production','RunNumber', 'FileType', DataQuality
    """
    server = self.__getServer()
    return server.getFilesSummary(dict)

  #############################################################################
  def getLimitedFiles(self, dict):
    """
    It returns a chunk of files. This method is equivalent to the getFiles.
    """
    server = self.__getServer()
    return server.getLimitedFiles(dict)

  #############################################################################
  def getAvailableDataQuality(self):
    """
    it returns all the available data quality flags.
    """
    server = self.__getServer()
    return server.getAvailableDataQuality()

  #############################################################################
  def getAvailableProductions(self):
    """
    It returns all the available productions which have associated file with replica flag yes.
    """
    server = self.__getServer()
    result = server.getAvailableProductions()
    return result

  #############################################################################
  def getAvailableRuns(self):
    """
    It returns all the available runs which have associated files with reploica flag yes.
    """
    server = self.__getServer()
    return server.getAvailableRuns()

  #############################################################################
  def getAvailableEventTypes(self):
    """
    It returns all the available event types.
    """
    server = self.__getServer()
    result = server.getAvailableEventTypes()
    return result

  ########################################REVIEW#####################################
  def getMoreProductionInformations(self, prodid):
    #DELETE !!!!!!!!!
    server = self.__getServer()
    result = server.getMoreProductionInformations(prodid)
    return result

  #############################################################################
  def getJobInformation(self, dict):
    """
    It returns the job metadata information for a given lfn produced by this job.
    """
    server = self.__getServer()
    result = server.getJobInformation(dict)
    return result

  #############################################################################
  def getJobInfo(self, lfn):
    """
    It returns the job metadata information for a given lfn produced by this job.
    """
    server = self.__getServer()
    result = server.getJobInfo(lfn)
    return result

  #############################################################################
  def getRunNumber(self, lfn):
    """
    It returns the run number for a given lfn!
    """
    server = self.__getServer()
    result = server.getRunNumber(lfn)
    return result

  #############################################################################
  def getProductionFiles(self, prod, fileType, replica = 'ALL'):
    """
    It returns files and their metadata for a given production, file type and replica.
    """
    server = self.__getServer()
    result = server.getProductionFiles(int(prod), fileType, replica)
    return result

  #############################################################################
  def getRunFiles(self, runid):
    """
    It returns all the files and their metadata for a given run number!
    """
    server = self.__getServer()
    result = server.getRunFiles(runid)
    return result

  #############################################################################
  def updateFileMetaData(self, filename, fileAttr):
    """
    This method used to modify files metadata.
    Input parametes is a stirng (filename) and a dictionary (fileAttr) with the file attributes. {'GUID':34826386286382,'EventStat':222222}
    """
    server = self.__getServer()
    return server.updateFileMetaData(filename, fileAttr)

  #############################################################################
  def renameFile(self, oldLFN, newLFN):
    """
    It allows to change the name of a file which is in the Bookkeeping Metadata Catalogue.
    """
    server = self.__getServer()
    return server.renameFile(oldLFN, newLFN)

  #############################################################################
  def insertTag(self, values):
    """
    It used to register tags (CONDB, DDDB, etc) to the database. The input parameter is dictionary: {'TagName':'Value'}
    """
    server = self.__getServer()
    result = server.insertTag(values)
    return result
  #############################################################################
  def setFileDataQuality(self, lfns, flag):
    """
    It is used to set the files data quality flags. The input parameters is an lfn or a list of lfns and the data quality flag.
    """
    if type(lfns) == types.StringType:
      lfns = [lfns]
    server = self.__getServer()
    result = server.setFileDataQuality(lfns, flag)
    return result

#############################################################################
  def getJobInputAndOutputJobFiles(self, jobids):
    """
    It returns the job input and output files for a given jobid.
    """
    server = self.__getServer()
    result = server.getJobInputAndOutputJobFiles(jobids)
    return result

  #############################################################################
  def setRunAndProcessingPassDataQuality(self, runNB, procpass, flag):
    """
    It sets the data quality to a run which belong to a given processing pass. This method insert a new row to the runquality table.
    This value used to set the data quality flag to a given run files which processed by a given processing pass.
    """
    server = self.__getServer()
    result = server.setRunAndProcessingPassDataQuality(long(runNB), procpass, flag)
    return result

  #############################################################################
  def setRunDataQuality(self, runNb, flag):
    """
    It sets the data quality for a given run! The input parameter is the run number and a data quality flag.
    """
    server = self.__getServer()
    result = server.setRunDataQuality(runNb, flag)
    return result

  #############################################################################
  def setProductionDataQuality(self, prod, flag):
    """
    It sets the data quality for a given production!
    """
    server = self.__getServer()
    result = server.setProductionDataQuality(prod, flag)
    return result


  #############################################################################
  def getFileAncestors(self, lfns, depth=0, replica=True):
    """
    It returns the ancestors of a file or a list of files. It also returns the metadata of the ancestor files.
    """
    server = self.__getServer()
    result = None
    if type(lfns) == types.StringType:
      result = server.getFileAncestors([lfns], depth, replica)
    else:
      result = server.getFileAncestors(lfns, depth, replica)
    return result

  #############################################################################
  def getFileDescendents(self, lfns, depth=0, production=0, checkreplica=False):
    """
    It returns the descendants of a file or a list of files.
    """
    server = self.__getServer()
    result = None
    if type(lfns) == types.StringType:
      result = server.getFileDescendents([lfns], depth, production, checkreplica)
    else:
      result = server.getFileDescendents(lfns, depth, production, checkreplica)
    return result

  #############################################################################
  def insertSimConditions(self, simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity, G4settings):
    """
    It inserts a simulation condition to the Bookkeeping Metadata catalogue.
    """
    server = self.__getServer()
    result = server.insertSimConditions( simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity, G4settings )
    return result

  #############################################################################
  def getSimConditions(self):
    """
    It returns all the simulation conditions which are in the Bookkeeping Metadata catalogue.
    """
    server = self.__getServer()
    result = server.getSimConditions()
    return result

  #############################################################################
  def addFiles(self, lfns):
    """
    It sets the replica flag Yes for a given list of files.
    """
    server = self.__getServer()
    return server.addFiles(lfns)

  #############################################################################
  def removeFiles(self, lfns):
    """
    It removes the replica flag for a given list of files.
    """
    server = self.__getServer()
    return server.removeFiles(lfns)

  #############################################################################
  def getFileMetadata(self, lfns):
    """
    It returns the metadata information for a given file or a list of files.
    """
    server = self.__getServer()
    return server.getFileMetadata(lfns)

  #############################################################################
  def getFileMetaDataForWeb(self, lfns):
    """
    This method only used by the web portal. It is same as getFileMetadata.
    """
    server = self.__getServer()
    result = server.getFileMetaDataForWeb(lfns)
    return result

  #############################################################################
  def getProductionFilesForWeb(self, prod, ftype, SortDict, StartItem, Maxitems):
    """
    It returns files and their metadata information for a given production.
    """
    server = self.__getServer()
    result = server.getProductionFilesForWeb(int(prod), ftype, SortDict, long(StartItem), long(Maxitems))
    return result

  #############################################################################
  def exists(self, lfns):
    """
    It used to check the existence of a list of files in the Bookkeeping Metadata catalogue.
    """
    server = self.__getServer()
    return server.exists(lfns)

  #############################################################################
  def getRunInformations(self, runnb):
    """
    It returns run information and statistics.
    """
    server = self.__getServer()
    result = server.getRunInformations(int(runnb))
    return result

  #############################################################################
  def getRunInformation(self, dict):
    """
    It returns run information and statistics.
    """
    server = self.__getServer()
    if 'Fields' not in dict:
      dict['Fields'] = ['ConfigName', 'ConfigVersion', 'JobStart', 'JobEnd', 'TCK', 'FillNumber', 'ProcessingPass', 'ConditionDescription', 'CONDDB', 'DDDB']
    if 'Statistics' in dict and len(dict['Statistics']) == 0:
      dict['Statistics']=['NbOfFiles','EventStat', 'FileSize', 'FullStat', 'Luminosity', 'InstLumonosity', 'EventType']

    result = server.getRunInformation(dict)
    return result

  #############################################################################
  def getFileCreationLog(self, lfn):
    """
    For a given file returns the log files of the job which created it.
    """
    server = self.__getServer()
    result = server.getFileCreationLog(lfn)
    return result

  #############################################################################
  def insertEventType(self, evid, desc, primary):
    """
    It inserts an event type to the Bookkeeping Metadata catalogue.
    """
    server = self.__getServer()
    return server.insertEventType(long(evid), desc, primary)

  #############################################################################
  def updateEventType(self, evid, desc, primary):
    """
    It can used to modify an existing event type.
    """
    server = self.__getServer()
    return server.updateEventType(long(evid), desc, primary)

  #############################################################################
  def getProductionSummary(self, dict):
    """
    It can used to count the number of events for a given dataset.
    """
    server = self.__getServer()
    result = server.getProductionSummary(dict)
    return result


  #############################################################################
  def getProductionInformations(self, prodid):
    """
    It returns a statistic (data processing phases, number of events, etc.) for a given production.
    """
    server = self.__getServer()
    result = server.getProductionInformations(long(prodid))
    return result

  #############################################################################
  def getProductionInformationsFromView(self, prodid):
    """
    It is exactly same as getProductionInformations, but it much faster. The result is in the materialized view.
    """
    server = self.__getServer()
    return server.getProductionInformationsFromView(long(prodid))

  #############################################################################
  def getFileHistory(self, lfn):
    """
    It returns all the information about a file.
    """
    server = self.__getServer()
    return server.getFileHistory(lfn)


  #############################################################################
  def getProductionNbOfJobs(self, prodid):
    """
    It returns the number of jobs for a given production.
    """
    server = self.__getServer()
    return server.getProductionNbOfJobs(long(prodid))

  #############################################################################
  def getProductionNbOfEvents(self, prodid):
    """
    It returns the number of events for a given production.
    """
    server = self.__getServer()
    return server.getNumberOfEvents(long(prodid))

  #############################################################################
  def getProductionSizeOfFiles(self, prodid):
    """
    It returns the size of files for a given production.
    """
    server = self.__getServer()
    return server.getProductionSizeOfFiles(long(prodid))

  #############################################################################
  def getProductionNbOfFiles(self, prodid):
    """
    It returns the number of files produced by a given production.
    """
    server = self.__getServer()
    return server.getProductionNbOfFiles(long(prodid))

  #############################################################################
  def getNbOfJobsBySites(self, prodid):
    """
    It returns the number of jobs executed at different sites for a given production.
    """
    server = self.__getServer()
    return server.getNbOfJobsBySites(long(prodid))

  #############################################################################
  def getAvailableTags(self):
    """
    It returns the available database tags.
    """
    server = self.__getServer()
    return server.getAvailableTags()

  #############################################################################
  def getProductionProcessedEvents(self, prodid):
    """
    it returns the number of events processed for a given production.
    """
    server = self.__getServer()
    return server.getProductionProcessedEvents(int(prodid))

  #############################################################################
  def getRunsForAGivenPeriod(self, dict):
    """
    It returns the available runs between a period.
    Input parameters:
    AllowOutsideRuns: If it is true, it only returns the runs which finished before EndDate.
    StartDate: the run start period
    EndDate: the run end period
    CheckRunStatus: if it is true, it check the run is processed or not processed.
    """
    server = self.__getServer()
    return server.getRunsForAGivenPeriod(dict)

  #############################################################################
  def getRuns(self, dict):
    """
    It returns the runs for a given configuration name and version.
    Input parameters:

    """
    server = self.__getServer()
    return server.getRuns(dict)

  #############################################################################
  def getProductionsFromView(self, dict):
    """
    It returns the productions from the bookkeeping view for a given processing pass and run number.
    Input parameters:
    RunNumber
    ProcessingPass
    """
    server = self.__getServer()
    return server.getProductionsFromView(dict)

  #############################################################################
  def getRunFilesDataQuality(self, runs):
    """
    It returns the data quality of files for set of runs.
    Input parameters:
    runs: list of run numbers.
    """
    if type(runs) in [types.StringType, types.LongType, types.IntType]:
      runs = [runs]
    server = self.__getServer()
    return server.getRunFilesDataQuality(runs)

  #############################################################################
  def setFilesInvisible(self, lfns):
    """
    It is used to set the file(s) invisible in the database
    Input parameter:
    lfns: an lfn or list of lfns
    """
    server = self.__getServer()
    result = None
    if type(lfns) == types.StringType:
      result = server.setFilesInvisible([lfns])
    else:
      result = server.setFilesInvisible(lfns)
    return result

  #############################################################################
  def setFilesVisible(self, lfns):
    """
    It is used to set the file(s) invisible in the database
    Input parameter:
    lfns: an lfn or list of lfns
    """
    server = self.__getServer()
    result = None
    if type(lfns) == types.StringType:
      result = server.setFilesVisible([lfns])
    else:
      result = server.setFilesVisible(lfns)
    return result

  #############################################################################
  def getRunAndProcessingPassDataQuality(self, runnb, processing):
    """
    It returns the data quality flag for a given run and processing pass.
    """
    server = self.__getServer()
    result = server.getRunAndProcessingPassDataQuality(long(runnb), long(processing))
    return result

  #############################################################################
  def getAvailableConfigurations(self):
    """
    It returns the available configurations.
    """
    server = self.__getServer()
    result = server.getAvailableConfigurations()
    return result

  #############################################################################
  def getProductionProcessingPass(self, prodid):
    """
    It returns the processing pass for a given production.
    """
    server = self.__getServer()
    return server.getProductionProcessingPass(long(prodid))

  #############################################################################
  def getRunProcessingPass(self, runnumber):
    """
    it returns the run number for a given run.
    """
    server = self.__getServer()
    return server.getRunProcessingPass(long(runnumber))

  ############################################################################
  def getProductionFilesStatus(self, productionid=None, lfns=[]):
    """
    It returns the file status in the bkk for a given production or a list of lfns.
    """
    server = self.__getServer()
    return server.getProductionFilesStatus(productionid, lfns)

  #############################################################################
  def getFilesWithGivenDataSets(self, values):
    """
    It returns a list of files for a given condition.
    """
    server = self.__getServer()
    return server.getFilesWithGivenDataSets(values)

  #############################################################################
  def getVisibleFilesWithMetadata(self, values):
    """
    It returns a list of files with metadata for a given condition.
    """
    server = self.__getServer()
    return server.getVisibleFilesWithMetadata(values)

  #############################################################################
  def addProduction(self, dict):
    """
    It is used to register a production to the bkk.
    Input parameters:
    SimulationConditions
    DataTakingConditions
    Steps: the step which is used to process data for a given production.
    Production:
    InputProductionTotalProcessingPass: it is a path of the input data processing pass
    """
    server = self.__getServer()
    return server.addProduction(dict)

  #############################################################################
  def getEventTypes(self, dict):
    """
    It returns the available event types for a given configuration name and configuration version.
    Input parameters:
    ConfigName, ConfigVersion, Production
    """
    server = self.__getServer()
    return server.getEventTypes(dict)

  #############################################################################
  def getProcessingPassSteps(self, dict):
    """
    It returns the steps for a given stepname processing pass ands production.
    """
    server = self.__getServer()
    return server.getProcessingPassSteps(dict)

  #############################################################################
  def getProductionProcessingPassSteps(self, dict):
    """
    it returns the steps for a given production.
    """
    server = self.__getServer()
    return server.getProductionProcessingPassSteps(dict)

  #############################################################################
  def getProductionOutputFileTypes(self, dict):
    """
    It returns the output file types which produced by a given production.
    """
    server = self.__getServer()
    return server.getProductionOutputFileTypes(dict)

  #############################################################################
  def getRunWithProcessingPassAndDataQuality(self, procpass, flag='ALL'):
    """
    It returns the run number for a given processing pass and a flag from the run quality table.
    """
    server = self.__getServer()
    return server.getRunWithProcessingPassAndDataQuality(procpass, flag)

  #############################################################################
  def getRunNbAndTck(self, lfn):
    """
    It returns the run number and tck for a given LFN.
    """
    server = self.__getServer()
    return server.getRunNbAndTck(lfn)

  #############################################################################
  def getRunAndProcessingPass(self, dict):
    """
    It returns all the processing pass and run number for a given run.
    """
    server = self.__getServer()
    return server.getRunAndProcessingPass(dict)

  #############################################################################
  def getNbOfRawFiles(self, dict):
    """
    It counts the raw files for a given run and (or) event type.
    """
    server = self.__getServer()
    return server.getNbOfRawFiles(dict)

  #############################################################################
  def getFileTypeVersion(self, lfn):
    """
    It returns the file type version of given lfns
    """
    server = self.__getServer()
    if type(lfn) == types.StringType:
      return server.getFileTypeVersion([lfn])
    else:
      return server.getFileTypeVersion(lfn)

  #############################################################################
  def getTCKs(self, dict):
    """
    It returns the tcks for a given data set.
    """
    server = self.__getServer()
    return server.getTCKs(dict)

  #############################################################################
  def getStepsMetadata(self, dict):
    """
    It returns the step(s) which is produced  a given dataset.
    """
    server = self.__getServer()
    return server.getStepsMetadata(dict)

  #############################################################################
  def getDirectoryMetadata(self, lfn):
    """
    It returns metadata informatiom for a given directory.
    """
    server = self.__getServer()
    return server.getDirectoryMetadata(lfn)

  #############################################################################
  def getFilesForGUID(self, guid):
    """
    It returns a file for a given GUID.
    """
    server = self.__getServer()
    return server.getFilesForGUID(guid)

  #############################################################################
  def getRunsGroupedByDataTaking(self):
    """
    It returns all the run numbers grouped by the data taking description.
    """
    server = self.__getServer()
    return server.getRunsGroupedByDataTaking()






  # The following method names are changed in the Bookkeeping client.

  #############################################################################
  def __errorReport(self, errMsg):

    s=  '                 WARNING                         \n'
    s+= '                                                 \n'
    s+= errMsg
    gLogger.error(s)

  #############################################################################
  def getProductionInformation(self, prodid):
    self.__errorReport("The 'getProductionInformation' method will be removed. If you would like to use it, please write a mail to LHCb bookkeeping <lhcb-bookkeeping@cern.ch>")
    server = self.__getServer()
    return server.getProductionInformation(long(prodid))

  #############################################################################
  def sendBookkeeping(self, name, xml):
    self.__errorReport("The 'sendBookkeeping' method is obsolete and it will be removed from the next LHCbDirac release. Please use the sendXMLBookkeepingReport!")
    return self.sendXMLBookkeepingReport(xml)

  #############################################################################
  def setQuality(self, lfns, flag):
    self.__errorReport("The 'setQuality' method is obsolete and it will be removed from the next LHCbDirac release. Please use the 'setFileDataQuality'!")
    return self.setFileDataQuality(lfns, flag)

  #############################################################################
  def getFilesSumary(self, dict):
    self.__errorReport("The 'getFilesSumary' method is obsolete and it will be removed from the next release. Please use the getFilesSummary!")
    return self.getFilesSummary(dict)

  #############################################################################
  def getAvailableRunNumbers(self):
    self.__errorReport("The 'getAvailableRunNumbers' method is obsolete and it will be removed from the next release. Please use the 'getAvailableRuns'!")
    return self.getAvailableRuns()

  #############################################################################
  def getInputAndOutputJobFiles(self, jobids):
    self.__errorReport("The 'getInputAndOutputJobFiles' method is obsolete and it will be removed from the next release. Please use the 'getJobInputAndOutputJobFiles'!")
    return self.getJobInputAndOutputJobFiles(jobids)

  #############################################################################
  def setRunQualityWithProcessing(self, runNB, procpass, flag):
    self.__errorReport("The 'setRunQualityWithProcessing' method is obsolete and it will be removed from the next release. Please use the 'setRunAndProcessingPassDataQuality'!")
    return self.setRunAndProcessingPassDataQuality(runNB, procpass, flag)

  #############################################################################
  def setQualityRun(self, runNb, flag):
    self.__errorReport("The 'setQualityRun' method is obsolete and it will be removed from the next release. Please use the 'setRunDataQuality'!")
    return self.setRunDataQuality(runNb, flag)

  #############################################################################
  def setQualityProduction(self, prod, flag):
    self.__errorReport("The 'setQualityProduction' method is obsolete and it will be removed from the next release. Please use the 'setProductionDataQuality'!")
    return self.setProductionDataQuality(prod, flag)

  #############################################################################
  def getLFNsByProduction(self, prodid):
    self.__errorReport("The 'getLFNsByProduction' method is obsolete and it will be removed from the next release. Please use the 'getProductionFiles'!")
    return self.getProductionFiles(prodid, 'ALL','ALL')

  #############################################################################
  def getAncestors(self, lfns, depth=1):
    self.__errorReport("The 'getAncestors' method is obsolete and it will be removed from the next release. Please use the 'getFileAncestors'!")
    server = self.__getServer()
    result = None
    if type(lfns) == types.StringType:
      retVal = server.getFileAncestors([lfns], depth, True)
    else:
      retVal = server.getFileAncestors(lfns, depth, True)
    if retVal['OK']:
      values = retVal['Value']
      for key, value in values['Successful'].items():
        values['Successful'][key] = [ i['FileName'] for i in value]
      result = S_OK(values)
    else:
      result = retVal

    return result

  #############################################################################
  def getAllAncestorsWithFileMetaData(self, lfns, depth=1):
    self.__errorReport("The 'getAllAncestorsWithFileMetaData' method is obsolete and it will be removed from the next release. Please use the 'getFileAncestors'!")
    server = self.__getServer()
    result = None
    if type(lfns) == types.StringType:
      result = server.getFileAncestors([lfns], depth, False)
    else:
      result = server.getFileAncestors(lfns, depth, False)
    return result

  #############################################################################
  def getAllAncestors(self, lfns, depth=1):
    self.__errorReport("The 'getAllAncestors' method is obsolete and it will be removed from the next release. Please use the 'getFileAncestors'!")
    server = self.__getServer()
    result = None
    if type(lfns) == types.StringType:
      retVal = server.getFileAncestors([lfns], depth, False)
    else:
      retVal = server.getFileAncestors(lfns, depth, False)
    if retVal['OK']:
      values = retVal['Value']
      for key, value in values['Successful'].items():
        values['Successful'][key] = [ i['FileName'] for i in value]
      result = S_OK(values)
    else:
      result = retVal

    return result

  #############################################################################
  def getDescendents(self, lfns, depth=0):
    self.__errorReport("The 'getDescendents' method is obsolete and it will be removed from the next release. Please use the 'getFileDescendents'!")
    server = self.__getServer()
    result = None
    if type(lfns) == types.StringType:
      result = server.getFileDescendents([lfns], depth, 0, True)
    else:
      result = server.getFileDescendents(lfns, depth, 0, True)
    return result

  #############################################################################
  def getAllDescendents(self, lfns, depth=0, production=0, checkreplica=False):
    self.__errorReport("The 'getAllDescendents' method is obsolete and it will be removed from the next release. Please use the 'getFileDescendents'!")
    server = self.__getServer()
    result = None
    if type(lfns) == types.StringType:
      result = server.getFileDescendents([lfns], depth, production, checkreplica)
    else:
      result = server.getFileDescendents(lfns, depth, production, checkreplica)
    return result

  #############################################################################
  def checkfile(self, fileName):
    self.__errorReport("The 'checkfile' method will be removed. If you would like to use it, please write a mail to LHCb bookkeeping <lhcb-bookkeeping@cern.ch>")
    server = self.__getServer()
    result = server.checkfile(fileName)
    return result

  #############################################################################
  def checkFileTypeAndVersion(self, type, version):
    self.__errorReport("The 'checkFileTypeAndVersion' method will be removed. If you would like to use it, please write a mail to LHCb bookkeeping <lhcb-bookkeeping@cern.ch>")
    server = self.__getServer()
    result = server.checkFileTypeAndVersion(type, version)
    return result

  #############################################################################
  def checkEventType(self, eventTypeId):
    self.__errorReport("The 'checkEventType' method will be removed. If you would like to use it, please write a mail to LHCb bookkeeping <lhcb-bookkeeping@cern.ch>")
    server = self.__getServer()
    result = server.checkEventType(long(eventTypeId))
    return result

  #############################################################################
  def removeReplica(self, fileName):
    self.__errorReport("The 'removeReplica' method will be removed. If you would like to use it, please write a mail to LHCb bookkeeping <lhcb-bookkeeping@cern.ch>")
    server = self.__getServer()
    return server.removeReplica(fileName)

  #############################################################################
  def addReplica(self, fileName):
    self.__errorReport("The 'addReplica' method will be removed. If you would like to use it, please write a mail to LHCb bookkeeping <lhcb-bookkeeping@cern.ch>")
    server = self.__getServer()
    return server.addReplica(fileName)

  #############################################################################
  def getFilesInformations(self, lfns):
    self.__errorReport("The 'getFilesInformations' method is obsolete and it will be removed from the next release. Please use the 'getFileMetadata'!")
    server = self.__getServer()
    return server.getFileMetadata(lfns)

  #############################################################################
  def getFileMetaDataForUsers(self, lfns):
    self.__errorReport("The 'getFileMetaDataForUsers' method is obsolete and it will be removed from the next release. Please use the 'getFileMetaDataForWeb'!")
    server = self.__getServer()
    result = server.getFileMetaDataForWeb(lfns)
    return result

  ############################################################################
  def getProductionFilesForUsers(self, prod, ftype, SortDict, StartItem, Maxitems):
    self.__errorReport("The 'getProductionFilesForUsers' method is obsolete and it will be removed from the next release. Please use the 'getProductionFilesForWeb'!")
    server = self.__getServer()
    result = server.getProductionFilesForWeb(int(prod), ftype, SortDict, long(StartItem), long(Maxitems))
    return result

  #############################################################################
  def getLogfile(self, lfn):
    self.__errorReport("The 'getLogfile' method is obsolete and it will be removed from the next release. Please use the 'getFileCreationLog'!")
    server = self.__getServer()
    result = server.getFileCreationLog(lfn)
    return result

  #############################################################################
  def addEventType(self, evid, desc, primary):
    self.__errorReport("The 'addEventType' method is obsolete and it will be removed from the next release. Please use the 'insertEventType'!")
    server = self.__getServer()
    return server.insertEventType(long(evid), desc, primary)

  #############################################################################
  def getProductionInformations_new(self, prodid):
    self.__errorReport("The 'getProductionInformations_new' method is obsolete and it will be removed from the next release. Please use the 'getProductionInformations'!")
    server = self.__getServer()
    result = server.getProductionInformations(long(prodid))
    return result

  #############################################################################
  def getJobsNb(self, prodid):
    self.__errorReport("The 'getJobsNb' method is obsolete and it will be removed from the next release. Please use the 'getProductionNbOfJobs'!")
    server = self.__getServer()
    return server.getProductionNbOfJobs(long(prodid))

  #############################################################################
  def getNumberOfEvents(self, prodid):
    self.__errorReport("The 'getNumberOfEvents' method is obsolete and it will be removed from the next release. Please use the 'getNumberOfEvents'!")
    server = self.__getServer()
    return server.getProductionNbOfEvents(long(prodid))

  #############################################################################
  def getSizeOfFiles(self, prodid):
    self.__errorReport("The 'getSizeOfFiles' method is obsolete and it will be removed from the next release. Please use the 'getProductionSizeOfFiles'!")
    server = self.__getServer()
    return server.getProductionSizeOfFiles(long(prodid))

  #############################################################################
  def getNbOfFiles(self, prodid):
    self.__errorReport("The 'getNbOfFiles' method is obsolete and it will be removed from the next release. Please use the 'getProductionNbOfFiles'!")
    server = self.__getServer()
    return server.getProductionNbOfFiles(long(prodid))

  #############################################################################
  def getProcessedEvents(self, prodid):
    self.__errorReport("The 'getProcessedEvents' method is obsolete and it will be removed from the next release. Please use the 'getProductionProcessedEvents'!")
    server = self.__getServer()
    return server.getProductionProcessedEvents(int(prodid))

  #############################################################################
  def getRunsWithAGivenDates(self, dict):
    self.__errorReport("The 'getRunsWithAGivenDates' method is obsolete and it will be removed from the next release. Please use the 'getRunsForAGivenPeriod'!")
    server = self.__getServer()
    return server.getRunsForAGivenPeriod(dict)

  #############################################################################
  def getProductiosWithAGivenRunAndProcessing(self, dict):
    self.__errorReport("The 'getProductiosWithAGivenRunAndProcessing' method is obsolete and it will be removed from the next release. Please use the 'getProductionsFromView'!")
    server = self.__getServer()
    return server.getProductionsFromView(dict)

  #############################################################################
  def getDataQualityForRuns(self, runs):
    self.__errorReport("The 'getDataQualityForRuns' method is obsolete and it will be removed from the next release. Please use the 'getRunFilesDataQuality'!")
    server = self.__getServer()
    return server.getRunFilesDataQuality(runs)

  #############################################################################
  def getRunFlag(self, runnb, processing):
    self.__errorReport("The 'getRunFlag' method is obsolete and it will be removed from the next release. Please use the 'getRunAndProcessingPassDataQuality'!")
    server = self.__getServer()
    result = server.getRunAndProcessingPassDataQuality(long(runnb), long(processing))
    return result

  #############################################################################
  def getProductionProcessingPassID(self, prodid):
    self.__errorReport("The 'getProductionProcessingPassID' method will be removed. If you would like to use it, please write a mail to LHCb bookkeeping <lhcb-bookkeeping@cern.ch>")
    server = self.__getServer()
    return server.getProductionProcessingPassID(long(prodid))

  ############################################################################
  def getProductionStatus(self, productionid=None, lfns=[]):
    self.__errorReport("The 'getProductionStatus' method is obsolete and it will be removed from the next release. Please use the 'getProductionFilesStatus'!")
    server = self.__getServer()
    result = None
    if productionid != None:
      result = server.getProductionFilesStatus(productionid, None)
    else:
      result = server.getProductionFilesStatus(None, lfns)
    return result

  #############################################################################
  def getFiles(self, dict):
    self.__errorReport("The 'getFiles' method is obsolete and it will be removed from the next release. Please use the 'getFilesWithMetadata'!")
    return self.getFilesWithMetadata(dict)

  #############################################################################
  def getFilesWithGivenDataSetsForUsers(self, values):
    self.__errorReport("The 'getFilesWithGivenDataSetsForUsers' method is obsolete and it will be removed from the next release. Please use the 'getVisibleFilesWithMetadata'!")
    server = self.__getServer()
    return server.getVisibleFilesWithMetadata(values)

  #############################################################################
  def getStandardEventTypes(self, dict):
    self.__errorReport("The 'getStandardEventTypes' method is obsolete and it will be removed from the next release. Please use the 'getEventTypes'!")
    server = self.__getServer()
    return server.getEventTypes(dict)

   #############################################################################
  def getProductionOutputFiles(self, dict):
    self.__errorReport("The 'getProductionOutputFiles' method is obsolete and it will be removed from the next release. Please use the 'getProductionOutputFileTypes'!")
    server = self.__getServer()
    return server.getProductionOutputFileTypes(dict)

  #############################################################################
  def getRunQuality(self, procpass, flag='ALL'):
    self.__errorReport("The 'getRunQuality' method is obsolete and it will be removed from the next release. Please use the 'getRunWithProcessingPassAndDataQuality'!")
    server = self.__getServer()
    return server.getRunWithProcessingPassAndDataQuality(procpass, flag)

  #############################################################################
  def getRunProcPass(self, dict):
    self.__errorReport("The 'getRunProcPass' method is obsolete and it will be removed from the next release. Please use the 'getRunAndProcessingPass'!")
    server = self.__getServer()
    return server.getRunAndProcessingPass(dict)

  #############################################################################
  def getRunNbFiles(self, dict):
    self.__errorReport("The 'getRunNbFiles' method is obsolete and it will be removed from the next release. Please use the 'getNbOfRawFiles'!")
    server = self.__getServer()
    return server.getNbOfRawFiles(dict)

  #############################################################################
  def getTypeVersion(self, lfn):
    self.__errorReport("The 'getTypeVersion' method is obsolete and it will be removed from the next release. Please use the 'getFileTypeVersion'!")
    server = self.__getServer()
    if type(lfn) == types.StringType:
      return server.getFileTypeVersion([lfn])
    else:
      return server.getFileTypeVersion(lfn)

   #############################################################################
  def getAvailableTcks(self, dict):
    self.__errorReport("The 'getAvailableTcks' method is obsolete and it will be removed from the next release. Please use the 'getTCKs'!")
    server = self.__getServer()
    return server.getTCKs(dict)


