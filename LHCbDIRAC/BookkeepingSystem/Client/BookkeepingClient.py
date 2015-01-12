########################################################################
# $Id$
########################################################################

"""
in_dict = {'EventTypeId': 93000000,
        'ConfigVersion': 'Collision10',
        'ProcessingPass': '/Real Data',
        'ConfigName': 'LHCb',
        'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
        'Production':7421
         }
"""
from DIRAC                           import S_OK, S_ERROR, gLogger
from DIRAC.Core.DISET.RPCClient      import RPCClient
from DIRAC.Core.DISET.TransferClient import TransferClient
import types, cPickle, tempfile


__RCSID__ = "$Id$"

class BookkeepingClient:
  """ This class expose the methods of the Bookkeeping Service"""

  def __init__( self, rpcClient = None ):
    self.rpcClient = rpcClient

  def __getServer( self, timeout = 3600 ):
    """It returns the access protocol to the Bookkeeping service"""
    if self.rpcClient:
      return self.rpcClient
    else:
      return RPCClient( 'Bookkeeping/BookkeepingManager', timeout = timeout )

  #############################################################################
  def echo( self, string ):
    """It print the string"""
    server = self.__getServer()
    res = server.echo( string )
    print res

  #############################################################################
  def sendXMLBookkeepingReport( self, xml ):
    """
    This method is used to upload an xml report which is produced after when the job successfully finished.
    The input parameter 'xml' is a string which contains various information (metadata) about the finished job in the Grid in an XML format.
    """
    server = self.__getServer()
    return server.sendXMLBookkeepingReport( xml )


  #############################################################################
  def getAvailableSteps( self, in_dict ):
    """
    It returns all the available steps which corresponds to a given conditions. The in_dict contains the following conditions: StartDate, StepId, InputFileTypes, OutputFileTypes,
    ApplicationName, ApplicationVersion, OptionFiles, DDDB, CONDDB, ExtraPackages, Visible, ProcessingPass, Usable, RuntimeProjects, DQTag, OptionsFormat, StartItem, MaxItem
    """
    server = self.__getServer()
    return server.getAvailableSteps( in_dict )

  #############################################################################
  def getRuntimeProjects( self, in_dict ):
    """
    It returns a runtime project for a given step. The input parameter is a in_dictionary which has only one key StepId
    """
    server = self.__getServer()
    return server.getRuntimeProjects( in_dict )

  #############################################################################
  def getAvailableFileTypes( self ):
    """
    It returns all the available files which are registered to the bkk.
    """
    server = self.__getServer()
    retVal = server.getAvailableFileTypes()
    if retVal['OK']:
      records = []
      parameters = ["FileType", "Description"]
      for record in retVal['Value']:
        records += [list( record )]
      return S_OK( {'ParameterNames':parameters, 'Records':records, 'TotalRecords':len( records )} )
    else:
      return retVal


  #############################################################################
  def insertFileTypes( self, ftype, desc, fileType ):
    """
    It is used to register a file type. It has the following input parameters:
      -ftype: file type; for example: COOL.DST
      -desc: a short description which describes the file content
      -fileType: the file format such as ROOT, POOL_ROOT, etc.
    """
    server = self.__getServer()
    return server.insertFileTypes( ftype, desc, fileType )

  #############################################################################
  def insertStep( self, in_dict ):
    """
    It used to insert a step to the Bookkeeping Metadata Catalogue. The imput parameter is a dictionary which contains the steps attributes.
    For example: Dictionary format: {'Step': {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': '', 'ApplicationVersion': 'v29r1', 'ext-comp-1273': 'CHARM.MDST (Charm micro dst)', 'ExtraPackages': '', 'StepName': 'davinci prb2', 'ProcessingPass': 'WG-Coool', 'ext-comp-1264': 'CHARM.DST (Charm stream)', 'Visible': 'Y', 'DDDB': '', 'OptionFiles': '', 'CONDDB': ''}, 'OutputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.MDST'}], 'InputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.DST'}],'RuntimeProjects':[{StepId:13878}]}
    """
    server = self.__getServer()
    return server.insertStep( in_dict )

  #############################################################################
  def deleteStep( self, stepid ):
    """
    It used to delete a given step.
    """
    server = self.__getServer()
    return server.deleteStep( int( stepid ) )

  #############################################################################
  def updateStep( self, in_dict ):
    """
    It is used to modify the step attributes.
    """
    server = self.__getServer()
    return server.updateStep( in_dict )

  #############################################################################
  def getStepInputFiles( self, stepId ):
    """
    It returns the input files for a given step.
    """
    server = self.__getServer()
    return server.getStepInputFiles( int( stepId ) )

  #############################################################################
  def setStepInputFiles( self, stepid, files ):
    """
     It is used to set input file types to a Step.
    """
    server = self.__getServer()
    return server.setStepInputFiles( stepid, files )

  #############################################################################
  def setStepOutputFiles( self, stepid, files ):
    """
    It is used to set output file types to a Step.
    """
    server = self.__getServer()
    return server.setStepOutputFiles( stepid, files )

  #############################################################################
  def getStepOutputFiles( self, stepId ):
    """
    It returns the output file types for a given Step.
    """
    server = self.__getServer()
    return server.getStepOutputFiles( int( stepId ) )

  #############################################################################
  def getAvailableConfigNames( self ):
    """
    It returns all the available configuration names which are used.
    """
    server = self.__getServer()
    return server.getAvailableConfigNames()

  #############################################################################
  def getConfigVersions( self, in_dict ):
    """
    It returns all the available configuration version for a given condition.
    Input parameter is a dictionary which has the following key: 'ConfigName'
    For example: in_dict = {'ConfigName':'MC'}
    """
    server = self.__getServer()
    return server.getConfigVersions( in_dict )

  #############################################################################
  def getConditions( self, in_dict ):
    """
    It returns all the available conditions for a given conditions.
    Input parameter is a dictionary which has the following keys: 'ConfigName', 'ConfigVersion', 'EventType'
    For example: in_dict = {'ConfigName':'MC','ConfigVersion':'MC10'}
    """
    server = self.__getServer()
    return server.getConditions( in_dict )

  #############################################################################
  def getProcessingPass( self, in_dict, path = '/' ):
    """
    It returns the processing pass for a given conditions.
    Input parameter is a dictionary and a path (string) which has the following keys: 'ConfigName', 'ConfigVersion', 'ConditionDescription','Production', 'RunNumber', 'EventType'
    This method is used to recursively browse the processing pass. To start the browsing you have to define the path as a root: path = '/'
    Note: it returns a list with two dictionary. First dictionary contains the processing passes while the second dictionary contains the event types.
    """
    server = self.__getServer()
    return server.getProcessingPass( in_dict, path )

  #############################################################################
  def getProcessingPassId( self, fullpath ):
    """
    It returns the ProcessingPassId for a given path. this method should not used!
    """
    server = self.__getServer()
    return server.getProcessingPassId( fullpath )

  #############################################################################
  def getProductions( self, in_dict ):
    """
    It returns the productions for a given conditions.
    Input parameter is a dictionary which has the following keys: 'ConfigName', 'ConfigVersion', 'ConditionDescription', 'EventType','ProcessingPass'
    """
    server = self.__getServer()
    return server.getProductions( in_dict )

  #############################################################################
  def getFileTypes( self, in_dict ):
    """
    It returns the file types for a given conditions.
    Input parameter is a dictionary which has the following keys: 'ConfigName', 'ConfigVersion', 'ConditionDescription', 'EventType','ProcessingPass','Production','RunNumber'
    """
    server = self.__getServer()
    return server.getFileTypes( in_dict )

  #############################################################################
  @staticmethod
  def getFilesWithMetadata( in_dict ):
    """
    It returns the files for a given conditions.
    Input parameter is a dictionary which has the following keys: 'ConfigName',
    'ConfigVersion', 'ConditionDescription', 'EventType',
    'ProcessingPass','Production','RunNumber', 'FileType', DataQuality, StartDate, EndDate
    """
    in_dict = dict( in_dict )
    bkk = TransferClient( 'Bookkeeping/BookkeepingManager' )
    savedbuffer = cPickle.dumps( in_dict )
    file_name = tempfile.NamedTemporaryFile()
    params = str( savedbuffer )
    retVal = bkk.receiveFile( file_name.name, params )
    if not retVal['OK']:
      return retVal
    else:
      value = cPickle.load( open( file_name.name ) )
      file_name.close()
      return S_OK( value )


  #############################################################################
  def getFilesSummary( self, in_dict ):
    """
    It returns sumary for a given data set.
    Input parameter is a dictionary which has the following keys: 'ConfigName', 'ConfigVersion', 'ConditionDescription', 'EventType','ProcessingPass','Production','RunNumber', 'FileType', DataQuality
    """
    server = self.__getServer()
    return server.getFilesSummary( in_dict )

  #############################################################################
  def getLimitedFiles( self, in_dict ):
    """
    It returns a chunk of files. This method is equivalent to the getFiles.
    """
    server = self.__getServer()
    return server.getLimitedFiles( in_dict )

  #############################################################################
  def getAvailableDataQuality( self ):
    """
    it returns all the available data quality flags.
    """
    server = self.__getServer()
    return server.getAvailableDataQuality()

  #############################################################################
  def getAvailableProductions( self ):
    """
    It returns all the available productions which have associated file with replica flag yes.
    """
    server = self.__getServer()
    return server.getAvailableProductions()


  #############################################################################
  def getAvailableRuns( self ):
    """
    It returns all the available runs which have associated files with reploica flag yes.
    """
    server = self.__getServer()
    return server.getAvailableRuns()

  #############################################################################
  def getAvailableEventTypes( self ):
    """
    It returns all the available event types.
    """
    server = self.__getServer()
    return server.getAvailableEventTypes()


  ########################################REVIEW#####################################
  def getMoreProductionInformations( self, prodid ):
    """It returns inforation about a production"""
    #DELETE !!!!!!!!!
    server = self.__getServer()
    return server.getMoreProductionInformations( prodid )


  #############################################################################
  def getJobInformation( self, in_dict ):
    """
    It returns the job metadata information for a given lfn produced by this job.
    """
    server = self.__getServer()
    return server.getJobInformation( in_dict )


  #############################################################################
  def getJobInfo( self, lfn ):
    """
    It returns the job metadata information for a given lfn produced by this job.
    """
    server = self.__getServer()
    return server.getJobInfo( lfn )


  #############################################################################
  def bulkJobInfo( self, in_dict ):
    """
    It returns the job metadata information for a given condition:
    -a list of lfns
    - a list of DIRAC job ids
    - a list of jobNames
    in_dict = {'lfn':[],jobId:[],jobName:[]}

    """
    conditions = {}
    if type( in_dict ) == types.StringType:
      conditions['lfn'] = [in_dict]
    elif type ( in_dict ) == types.ListType:
      conditions['lfn'] = in_dict
    else:
      conditions = in_dict

    server = self.__getServer()
    return server.bulkJobInfo( conditions )


  #############################################################################
  def getRunNumber( self, lfn ):
    """
    It returns the run number for a given lfn!
    """
    server = self.__getServer()
    return server.getRunNumber( lfn )


  #############################################################################
  def getProductionFiles( self, prod, fileType, replica = 'ALL' ):
    """
    It returns files and their metadata for a given production, file type and replica.
    """
    server = self.__getServer()
    return server.getProductionFiles( int( prod ), fileType, replica )


  #############################################################################
  def getRunFiles( self, runid ):
    """
    It returns all the files and their metadata for a given run number!
    """
    server = self.__getServer()
    return server.getRunFiles( runid )


  #############################################################################
  def updateFileMetaData( self, filename, fileAttr ):
    """
    This method used to modify files metadata.
    Input parametes is a stirng (filename) and a dictionary (fileAttr) with the file attributes. {'GUID':34826386286382,'EventStat':222222}
    """
    server = self.__getServer()
    return server.updateFileMetaData( filename, fileAttr )

  #############################################################################
  def renameFile( self, oldLFN, newLFN ):
    """
    It allows to change the name of a file which is in the Bookkeeping Metadata Catalogue.
    """
    server = self.__getServer()
    return server.renameFile( oldLFN, newLFN )

  #############################################################################
  def insertTag( self, values ):
    """
    It used to register tags (CONDB, DDDB, etc) to the database. The input parameter is dictionary: {'TagName':'Value'}
    """
    server = self.__getServer()
    return server.insertTag( values )

  #############################################################################
  def setFileDataQuality( self, lfns, flag ):
    """
    It is used to set the files data quality flags. The input parameters is an lfn or a list of lfns and the data quality flag.
    """
    if type( lfns ) == types.StringType:
      lfns = [lfns]
    server = self.__getServer()
    return server.setFileDataQuality( lfns, flag )


#############################################################################
  def setRunAndProcessingPassDataQuality( self, runNB, procpass, flag ):
    """
    It sets the data quality to a run which belong to a given processing pass. This method insert a new row to the runquality table.
    This value used to set the data quality flag to a given run files which processed by a given processing pass.
    """
    server = self.__getServer()
    return server.setRunAndProcessingPassDataQuality( long( runNB ), procpass, flag )


  #############################################################################
  def setRunDataQuality( self, runNb, flag ):
    """
    It sets the data quality for a given run! The input parameter is the run number and a data quality flag.
    """
    server = self.__getServer()
    return server.setRunDataQuality( runNb, flag )


  #############################################################################
  def setProductionDataQuality( self, prod, flag ):
    """
    It sets the data quality for a given production!
    """
    server = self.__getServer()
    return server.setProductionDataQuality( prod, flag )



  #############################################################################
  def getFileAncestors( self, lfns, depth = 0, replica = True ):
    """
    It returns the ancestors of a file or a list of files. It also returns the metadata of the ancestor files.
    """
    server = self.__getServer()

    if type( lfns ) == types.StringType:
      lfns = [lfns]
    return server.getFileAncestors( lfns, depth, replica )


  #############################################################################
  def getFileDescendants( self, lfns, depth = 0, production = 0, checkreplica = False ):
    """
    It returns the descendants of a file or a list of files.
    """
    server = self.__getServer()

    if type( lfns ) == types.StringType:
      lfns = [lfns]
    return server.getFileDescendants( lfns, depth, production, checkreplica )


  #############################################################################
  def getFileDescendents( self, lfns, depth = 0, production = 0, checkreplica = False ):
    """
    It returns the descendants of a file or a list of files.
    """
    return self.getFileDescendants( lfns, depth, production, checkreplica )

  #############################################################################
  def insertSimConditions( self, in_dict ):
    """
    It inserts a simulation condition to the Bookkeeping Metadata catalogue.
    """
    server = self.__getServer()
    return server.insertSimConditions( in_dict )


  #############################################################################
  def getSimConditions( self ):
    """
    It returns all the simulation conditions which are in the Bookkeeping Metadata catalogue.
    """
    server = self.__getServer()
    return server.getSimConditions()


  #############################################################################
  def addFiles( self, lfns ):
    """
    It sets the replica flag Yes for a given list of files.
    """
    server = self.__getServer()
    if type( lfns ) == types.StringType:
      lfns = [lfns]
    return server.addFiles( lfns )

  #############################################################################
  def removeFiles( self, lfns ):
    """
    It removes the replica flag for a given list of files.
    """
    server = self.__getServer()
    if type( lfns ) == types.StringType:
      lfns = [lfns]
    return server.removeFiles( lfns )

  #############################################################################
  def getFileMetadata( self, lfns ):
    """
    It returns the metadata information for a given file or a list of files.
    """
    if type( lfns ) == types.StringType:
      lfns = [lfns]
    server = self.__getServer()
    return server.getFileMetadata( lfns )

  #############################################################################
  def getFileMetaDataForWeb( self, lfns ):
    """
    This method only used by the web portal. It is same as getFileMetadata.
    """
    server = self.__getServer()
    if type( lfns ) == types.StringType:
      lfns = [lfns]
    return server.getFileMetaDataForWeb( lfns )


  #############################################################################
  def getProductionFilesForWeb( self, prod, ftype, sortDict, startItem, maxitems ):
    """
    It returns files and their metadata information for a given production.
    """
    server = self.__getServer()
    return server.getProductionFilesForWeb( int( prod ), ftype, sortDict, long( startItem ), long( maxitems ) )


  #############################################################################
  def exists( self, lfns ):
    """
    It used to check the existence of a list of files in the Bookkeeping Metadata catalogue.
    """
    server = self.__getServer()
    if type( lfns ) == types.StringType:
      lfns = [lfns]
    return server.exists( lfns )

  #############################################################################
  def getRunInformations( self, runnb ):
    """
    It returns run information and statistics.
    """
    server = self.__getServer()
    return server.getRunInformations( int( runnb ) )


  #############################################################################
  def getRunInformation( self, in_dict ):
    """
    It returns run information and statistics.
    """
    server = self.__getServer()
    if 'Fields' not in in_dict:
      in_dict['Fields'] = ['ConfigName', 'ConfigVersion', 'JobStart', 'JobEnd', 'TCK',
                           'FillNumber', 'ProcessingPass', 'ConditionDescription', 'CONDDB', 'DDDB']
    if 'Statistics' in in_dict and len( in_dict['Statistics'] ) == 0:
      in_dict['Statistics'] = ['NbOfFiles', 'EventStat', 'FileSize', 'FullStat',
                             'Luminosity', 'InstLumonosity', 'EventType']

    return server.getRunInformation( in_dict )


  #############################################################################
  def getFileCreationLog( self, lfn ):
    """
    For a given file returns the log files of the job which created it.
    """
    server = self.__getServer()
    return server.getFileCreationLog( lfn )


  #############################################################################
  def insertEventType( self, evid, desc, primary ):
    """
    It inserts an event type to the Bookkeeping Metadata catalogue.
    """
    server = self.__getServer()
    return server.insertEventType( long( evid ), desc, primary )

  #############################################################################
  def updateEventType( self, evid, desc, primary ):
    """
    It can used to modify an existing event type.
    """
    server = self.__getServer()
    return server.updateEventType( long( evid ), desc, primary )

  #############################################################################
  def getProductionSummary( self, in_dict ):
    """
    It can used to count the number of events for a given dataset.
    """
    server = self.__getServer()
    return server.getProductionSummary( in_dict )



  #############################################################################
  def getProductionInformations( self, prodid ):
    """
    It returns a statistic (data processing phases, number of events, etc.) for a given production.
    """
    server = self.__getServer()
    return server.getProductionInformations( long( prodid ) )


  #############################################################################
  def getProductionInformationsFromView( self, prodid ):
    """
    It is exactly same as getProductionInformations, but it much faster. The result is in the materialized view.
    """
    server = self.__getServer()
    return server.getProductionInformationsFromView( long( prodid ) )

  #############################################################################
  def getFileHistory( self, lfn ):
    """
    It returns all the information about a file.
    """
    server = self.__getServer()
    return server.getFileHistory( lfn )


  #############################################################################
  def getProductionNbOfJobs( self, prodid ):
    """
    It returns the number of jobs for a given production.
    """
    server = self.__getServer()
    return server.getProductionNbOfJobs( long( prodid ) )

  #############################################################################
  def getProductionNbOfEvents( self, prodid ):
    """
    It returns the number of events for a given production.
    """
    server = self.__getServer()
    return server.getNumberOfEvents( long( prodid ) )

  #############################################################################
  def getProductionSizeOfFiles( self, prodid ):
    """
    It returns the size of files for a given production.
    """
    server = self.__getServer()
    return server.getProductionSizeOfFiles( long( prodid ) )

  #############################################################################
  def getProductionNbOfFiles( self, prodid ):
    """
    It returns the number of files produced by a given production.
    """
    server = self.__getServer()
    return server.getProductionNbOfFiles( long( prodid ) )

  #############################################################################
  def getNbOfJobsBySites( self, prodid ):
    """
    It returns the number of jobs executed at different sites for a given production.
    """
    server = self.__getServer()
    return server.getNbOfJobsBySites( long( prodid ) )

  #############################################################################
  def getAvailableTags( self ):
    """
    It returns the available database tags.
    """
    server = self.__getServer()
    return server.getAvailableTags()

  #############################################################################
  def getProductionProcessedEvents( self, prodid ):
    """
    it returns the number of events processed for a given production.
    """
    server = self.__getServer()
    return server.getProductionProcessedEvents( int( prodid ) )

  #############################################################################
  def getRunsForAGivenPeriod( self, in_dict ):
    """
    It returns the available runs between a period.
    Input parameters:
    AllowOutsideRuns: If it is true, it only returns the runs which finished before EndDate.
    StartDate: the run start period
    EndDate: the run end period
    CheckRunStatus: if it is true, it check the run is processed or not processed.
    """
    server = self.__getServer()
    return server.getRunsForAGivenPeriod( in_dict )

  #############################################################################
  def getRuns( self, in_dict ):
    """
    It returns the runs for a given configuration name and version.
    Input parameters:

    """
    server = self.__getServer()
    return server.getRuns( in_dict )

  #############################################################################
  def getProductionsFromView( self, in_dict ):
    """
    It returns the productions from the bookkeeping view for a given processing pass and run number.
    Input parameters:
    RunNumber
    ProcessingPass
    """
    server = self.__getServer()
    return server.getProductionsFromView( in_dict )

  #############################################################################
  def getRunFilesDataQuality( self, runs ):
    """
    It returns the data quality of files for set of runs.
    Input parameters:
    runs: list of run numbers.
    """
    if type( runs ) in [types.StringType, types.LongType, types.IntType]:
      runs = [runs]
    server = self.__getServer()
    return server.getRunFilesDataQuality( runs )

  #############################################################################
  def setFilesInvisible( self, lfns ):
    """
    It is used to set the file(s) invisible in the database
    Input parameter:
    lfns: an lfn or list of lfns
    """
    server = self.__getServer()

    if type( lfns ) == types.StringType:
      lfns = [lfns]
    return server.setFilesInvisible( lfns )


  #############################################################################
  def setFilesVisible( self, lfns ):
    """
    It is used to set the file(s) invisible in the database
    Input parameter:
    lfns: an lfn or list of lfns
    """
    server = self.__getServer()

    if type( lfns ) == types.StringType:
      lfns = [lfns]
    return server.setFilesVisible( lfns )


  #############################################################################
  def getRunAndProcessingPassDataQuality( self, runnb, processing ):
    """
    It returns the data quality flag for a given run and processing pass.
    """
    server = self.__getServer()
    return server.getRunAndProcessingPassDataQuality( long( runnb ), long( processing ) )


  #############################################################################
  def getAvailableConfigurations( self ):
    """
    It returns the available configurations.
    """
    server = self.__getServer()
    return server.getAvailableConfigurations()


  #############################################################################
  def getProductionProcessingPass( self, prodid ):
    """
    It returns the processing pass for a given production.
    """
    server = self.__getServer()
    return server.getProductionProcessingPass( long( prodid ) )

  #############################################################################
  def getRunProcessingPass( self, runnumber ):
    """
    it returns the run number for a given run.
    """
    server = self.__getServer()
    return server.getRunProcessingPass( long( runnumber ) )

  ############################################################################
  def getProductionFilesStatus( self, productionid = None, lfns = [] ):
    """
    It returns the file status in the bkk for a given production or a list of lfns.
    """
    server = self.__getServer()
    return server.getProductionFilesStatus( productionid, lfns )

  #############################################################################
  def getFilesWithGivenDataSets( self, values ):
    """
    It returns a list of files for a given condition.
    """
    return self.getFiles( values )

  #############################################################################
  def getVisibleFilesWithMetadata( self, values ):
    """
    It returns a list of files with metadata for a given condition.
    """
    server = self.__getServer()
    return server.getVisibleFilesWithMetadata( values )

  #############################################################################
  def addProduction( self, in_dict ):
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
    return server.addProduction( in_dict )

  #############################################################################
  def getEventTypes( self, in_dict ):
    """
    It returns the available event types for a given configuration name and configuration version.
    Input parameters:
    ConfigName, ConfigVersion, Production
    """
    server = self.__getServer()
    return server.getEventTypes( in_dict )

  #############################################################################
  def getProcessingPassSteps( self, in_dict ):
    """
    It returns the steps for a given stepname processing pass ands production.
    """
    server = self.__getServer()
    return server.getProcessingPassSteps( in_dict )

  #############################################################################
  def getProductionProcessingPassSteps( self, in_dict ):
    """
    it returns the steps for a given production.
    """
    server = self.__getServer()
    return server.getProductionProcessingPassSteps( in_dict )

  #############################################################################
  def getProductionOutputFileTypes( self, in_dict ):
    """
    It returns the output file types which produced by a given production.
    """
    server = self.__getServer()
    return server.getProductionOutputFileTypes( in_dict )

  #############################################################################
  def getRunWithProcessingPassAndDataQuality( self, procpass, flag = 'ALL' ):
    """
    It returns the run number for a given processing pass and a flag from the run quality table.
    """
    server = self.__getServer()
    return server.getRunWithProcessingPassAndDataQuality( procpass, flag )

  #############################################################################
  def getRunNbAndTck( self, lfn ):
    """
    It returns the run number and tck for a given LFN.
    """
    server = self.__getServer()
    return server.getRunNbAndTck( lfn )

  #############################################################################
  def getRunAndProcessingPass( self, in_dict ):
    """
    It returns all the processing pass and run number for a given run.
    """
    server = self.__getServer()
    return server.getRunAndProcessingPass( in_dict )

  #############################################################################
  def getNbOfRawFiles( self, in_dict ):
    """
    It counts the raw files for a given run and (or) event type.
    """
    server = self.__getServer()
    return server.getNbOfRawFiles( in_dict )

  #############################################################################
  def getFileTypeVersion( self, lfns ):
    """
    It returns the file type version of given lfns
    """
    server = self.__getServer()
    if type( lfns ) == types.StringType:
      lfns = lfns.split( ';' )
    return server.getFileTypeVersion( lfns )

  #############################################################################
  def getTCKs( self, in_dict ):
    """
    It returns the tcks for a given data set.
    """
    server = self.__getServer()
    return server.getTCKs( in_dict )

  #############################################################################
  def getStepsMetadata( self, in_dict ):
    """
    It returns the step(s) which is produced  a given dataset.
    """
    server = self.__getServer()
    return server.getStepsMetadata( in_dict )

  #############################################################################
  def getDirectoryMetadata( self, lfn ):
    """
    It returns metadata informatiom for a given directory.
    """
    if type( lfn ) == types.StringType:
      lfn = [lfn]
    server = self.__getServer()
    return server.getDirectoryMetadata_new( lfn )

  #############################################################################
  def getFilesForGUID( self, guid ):
    """
    It returns a file for a given GUID.
    """
    server = self.__getServer()
    return server.getFilesForGUID( guid )

  #############################################################################
  def getRunsGroupedByDataTaking( self ):
    """
    It returns all the run numbers grouped by the data taking description.
    """
    server = self.__getServer()
    return server.getRunsGroupedByDataTaking()

  #############################################################################
  def getListOfFills( self, in_dict ):
    """
    It returns a list of FILL numbers for a given Configuration name,
    Configuration version and data taking description.
    """
    server = self.__getServer()
    return server.getListOfFills( in_dict )

  #############################################################################
  def getRunsForFill( self, fillid ):
    """
    It returns a list of runs for a given FILL
    """
    server = self.__getServer()
    try:
      fill = long( fillid )
    except ValueError, ex:
      return S_ERROR( ex )
    return server.getRunsForFill( fill )

  #############################################################################
  def getListOfRuns( self, in_dict ):
    """
    It returns a list of runs for a given conditions.
    Input parameter is a dictionary which has the following keys: 'ConfigName', 'ConfigVersion', 'ConditionDescription', 'EventType','ProcessingPass'
    """
    server = self.__getServer()
    return server.getListOfRuns( in_dict )

  #############################################################################
  def getSimulationConditions( self, in_dict ):
    """It returns a list of simulation conditions for a given conditions
    """
    server = self.__getServer()
    return server.getSimulationConditions( in_dict )

  #############################################################################
  def updateSimulationConditions( self, in_dict ):
    """It updates a given simulation condition
    """
    server = self.__getServer()
    return server.updateSimulationConditions( in_dict )

  #############################################################################
  def deleteSimulationConditions( self, simid ):
    """It deletes a given simulation condition
    """
    try:
      simid = long( simid )
    except ValueError, ex:
      return S_ERROR( ex )
    server = self.__getServer()
    return server.deleteSimulationConditions( simid )

  #############################################################################
  def getProductionSummaryFromView( self, in_dict ):
    """it returns a summary for a given condition."""
    server = self.__getServer()
    return server.getProductionSummaryFromView( in_dict )

  #############################################################################
  def getJobInputOutputFiles( self, diracjobids ):
    """It returns the input and output files for a given DIRAC jobid"""
    if type( diracjobids ) in [ types.LongType, types.IntType]:
      diracjobids = [diracjobids]
    server = self.__getServer()
    return server.getJobInputOutputFiles( diracjobids )

  # The following method names are changed in the Bookkeeping client.

  #############################################################################
  @staticmethod
  def __errorReport( errMsg ):
    """Temporary method. It prints an error message"""

    errorString = '                 WARNING                         \n'
    errorString += '                                                 \n'
    errorString += errMsg
    gLogger.error( errorString )
    return None

  #############################################################################
  def getProductionInformation( self, prodid ):
    """Temporary method"""
    self.__errorReport( "The 'getProductionInformation' method will be removed. \
    If you would like to use it, please write a mail to LHCb bookkeeping <lhcb-bookkeeping@cern.ch>" )
    server = self.__getServer()
    return server.getProductionInformation( long( prodid ) )

  #############################################################################
  def sendBookkeeping( self, name, xml ):
    """Temporary method"""
    self.__errorReport( "The 'sendBookkeeping' method is obsolete and it will be removed\
     from the next LHCbDirac release. Please use the sendXMLBookkeepingReport!" )
    return self.sendXMLBookkeepingReport( xml )

  #############################################################################
  def setQuality( self, lfns, flag ):
    """Temporary method"""
    self.__errorReport( "The 'setQuality' method is obsolete and it will be removed\
     from the next LHCbDirac release. Please use the 'setFileDataQuality'!" )
    if type( lfns ) == types.StringType:
      lfns = [lfns]
    return self.setFileDataQuality( lfns, flag )

  #############################################################################
  def getFilesSumary( self, in_dict ):
    """Temporary method"""
    self.__errorReport( "The 'getFilesSumary' method is obsolete and it will be removed\
     from the next release. Please use the getFilesSummary!" )
    return self.getFilesSummary( in_dict )

  #############################################################################
  def getAvailableRunNumbers( self ):
    """Temporary method"""
    self.__errorReport( "The 'getAvailableRunNumbers' method is obsolete and it will be \
    removed from the next release. Please use the 'getAvailableRuns'!" )
    return self.getAvailableRuns()

  #############################################################################
  def setRunQualityWithProcessing( self, runNB, procpass, flag ):
    """Temporary method"""
    self.__errorReport( "The 'setRunQualityWithProcessing' method is obsolete and it will be removed\
     from the next release. Please use the 'setRunAndProcessingPassDataQuality'!" )
    return self.setRunAndProcessingPassDataQuality( runNB, procpass, flag )

  #############################################################################
  def setQualityRun( self, runNb, flag ):
    """Temporary method"""
    self.__errorReport( "The 'setQualityRun' method is obsolete and it will be removed \
    from the next release. Please use the 'setRunDataQuality'!" )
    return self.setRunDataQuality( runNb, flag )

  #############################################################################
  def setQualityProduction( self, prod, flag ):
    """Temporary method"""
    self.__errorReport( "The 'setQualityProduction' method is obsolete and it will be removed\
     from the next release. Please use the 'setProductionDataQuality'!" )
    return self.setProductionDataQuality( prod, flag )

  #############################################################################
  def getLFNsByProduction( self, prodid ):
    """Temporary method"""
    self.__errorReport( "The 'getLFNsByProduction' method is obsolete and it will be\
     removed from the next release. Please use the 'getProductionFiles'!" )
    return self.getProductionFiles( prodid, 'ALL', 'ALL' )

  #############################################################################
  def getAncestors( self, lfns, depth = 1 ):
    """Temporary method"""
    self.__errorReport( "The 'getAncestors' method is obsolete and it will be removed \
    from the next release. Please use the 'getFileAncestors'!" )
    server = self.__getServer()

    if type( lfns ) == types.StringType:
      lfns = [lfns]
    retVal = server.getFileAncestors( lfns, depth, True )
    if retVal['OK']:
      values = retVal['Value']
      for key, value in values['Successful'].items():
        values['Successful'][key] = [ i['FileName'] for i in value]
      return S_OK( values )
    else:
      return retVal



  #############################################################################
  def getAllAncestorsWithFileMetaData( self, lfns, depth = 1 ):
    """Temporary method"""
    self.__errorReport( "The 'getAllAncestorsWithFileMetaData' method is obsolete and it will be removed\
     from the next release. Please use the 'getFileAncestors'!" )
    server = self.__getServer()

    if type( lfns ) == types.StringType:
      lfns = [lfns]
    return server.getFileAncestors( lfns, depth, False )


  #############################################################################
  def getAllAncestors( self, lfns, depth = 1 ):
    """Temporary method"""
    self.__errorReport( "The 'getAllAncestors' method is obsolete and it will be removed \
    from the next release. Please use the 'getFileAncestors'!" )
    server = self.__getServer()

    if type( lfns ) == types.StringType:
      lfns = [lfns]
    retVal = server.getFileAncestors( lfns, depth, False )
    if retVal['OK']:
      values = retVal['Value']
      for key, value in values['Successful'].items():
        values['Successful'][key] = [ i['FileName'] for i in value]
      return S_OK( values )
    else:
      return retVal



  #############################################################################
  def getDescendents( self, lfns, depth = 0 ):
    """Temporary method"""
    self.__errorReport( "The 'getDescendents' method is obsolete and \
    it will be removed from the next release. Please use the 'getFileDescendents'!" )
    server = self.__getServer()

    if type( lfns ) == types.StringType:
      lfns = [lfns]
    return server.getFileDescendents( lfns, depth, 0, True )


  #############################################################################
  def getAllDescendents( self, lfns, depth = 0, production = 0, checkreplica = False ):
    """Temporary method"""
    self.__errorReport( "The 'getAllDescendents' method is obsolete and it will be removed \
    from the next release. Please use the 'getFileDescendents'!" )
    server = self.__getServer()
    if type( lfns ) == types.StringType:
      lfns = [lfns]
    return server.getFileDescendents( lfns, depth, production, checkreplica )


  #############################################################################
  def checkfile( self, fileName ):
    """Temporary method"""
    self.__errorReport( "The 'checkfile' method will be removed. If you would like to use it, \
    please write a mail to LHCb bookkeeping <lhcb-bookkeeping@cern.ch>" )
    server = self.__getServer()
    return server.checkfile( fileName )


  #############################################################################
  def checkFileTypeAndVersion( self, filetype, version ):
    """Temporary method"""
    self.__errorReport( "The 'checkFileTypeAndVersion' method will be removed. If you would like to use it,\
     please write a mail to LHCb bookkeeping <lhcb-bookkeeping@cern.ch>" )
    server = self.__getServer()
    return server.checkFileTypeAndVersion( filetype, version )


  #############################################################################
  def checkEventType( self, eventTypeId ):
    """Temporary method"""
    self.__errorReport( "The 'checkEventType' method will be removed. If you would like to use it, \
    please write a mail to LHCb bookkeeping <lhcb-bookkeeping@cern.ch>" )
    server = self.__getServer()
    return server.checkEventType( long( eventTypeId ) )


  #############################################################################
  def removeReplica( self, fileName ):
    """Temporary method"""
    self.__errorReport( "The 'removeReplica' method will be removed. If you would like to use it, \
    please write a mail to LHCb bookkeeping <lhcb-bookkeeping@cern.ch>" )
    server = self.__getServer()
    return server.removeReplica( fileName )

  #############################################################################
  def addReplica( self, fileName ):
    """Temporary method"""
    self.__errorReport( "The 'addReplica' method will be removed. If you would like to use it, \
    please write a mail to LHCb bookkeeping <lhcb-bookkeeping@cern.ch>" )
    server = self.__getServer()
    return server.addReplica( fileName )

  #############################################################################
  def getFilesInformations( self, lfns ):
    """Temporary method"""
    self.__errorReport( "The 'getFilesInformations' method is obsolete and it will be removed\
     from the next release. Please use the 'getFileMetadata'!" )
    server = self.__getServer()
    if type( lfns ) == types.StringType:
      lfns = [lfns]
    return server.getFileMetadata( lfns )

  #############################################################################
  def getFileMetaDataForUsers( self, lfns ):
    """Temporary method"""
    self.__errorReport( "The 'getFileMetaDataForUsers' method is obsolete and it will be removed from\
     the next release. Please use the 'getFileMetaDataForWeb'!" )
    server = self.__getServer()
    if type( lfns ) == types.StringType:
      lfns = [lfns]
    return server.getFileMetaDataForWeb( lfns )


  ############################################################################
  def getProductionFilesForUsers( self, prod, ftype, sortDict, startItem, maxitems ):
    """Temporary method"""
    self.__errorReport( "The 'getProductionFilesForUsers' method is obsolete and it will be removed\
     from the next release. Please use the 'getProductionFilesForWeb'!" )
    server = self.__getServer()
    return server.getProductionFilesForWeb( int( prod ), ftype, sortDict, long( startItem ), long( maxitems ) )


  #############################################################################
  def getLogfile( self, lfn ):
    """Temporary method"""
    self.__errorReport( "The 'getLogfile' method is obsolete and it will be removed from\
     the next release. Please use the 'getFileCreationLog'!" )
    server = self.__getServer()
    return server.getFileCreationLog( lfn )


  #############################################################################
  def addEventType( self, evid, desc, primary ):
    """Temporary method"""
    self.__errorReport( "The 'addEventType' method is obsolete and it will be removed \
    from the next release. Please use the 'insertEventType'!" )
    server = self.__getServer()
    return server.insertEventType( long( evid ), desc, primary )

  #############################################################################
  def getProductionInformations_new( self, prodid ):
    """Temporary method"""
    self.__errorReport( "The 'getProductionInformations_new' method is obsolete and it will be removed\
     from the next release. Please use the 'getProductionInformations'!" )
    server = self.__getServer()
    return server.getProductionInformations( long( prodid ) )


  #############################################################################
  def getJobsNb( self, prodid ):
    """Temporary method"""
    self.__errorReport( "The 'getJobsNb' method is obsolete and it will be removed from\
     the next release. Please use the 'getProductionNbOfJobs'!" )
    server = self.__getServer()
    return server.getProductionNbOfJobs( long( prodid ) )

  #############################################################################
  def getNumberOfEvents( self, prodid ):
    """Temporary method"""
    self.__errorReport( "The 'getNumberOfEvents' method is obsolete and it will be\
     removed from the next release. Please use the 'getNumberOfEvents'!" )
    server = self.__getServer()
    return server.getProductionNbOfEvents( long( prodid ) )

  #############################################################################
  def getSizeOfFiles( self, prodid ):
    """Temporary method"""
    self.__errorReport( "The 'getSizeOfFiles' method is obsolete and it will be removed \
    from the next release. Please use the 'getProductionSizeOfFiles'!" )
    server = self.__getServer()
    return server.getProductionSizeOfFiles( long( prodid ) )

  #############################################################################
  def getNbOfFiles( self, prodid ):
    """Temporary method"""
    self.__errorReport( "The 'getNbOfFiles' method is obsolete and it will be removed\
     from the next release. Please use the 'getProductionNbOfFiles'!" )
    server = self.__getServer()
    return server.getProductionNbOfFiles( long( prodid ) )

  #############################################################################
  def getProcessedEvents( self, prodid ):
    """Temporary method"""
    self.__errorReport( "The 'getProcessedEvents' method is obsolete and it will be removed\
     from the next release. Please use the 'getProductionProcessedEvents'!" )
    server = self.__getServer()
    return server.getProductionProcessedEvents( int( prodid ) )

  #############################################################################
  def getRunsWithAGivenDates( self, in_dict ):
    """Temporary method"""
    self.__errorReport( "The 'getRunsWithAGivenDates' method is obsolete and it will be removed \
    from the next release. Please use the 'getRunsForAGivenPeriod'!" )
    server = self.__getServer()
    return server.getRunsForAGivenPeriod( in_dict )

  #############################################################################
  def getProductiosWithAGivenRunAndProcessing( self, in_dict ):
    """Temporary method"""
    self.__errorReport( "The 'getProductiosWithAGivenRunAndProcessing' method is obsolete and \
    it will be removed from the next release. Please use the 'getProductionsFromView'!" )
    server = self.__getServer()
    return server.getProductionsFromView( in_dict )

  #############################################################################
  def getDataQualityForRuns( self, runs ):
    """Temporary method"""
    self.__errorReport( "The 'getDataQualityForRuns' method is obsolete and it will be removed\
     from the next release. Please use the 'getRunFilesDataQuality'!" )
    server = self.__getServer()
    return server.getRunFilesDataQuality( runs )

  #############################################################################
  def getRunFlag( self, runnb, processing ):
    """Temporary method"""
    self.__errorReport( "The 'getRunFlag' method is obsolete and it will be removed from the next release.\
     Please use the 'getRunAndProcessingPassDataQuality'!" )
    server = self.__getServer()
    return server.getRunAndProcessingPassDataQuality( long( runnb ), long( processing ) )


  #############################################################################
  def getProductionProcessingPassID( self, prodid ):
    """Temporary method"""
    self.__errorReport( "The 'getProductionProcessingPassID' method will be removed.\
     If you would like to use it, please write a mail to LHCb bookkeeping <lhcb-bookkeeping@cern.ch>" )
    server = self.__getServer()
    return server.getProductionProcessingPassID( long( prodid ) )

  ############################################################################
  def getProductionStatus( self, productionid = None, lfns = [] ):
    """Temporary method"""
    self.__errorReport( "The 'getProductionStatus' method is obsolete and it will be removed \
    from the next release. Please use the 'getProductionFilesStatus'!" )
    server = self.__getServer()

    if productionid != None:
      return server.getProductionFilesStatus( productionid, None )
    else:
      return server.getProductionFilesStatus( None, lfns )


  #############################################################################
  def getFiles( self, in_dict ):
    """
    It returns a list of files for a given condition.
    """
    in_dict = dict( in_dict )
    bkk = TransferClient( 'Bookkeeping/BookkeepingManager' )
    in_dict['MethodName'] = 'getFiles'
    savedbuffer = cPickle.dumps( in_dict )
    file_name = tempfile.NamedTemporaryFile()
    params = str( savedbuffer )
    retVal = bkk.receiveFile( file_name.name, params )
    if not retVal['OK']:
      return retVal
    else:
      value = cPickle.load( open( file_name.name ) )
      file_name.close()
      return value


  #############################################################################
  def getFilesWithGivenDataSetsForUsers( self, values ):
    """Temporary method"""
    self.__errorReport( "The 'getFilesWithGivenDataSetsForUsers' method is obsolete and it will be removed\
     from the next release. Please use the 'getVisibleFilesWithMetadata'!" )
    server = self.__getServer()
    return server.getVisibleFilesWithMetadata( values )

  #############################################################################
  def getStandardEventTypes( self, in_dict ):
    """Temporary method"""
    self.__errorReport( "The 'getStandardEventTypes' method is obsolete and it will be removed from\
     the next release. Please use the 'getEventTypes'!" )
    server = self.__getServer()
    return server.getEventTypes( in_dict )

  #############################################################################
  def getProductionOutputFiles( self, in_dict ):
    """Temporary method"""
    self.__errorReport( "The 'getProductionOutputFiles' method is obsolete and it will be removed\
     from the next release. Please use the 'getProductionOutputFileTypes'!" )
    server = self.__getServer()
    return server.getProductionOutputFileTypes( in_dict )

  #############################################################################
  def getRunQuality( self, procpass, flag = 'ALL' ):
    """Temporary method"""
    self.__errorReport( "The 'getRunQuality' method is obsolete and it will be removed from \
    the next release. Please use the 'getRunWithProcessingPassAndDataQuality'!" )
    server = self.__getServer()
    return server.getRunWithProcessingPassAndDataQuality( procpass, flag )

  #############################################################################
  def getRunProcPass( self, in_dict ):
    """Temporary method"""
    self.__errorReport( "The 'getRunProcPass' method is obsolete and it will be removed\
     from the next release. Please use the 'getRunAndProcessingPass'!" )
    server = self.__getServer()
    return server.getRunAndProcessingPass( in_dict )

  #############################################################################
  def getRunNbFiles( self, in_dict ):
    """Temporary method"""
    self.__errorReport( "The 'getRunNbFiles' method is obsolete and it will be removed\
     from the next release. Please use the 'getNbOfRawFiles'!" )
    server = self.__getServer()
    return server.getNbOfRawFiles( in_dict )

  #############################################################################
  def getTypeVersion( self, lfn ):
    """Temporary method"""
    self.__errorReport( "The 'getTypeVersion' method is obsolete and it will be removed \
    from the next release. Please use the 'getFileTypeVersion'!" )
    server = self.__getServer()
    if type( lfn ) == types.StringType:
      return server.getFileTypeVersion( [lfn] )
    else:
      return server.getFileTypeVersion( lfn )

  #############################################################################
  def getAvailableTcks( self, in_dict ):
    """Temporary method"""
    self.__errorReport( "The 'getAvailableTcks' method is obsolete and it\
    will be removed from the next release. Please use the 'getTCKs'!" )
    server = self.__getServer()
    return server.getTCKs( in_dict )

class BKClientWithRetry():
  """
  Utility class wrapping BKClient with retries
  """
  def __init__( self, bkClient = None, retries = None ):
    if not bkClient:
      bkClient = BookkeepingClient()
    self.bk = bkClient
    self.retries = retries if retries else 5
    self.method = None
  def __getattr__( self, x ):
    self.method = x
    return self.__executeMethod
  def __executeMethod( self, *args, **kwargs ):
    fcn = getattr( self.bk, self.method )
    for _i in range( self.retries ):
      res = fcn( *args, **kwargs )
      if res['OK']:
        break
    return res



