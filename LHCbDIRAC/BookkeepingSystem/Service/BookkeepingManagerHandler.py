""" BookkeepingManaher service is the front-end to the Bookkeeping database
"""

import cPickle

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.ConfigurationSystem.Client.PathFinder import getServiceSection
from DIRAC.ConfigurationSystem.Client.Helpers import cfgPath
from DIRAC.ConfigurationSystem.Client.Config import gConfig

from LHCbDIRAC.BookkeepingSystem.DB.BookkeepingDatabaseClient import BookkeepingDatabaseClient
from LHCbDIRAC.BookkeepingSystem.Service.XMLReader.XMLFilesReaderManager import XMLFilesReaderManager
from LHCbDIRAC.BookkeepingSystem.Client import JEncoder
from LHCbDIRAC.BookkeepingSystem.DB.Utilities import checkEnoughBKArguments

__RCSID__ = "$Id$"

#pylint: disable=invalid-name


dataMGMT_ = None

reader_ = None

global default
default = 'ALL'

def initializeBookkeepingManagerHandler( serviceInfo ):
  """ Put here necessary initializations needed at the service start
  """
  global dataMGMT_
  dataMGMT_ = BookkeepingDatabaseClient()

  global reader_
  reader_ = XMLFilesReaderManager()
  return S_OK()


class BookkeepingManagerHandler( RequestHandler ):

  """
  Bookkeeping Service class. It serves the requests made the users by using the BookkeepingClient.
  """

  @classmethod
  def initializeHandler( cls, serviceInfoDict ):
    """
      initialize the variables used to identify queries, which are not containing enough conditions.
    """

    bkkSection = getServiceSection( "Bookkeeping/BookkeepingManager" )
    if not bkkSection:
      cls.email = 'lhcb-bookkeeping@cern.ch'
      cls.forceExecution = False
    else:
      cls.email = gConfig.getValue( cfgPath( bkkSection , 'Email' ), 'lhcb-bookkeeping@cern.ch' )
      cls.forceExecution = gConfig.getValue( cfgPath( bkkSection , 'ForceExecution' ), False )
    gLogger.info( "Email used to track queries: %s forceExecution" % cls.email, cls.forceExecution )
    return S_OK()
  ###########################################################################
  # types_<methodname> global variable is a list which defines for each exposed
  # method the types of its arguments, the argument types are ignored if the list is empty.

  types_echo = [basestring]
  @staticmethod
  def export_echo( inputstring ):
    """ Echo input to output
    """
    return S_OK( inputstring )

  #############################################################################
  types_sendBookkeeping = [basestring, basestring]
  def export_sendBookkeeping( self, name, xml ):
    """
    more info in the BookkeepingClient.py
    """
    return self.export_sendXMLBookkeepingReport( xml )


  #############################################################################
  types_sendXMLBookkeepingReport = [basestring]
  @staticmethod
  def export_sendXMLBookkeepingReport( xml ):
    """more info in the BookkeepingClient.py"""
    result = S_ERROR()
    try:
      retVal = reader_.readXMLfromString( xml )
      if not retVal['OK']:
        result = S_ERROR( retVal['Message'] )
      elif retVal['Value'] == '':
        result = S_OK( "The send bookkeeping finished successfully!" )
      else:
        result = retVal
    except Exception as x:
      errorMsg = "XML processing error"
      gLogger.exception(errorMsg, lException = x)
      result = S_ERROR( errorMsg )
    return result

  #############################################################################
  types_getAvailableSteps = [dict]
  @staticmethod
  def export_getAvailableSteps( in_dict ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getAvailableSteps( in_dict )

  #############################################################################
  types_getRuntimeProjects = [dict]
  @staticmethod
  def export_getRuntimeProjects( in_dict ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getRuntimeProjects( in_dict )

  #############################################################################
  types_getStepInputFiles = [int]
  @staticmethod
  def export_getStepInputFiles( stepId ):
    """more info in the BookkeepingClient.py"""
    result = S_ERROR()
    retVal = dataMGMT_.getStepInputFiles( stepId )
    if retVal['OK']:
      records = []
      parameters = ['FileType', 'Visible']
      for record in retVal['Value']:
        records += [list( record )]
      result = S_OK( {'ParameterNames':parameters, 'Records':records, 'TotalRecords':len( records )} )
    else:
      result = retVal
    return result

  #############################################################################
  types_setStepInputFiles = [int, list]
  @staticmethod
  def export_setStepInputFiles( stepid, files ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.setStepInputFiles( stepid, files )

  #############################################################################
  types_setStepOutputFiles = [int, list]
  @staticmethod
  def export_setStepOutputFiles( stepid, files ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.setStepOutputFiles( stepid, files )

  #############################################################################
  types_getStepOutputFiles = [int]
  @staticmethod
  def export_getStepOutputFiles( stepId ):
    """more info in the BookkeepingClient.py"""
    result = S_ERROR()
    retVal = dataMGMT_.getStepOutputFiles( stepId )
    if retVal['OK']:
      records = []
      parameters = ['FileType', 'Visible']
      for record in retVal['Value']:
        records += [list( record )]
      result = S_OK( {'ParameterNames':parameters, 'Records':records, 'TotalRecords':len( records )} )
    else:
      result = retVal
    return result

  #############################################################################
  types_getAvailableFileTypes = []
  @staticmethod
  def export_getAvailableFileTypes():
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getAvailableFileTypes()

  #############################################################################
  types_insertFileTypes = [basestring, basestring, basestring]
  @staticmethod
  def export_insertFileTypes( ftype, desc, fileType ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.insertFileTypes( ftype, desc, fileType )

  #############################################################################
  types_insertStep = [dict]
  @staticmethod
  def export_insertStep( in_dict ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.insertStep( in_dict )

  #############################################################################
  types_deleteStep = [int]
  @staticmethod
  def export_deleteStep( stepid ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.deleteStep( stepid )

  #############################################################################
  types_updateStep = [dict]
  @staticmethod
  def export_updateStep( in_dict ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.updateStep( in_dict )

  ##############################################################################
  types_getAvailableConfigNames = []
  @staticmethod
  def export_getAvailableConfigNames():
    """more info in the BookkeepingClient.py"""
    retVal = dataMGMT_.getAvailableConfigNames()
    if retVal['OK']:
      records = []
      parameters = ['Configuration Name']
      for record in retVal['Value']:
        records += [list( record )]
      return S_OK( {'ParameterNames':parameters, 'Records':records, 'TotalRecords':len( records )} )
    else:
      return retVal

  #############################################################################
  types_getConfigVersions = [dict]
  @staticmethod
  def export_getConfigVersions( in_dict ):
    """more info in the BookkeepingClient.py"""
    result = S_ERROR()
    configName = in_dict.get( 'ConfigName', default )
    retVal = dataMGMT_.getConfigVersions( configName )
    if retVal['OK']:
      records = []
      parameters = ['Configuration Version']
      for record in retVal['Value']:
        records += [list( record )]
      result = S_OK( {'ParameterNames':parameters, 'Records':records, 'TotalRecords':len( records )} )
    else:
      result = retVal
    return result

  #############################################################################
  types_getConditions = [dict]
  @staticmethod
  def export_getConditions( in_dict ):
    """more info in the BookkeepingClient.py"""
    result = S_ERROR()
    ok = True
    configName = in_dict.get( 'ConfigName', default )
    configVersion = in_dict.get( 'ConfigVersion', default )
    evt = in_dict.get( 'EventType', in_dict.get( 'EventTypeId', default ) )

    if 'EventTypeId' in in_dict:
      gLogger.verbose( 'EventTypeId will be not accepted! Please change it to EventType' )

    retVal = dataMGMT_.getConditions( configName, configVersion, evt )
    if retVal['OK']:
      values = retVal['Value']
      sim_parameters = ['SimId',
                        'Description',
                        'BeamCondition',
                        'BeamEnergy',
                        'Generator',
                        'MagneticField',
                        'DetectorCondition',
                        'Luminosity',
                        'G4settings' ]
      daq_parameters = ['DaqperiodId', 'Description',
                        'BeamCondition', 'BeanEnergy',
                        'MagneticField', 'VELO',
                        'IT', 'TT', 'OT', 'RICH1',
                        'RICH2', 'SPD_PRS', 'ECAL',
                        'HCAL', 'MUON', 'L0', 'HLT',
                        'VeloPosition']
      sim_records = []
      daq_records = []

      if len( values ) > 0:
        for record in values:
          if record[0] != None:
            sim_records += [[record[0], record[2],
                             record[3], record[4],
                             record[5], record[6],
                             record[7], record[8],
                             record[9]]]
          elif record[1] != None:
            daq_records += [[record[1], record[10], record[11],
                             record[12], record[13], record[14],
                             record[15], record[16], record[17],
                             record[18], record[19], record[20],
                             record[21], record[22], record[23],
                             record[24], record[25], record[26]]]
          else:
            result = S_ERROR( "Condition does not existis!" )
            ok = False
      if ok:
        result = S_OK( [{'ParameterNames':sim_parameters,
                         'Records':sim_records,
                         'TotalRecords':len( sim_records )}, { 'ParameterNames':daq_parameters,
                                                               'Records':daq_records,
                                                               'TotalRecords':len( daq_records )}
                       ] )
    else:
      result = retVal

    return result

  #############################################################################
  types_getProcessingPass = [dict, basestring]
  @staticmethod
  def export_getProcessingPass( in_dict, path ):
    """more info in the BookkeepingClient.py"""
    configName = in_dict.get( 'ConfigName', default )
    configVersion = in_dict.get( 'ConfigVersion', default )
    conddescription = in_dict.get( 'ConditionDescription', default )
    prod = in_dict.get( 'Production', default )
    runnb = in_dict.get( 'RunNumber', default )
    evt = in_dict.get( 'EventType', in_dict.get( 'EventTypeId', default ) )
    return dataMGMT_.getProcessingPass( configName, configVersion, conddescription, runnb, prod, evt, path )

  ############################################################################
  types_getStandardProcessingPass = [dict, basestring]
  def export_getStandardProcessingPass( self, in_dict, path ):
    """more info in the BookkeepingClient.py"""
    result = S_ERROR()
    retVal = self.export_getProcessingPass( in_dict, path )
    if retVal['OK']:
      result = S_OK( retVal['Value'] )
    else:
      result = retVal
    return result

  #############################################################################
  types_getProductions = [dict]
  @staticmethod
  def export_getProductions( in_dict ):
    """more info in the BookkeepingClient.py"""
    result = S_ERROR()
    configName = in_dict.get( 'ConfigName', default )
    configVersion = in_dict.get( 'ConfigVersion', default )
    conddescription = in_dict.get( 'ConditionDescription', default )
    processing = in_dict.get( 'ProcessingPass', default )
    evt = in_dict.get( 'EventType', in_dict.get( 'EventTypeId', default ) )
    replicaFlag = in_dict.get( 'ReplicaFlag', 'Yes' )
    ftype = in_dict.get( 'FileType', default )
    visible = in_dict.get( 'Visible', 'Y' )
    if 'EventTypeId' in in_dict:
      gLogger.verbose( 'The EventTypeId has to be replaced by EventType!' )

    retVal = dataMGMT_.getProductions( configName, configVersion, conddescription, processing, evt, visible, ftype, replicaFlag)
    if retVal['OK']:
      records = []
      parameters = ['Production/RunNumber']
      for record in retVal['Value']:
        records += [[record[0]]]
      result = S_OK( {'ParameterNames':parameters, 'Records':records, 'TotalRecords':len( records )} )
    else:
      result = retVal
    return result

  #############################################################################
  types_getFileTypes = [dict]
  @staticmethod
  def export_getFileTypes( in_dict ):
    """more info in the BookkeepingClient.py"""
    result = S_ERROR()
    configName = in_dict.get( 'ConfigName', default )
    configVersion = in_dict.get( 'ConfigVersion', default )
    conddescription = in_dict.get( 'ConditionDescription', default )
    processing = in_dict.get( 'ProcessingPass', default )
    evt = in_dict.get( 'EventType', in_dict.get( 'EventTypeId', default ) )
    production = in_dict.get( 'Production', default )
    runnb = in_dict.get( 'RunNumber', default )
    visible = in_dict.get( 'Visible', 'Y' )

    if 'EventTypeId' in in_dict:
      gLogger.verbose( 'The EventTypeId has to be replaced by EventType!' )

    retVal = dataMGMT_.getFileTypes( configName, configVersion,
                                     conddescription, processing,
                                     evt, runnb,
                                     production, visible )
    if retVal['OK']:
      records = []
      parameters = ['FileTypes']
      for record in retVal['Value']:
        records += [[record[0]]]
      result = S_OK( {'ParameterNames':parameters, 'Records':records, 'TotalRecords':len( records )} )
    else:
      result = retVal
    return result

  #############################################################################
  types_getStandardEventTypes = [dict]
  def export_getStandardEventTypes( self, in_dict ):
    """more info in the BookkeepingClient.py"""
    self.export_getEventTypes( in_dict )

  #############################################################################
  def transfer_toClient( self, parameters, token, fileHelper ):
    """ This method used to transfer data using a file. Currently two client methods are
    using this function: getFiles, getFilesWithMetadata
    """
    result = S_OK()
    iscPickleFormat = False
    try:
      in_dict = JEncoder.loads( parameters )
    except Exception as _:
      iscPickleFormat = True
      in_dict = cPickle.loads( parameters )
    gLogger.debug( "The following dictionary received: %s" % in_dict )
    methodName = in_dict.get( 'MethodName', default )
    if methodName == 'getFiles':
      retVal = self.__getFiles( in_dict )
    else:
      retVal = self.__getFilesWithMetadata( in_dict )

    if iscPickleFormat:
      fileString = cPickle.dumps( retVal, protocol = 2 )
    else:
      fileString = JEncoder.dumps( retVal )

    retVal = fileHelper.stringToNetwork( fileString )
    if retVal['OK']:
      gLogger.info( 'Sent file %s of size %d' % ( str( in_dict ), len( fileString ) ) )
    else:
      gLogger.error( "Failed to send files %s" % in_dict )
      result = retVal
    return result

  #############################################################################
  @staticmethod
  @checkEnoughBKArguments
  def __getFiles( in_dict ):
    """It returns a list of files.
    """
    simdesc = in_dict.get( 'SimulationConditions', default )
    datataking = in_dict.get( 'DataTakingConditions', default )
    procPass = in_dict.get( 'ProcessingPass', default )
    ftype = in_dict.get( 'FileType', default )
    evt = in_dict.get( 'EventType', default )
    configname = in_dict.get( 'ConfigName', default )
    configversion = in_dict.get( 'ConfigVersion', default )
    prod = in_dict.get( 'Production', in_dict.get( 'ProductionID', default ) )
    flag = in_dict.get( 'DataQuality', in_dict.get( 'DataQualityFlag', default ) )
    startd = in_dict.get( 'StartDate', None )
    endd = in_dict.get( 'EndDate', None )
    nbofevents = in_dict.get( 'NbOfEvents', False )
    startRunID = in_dict.get( 'StartRun', None )
    endRunID = in_dict.get( 'EndRun', None )
    runNbs = in_dict.get( 'RunNumber', in_dict.get( 'RunNumbers', [] ) )
    if not isinstance( runNbs, list ):
      runNbs = [runNbs]
    replicaFlag = in_dict.get( 'ReplicaFlag', 'Yes' )
    visible = in_dict.get( 'Visible', default )
    filesize = in_dict.get( 'FileSize', False )
    tck = in_dict.get( 'TCK', [] )

    if 'ProductionID' in in_dict:
      gLogger.verbose( 'ProductionID will be removed. It will changed to Production' )

    if 'DataQualityFlag' in in_dict:
      gLogger.verbose( 'DataQualityFlag will be removed. It will changed to DataQuality' )

    if 'RunNumbers' in in_dict:
      gLogger.verbose( 'RunNumbers will be removed. It will changed to RunNumbers' )

    result = []
    retVal = dataMGMT_.getFiles( simdesc, datataking,
                                 procPass, ftype, evt,
                                 configname, configversion,
                                 prod, flag, startd, endd,
                                 nbofevents, startRunID,
                                 endRunID, runNbs,
                                 replicaFlag, visible,
                                 filesize, tck )
    if not retVal['OK']:
      result = retVal
    else:
      values = retVal['Value']
      for i in values:
        result += [i[0]]
      result = S_OK( result )

    return result

  #############################################################################
  @staticmethod
  @checkEnoughBKArguments
  def __getFilesWithMetadata( in_dict ):
    """
    It returns the files with their metadata. This result will be transfered to the client
    using a pickle file
    """
    configName = in_dict.get( 'ConfigName', default )
    configVersion = in_dict.get( 'ConfigVersion', default )
    conddescription = in_dict.get( 'ConditionDescription', default )
    processing = in_dict.get( 'ProcessingPass', default )
    evt = in_dict.get( 'EventType', in_dict.get( 'EventTypeId', default ) )
    production = in_dict.get( 'Production', default )
    filetype = in_dict.get( 'FileType', default )
    quality = in_dict.get( 'DataQuality', in_dict.get( 'Quality', default ) )
    visible = in_dict.get( 'Visible', 'Y' )
    replicaFlag = in_dict.get( 'ReplicaFlag', 'Yes' )
    startDate = in_dict.get( 'StartDate', None )
    endDate = in_dict.get( 'EndDate', None )
    runnumbers = in_dict.get( 'RunNumber', in_dict.get( 'RunNumbers', [] ) )
    startRunID = in_dict.get( 'StartRun', None )
    endRunID = in_dict.get( 'EndRun', None )
    tcks = in_dict.get( 'TCK' )

    if 'EventTypeId' in in_dict:
      gLogger.verbose( 'The EventTypeId has to be replaced by EventType!' )

    if 'Quality' in in_dict:
      gLogger.verbose( 'The Quality has to be replaced by DataQuality!' )

    retVal = dataMGMT_.getFilesWithMetadata( configName,
                                             configVersion,
                                             conddescription,
                                             processing,
                                             evt,
                                             production,
                                             filetype,
                                             quality,
                                             visible,
                                             replicaFlag,
                                             startDate,
                                             endDate,
                                             runnumbers,
                                             startRunID,
                                             endRunID,
                                             tcks )
    if retVal['OK']:
      records = []
      parameters = ['FileName', 'EventStat', 'FileSize',
                    'CreationDate', 'JobStart', 'JobEnd',
                    'WorkerNode', 'FileType', 'RunNumber',
                    'FillNumber', 'FullStat', 'DataqualityFlag',
                    'EventInputStat', 'TotalLuminosity', 'Luminosity',
                    'InstLuminosity', 'TCK','GUID', 'ADLER32', 'EventType','MD5SUM',
                    'VisibilityFlag', 'JobId', 'GotReplica', 'InsertTimeStamp']
      for record in retVal['Value']:
        records += [[record[0], record[1], record[2],
                     record[3], record[4], record[5],
                     record[6], record[7], record[8],
                     record[9], record[10], record[11],
                     record[12], record[13], record[14],
                     record[15], record[16], record[17], record[18], record[19],
                     record[20], record[21], record[22], record[23], record[24]]]
      retVal = {'ParameterNames':parameters, 'Records':records, 'TotalRecords':len( records )}
    return retVal

  #############################################################################
  types_getFilesSumary = [dict]
  def export_getFilesSumary( self, in_dict ):
    """more info in the BookkeepingClient.py"""
    return self.export_getFilesSummary( in_dict )

  #############################################################################
  types_getFilesSummary = [dict]
  @checkEnoughBKArguments
  def export_getFilesSummary( self, in_dict ):
    """more info in the BookkeepingClient.py"""
    gLogger.debug( 'Input:' + str( in_dict ) )
    result = S_ERROR()

    configName = in_dict.get( 'ConfigName', default )
    configVersion = in_dict.get( 'ConfigVersion', default )
    condDescription = in_dict.get( 'ConditionDescription', default )
    processingPass = in_dict.get( 'ProcessingPass', default )
    eventType = in_dict.get( 'EventType', in_dict.get( 'EventTypeId', default ) )
    production = in_dict.get( 'Production', default )
    fileType = in_dict.get( 'FileType', default )
    dataQuality = in_dict.get( 'DataQuality', in_dict.get( 'Quality', default ) )
    startRun = in_dict.get( 'StartRun', None )
    endRun = in_dict.get( 'EndRun', None )
    visible = in_dict.get( 'Visible', 'Y' )
    startDate = in_dict.get( 'StartDate', None )
    endDate = in_dict.get( 'EndDate', None )
    runNumbers = in_dict.get( 'RunNumber', in_dict.get( 'RunNumbers', [] ) )
    replicaFlag = in_dict.get( 'ReplicaFlag', 'Yes' )
    tcks = in_dict.get( 'TCK' )

    if 'EventTypeId' in in_dict:
      gLogger.verbose( 'The EventTypeId has to be replaced by EventType!' )

    if 'Quality' in in_dict:
      gLogger.verbose( 'The Quality has to be replaced by DataQuality!' )

    retVal = dataMGMT_.getFilesSummary( configName = configName,
                                        configVersion = configVersion,
                                        conditionDescription = condDescription,
                                        processingPass = processingPass,
                                        eventType = eventType,
                                        production = production,
                                        fileType = fileType,
                                        dataQuality = dataQuality,
                                        startRun = startRun,
                                        endRun = endRun,
                                        visible = visible,
                                        startDate = startDate,
                                        endDate = endDate,
                                        runNumbers = runNumbers,
                                        replicaFlag = replicaFlag,
                                        tcks = tcks )
    if retVal['OK']:
      records = []
      parameters = ['NbofFiles', 'NumberOfEvents', 'FileSize', 'Luminosity', 'InstLuminosity']
      for record in retVal['Value']:
        records += [[record[0], record[1], record[2], record[3], record[4]]]
      result = S_OK( {'ParameterNames':parameters, 'Records':records, 'TotalRecords':len( records )} )
    else:
      result = retVal

    return result

  #############################################################################
  types_getLimitedFiles = [dict]
  @staticmethod
  def export_getLimitedFiles( in_dict ):
    """more info in the BookkeepingClient.py"""

    result = S_ERROR()
    configName = in_dict.get( 'ConfigName', default )
    configVersion = in_dict.get( 'ConfigVersion', default )
    conddescription = in_dict.get( 'ConditionDescription', default )
    processing = in_dict.get( 'ProcessingPass', default )
    evt = in_dict.get( 'EventType', in_dict.get( 'EventTypeId', default ) )
    production = in_dict.get( 'Production', default )
    filetype = in_dict.get( 'FileType', default )
    quality = in_dict.get( 'DataQuality', in_dict.get( 'Quality', default ) )
    runnb = in_dict.get( 'RunNumbers', in_dict.get( 'RunNumber', default ) )
    start = in_dict.get( 'StartItem', 0 )
    maxValue = in_dict.get( 'MaxItem', 10 )

    if 'EventTypeId' in in_dict:
      gLogger.verbose( 'The EventTypeId has to be replaced by EventType!' )

    if 'Quality' in in_dict:
      gLogger.verbose( 'The Quality has to be replaced by DataQuality!' )

    retVal = dataMGMT_.getLimitedFiles( configName,
                                        configVersion,
                                        conddescription,
                                        processing,
                                        evt,
                                        production,
                                        filetype,
                                        quality,
                                        runnb,
                                        start,
                                        maxValue )
    if retVal['OK']:
      records = []
      parameters = ['Name', 'EventStat', 'FileSize',
                    'CreationDate', 'JobStart', 'JobEnd',
                    'WorkerNode', 'FileType', 'EventType',
                    'RunNumber', 'FillNumber', 'FullStat',
                    'DataqualityFlag', 'EventInputStat',
                    'TotalLuminosity', 'Luminosity',
                    'InstLuminosity', 'TCK']
      for record in retVal['Value']:
        records += [[record[0], record[1], record[2],
                     str( record[3] ), str( record[4] ),
                     str( record[5] ), record[6], record[7],
                     record[8], record[9], record[10],
                     record[11], record[12], record[13],
                     record[14], record[15], record[16],
                     record[17]]]
      result = S_OK( {'ParameterNames':parameters, 'Records':records, 'TotalRecords':len( records )} )
    else:
      result = retVal
    return result

  #############################################################################
  types_getAvailableDataQuality = []
  @staticmethod
  def export_getAvailableDataQuality():
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getAvailableDataQuality()

  #############################################################################
  types_getAvailableProductions = []
  @staticmethod
  def export_getAvailableProductions():
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getAvailableProductions()

  #############################################################################
  types_getAvailableRuns = []
  @staticmethod
  def export_getAvailableRuns():
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getAvailableRuns()

  #############################################################################
  types_getAvailableEventTypes = []
  @staticmethod
  def export_getAvailableEventTypes():
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getAvailableEventTypes()

  #############################################################################
  types_getMoreProductionInformations = [int]
  @staticmethod
  def export_getMoreProductionInformations( prodid ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getMoreProductionInformations( prodid )

  #############################################################################
  types_getJobInfo = [basestring]
  @staticmethod
  def export_getJobInfo( lfn ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getJobInfo( lfn )

  #############################################################################
  types_bulkJobInfo = [dict]
  @staticmethod
  def export_bulkJobInfo( lfns ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.bulkJobInfo( lfns )

  #############################################################################
  types_getJobInformation = [dict]
  @staticmethod
  def export_getJobInformation( in_dict ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getJobInformation( in_dict )

  #############################################################################
  types_getRunNumber = [basestring]
  @staticmethod
  def export_getRunNumber( lfn ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getRunNumber( lfn )

  #############################################################################
  types_getRunNbAndTck = [basestring]
  @staticmethod
  def export_getRunNbAndTck( lfn ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getRunNbAndTck( lfn )

  #############################################################################
  types_getProductionFiles = [int, basestring]
  @staticmethod
  def export_getProductionFiles( prod, fileType, replica = default ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionFiles( int( prod ), fileType, replica )

  #############################################################################
  types_getAvailableRunNumbers = []
  def export_getAvailableRunNumbers( self ):
    """more info in the BookkeepingClient.py"""
    return self.export_getAvailableRuns()

  #############################################################################
  types_getRunFiles = [int]
  @staticmethod
  def export_getRunFiles( runid ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getRunFiles( runid )

  #############################################################################
  types_updateFileMetaData = [basestring, dict]
  @staticmethod
  def export_updateFileMetaData( filename, fileAttr ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.updateFileMetaData( filename, fileAttr )

  #############################################################################
  types_renameFile = [basestring, basestring]
  @staticmethod
  def export_renameFile( oldLFN, newLFN ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.renameFile( oldLFN, newLFN )

  #############################################################################
  types_getProductionProcessingPassID = [long]
  @staticmethod
  def export_getProductionProcessingPassID( prodid ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionProcessingPassID( prodid )

  #############################################################################
  types_getProductionProcessingPass = [long]
  @staticmethod
  def export_getProductionProcessingPass( prodid ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionProcessingPass( prodid )

  #############################################################################
  types_insertTag = [dict]
  @staticmethod
  def export_insertTag( values ):
    """more info in the BookkeepingClient.py"""
    successfull = {}
    faild = {}

    for i in values:
      tags = values[i]
      for tag in tags:
        retVal = dataMGMT_.existsTag( i, tag )
        if retVal['OK'] and not retVal['Value']:
          retVal = dataMGMT_.insertTag( i, tag )
          if not retVal['OK']:
            faild[tag] = i
          else:
            successfull[tag] = i
        else:
          faild[tag] = i
    return S_OK( {'Successfull':successfull, 'Faild':faild} )

  #############################################################################
  types_setQuality = [list, basestring]
  def export_setQuality( self, lfns, flag ):
    """more info in the BookkeepingClient.py"""
    return self.export_setFileDataQuality( lfns, flag )

  #############################################################################
  types_setFileDataQuality = [list, basestring]
  @staticmethod
  def export_setFileDataQuality( lfns, flag ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.setFileDataQuality( lfns, flag )

  #############################################################################
  types_setRunAndProcessingPassDataQuality = [long, basestring, basestring]
  @staticmethod
  def export_setRunAndProcessingPassDataQuality( runNB, procpass, flag ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.setRunAndProcessingPassDataQuality( runNB, procpass, flag )

  #############################################################################
  types_setRunQualityWithProcessing = [long, basestring, basestring]
  def export_setRunQualityWithProcessing( self, runNB, procpass, flag ):
    """more info in the BookkeepingClient.py"""
    return self.export_setRunAndProcessingPassDataQuality( runNB, procpass, flag )

  #############################################################################
  types_setRunDataQuality = [int, basestring]
  @staticmethod
  def export_setRunDataQuality( runNb, flag ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.setRunDataQuality( runNb, flag )

  #############################################################################
  types_setQualityRun = [int, basestring]
  @staticmethod
  def export_setQualityRun( runNb, flag ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.setRunDataQuality( runNb, flag )

  #############################################################################
  types_setProductionDataQuality = [int, basestring]
  @staticmethod
  def export_setProductionDataQuality( prod, flag ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.setProductionDataQuality( prod, flag )

  #############################################################################
  types_setQualityProduction = [int, basestring]
  def export_setQualityProduction( self, prod, flag ):
    """more info in the BookkeepingClient.py"""
    return self.export_setProductionDataQuality( prod, flag )

  types_getLFNsByProduction = [int]
  def export_getLFNsByProduction( self, prod ):
    """more info in the BookkeepingClient.py"""
    return self.export_getProductionFiles( prod, 'ALL', 'ALL' )

  #############################################################################
  types_getFileAncestors = [list, int, bool]
  @staticmethod
  def export_getFileAncestors( lfns, depth, replica ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileAncestors( lfns, depth, replica )

  #############################################################################
  types_getAllAncestors = [list, int]
  @staticmethod
  def export_getAllAncestors( lfns, depth ):
    """more info in the BookkeepingClient.py"""
    result = S_ERROR()
    retVal = dataMGMT_.getFileAncestors( lfns, depth, False )
    if retVal['OK']:
      values = retVal['Value']
      for key, value in values['Successful'].items():
        values['Successful'][key] = [ i['FileName'] for i in value]
      result = S_OK( values )
    else:
      result = retVal
    return result

  #############################################################################
  types_getAncestors = [list, int]
  @staticmethod
  def export_getAncestors( lfns, depth ):
    """more info in the BookkeepingClient.py"""
    result = S_ERROR()
    retVal = dataMGMT_.getFileAncestors( lfns, depth, True )
    if retVal['OK']:
      values = retVal['Value']
      for key, value in values['Successful'].items():
        values['Successful'][key] = [ i['FileName'] for i in value]
      result = S_OK( values )
    else:
      result = retVal
    return result

  #############################################################################
  types_getAllAncestorsWithFileMetaData = [list, int]
  @staticmethod
  def export_getAllAncestorsWithFileMetaData( lfns, depth ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileAncestors( lfns, depth, False )

  #############################################################################
  types_getAllDescendents = [list, int, int, bool]
  @staticmethod
  def export_getAllDescendents( lfn, depth = 0, production = 0, checkreplica = False ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileDescendents( lfn, depth, production, checkreplica )

  #############################################################################
  types_getDescendents = [list, int]
  def export_getDescendents( self, lfn, depth ):
    """more info in the BookkeepingClient.py"""
    return self.export_getFileDescendants( lfn, depth )

  #############################################################################
  types_getFileDescendents = [list, int, int, bool]
  @staticmethod
  def export_getFileDescendents( lfn, depth, production = 0, checkreplica = True ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileDescendents( lfn, depth, production, checkreplica )

  #############################################################################
  types_getFileDescendants = [list, int, int, bool]
  def export_getFileDescendants( self, lfn, depth, production = 0, checkreplica = True ):
    """more info in the BookkeepingClient.py"""
    return self.export_getFileDescendents( lfn, depth, production, checkreplica )

  #############################################################################
  types_checkfile = [basestring]
  @staticmethod
  def export_checkfile( fileName ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.checkfile( fileName )

  #############################################################################
  types_checkFileTypeAndVersion = [basestring, basestring]
  @staticmethod
  def export_checkFileTypeAndVersion( ftype, version ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.checkFileTypeAndVersion( ftype, version )

  #############################################################################
  types_checkEventType = [long]
  @staticmethod
  def export_checkEventType( eventTypeId ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.checkEventType( eventTypeId )

  #############################################################################
  types_insertSimConditions = [dict]
  @staticmethod
  def export_insertSimConditions( in_dict ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.insertSimConditions( in_dict )

  #############################################################################
  types_getSimConditions = []
  @staticmethod
  def export_getSimConditions():
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getSimConditions()

  #############################################################################
  types_removeReplica = [basestring]
  @staticmethod
  def export_removeReplica( fileName ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.removeReplica( fileName )

  #############################################################################
  types_getFileMetadata = [list]
  @staticmethod
  def export_getFileMetadata( lfns ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileMetadata( lfns )

  #############################################################################
  types_getFilesInformations = [list]
  @staticmethod
  def export_getFilesInformations( lfns ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileMetadata( lfns )

  #############################################################################
  types_getFileMetaDataForUsers = [list]
  @staticmethod
  def export_getFileMetaDataForUsers( lfns ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileMetaDataForWeb( lfns )

  #############################################################################
  types_getFileMetaDataForWeb = [list]
  @staticmethod
  def export_getFileMetaDataForWeb( lfns ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileMetaDataForWeb( lfns )

  #############################################################################
  types_getProductionFilesForUsers = [int, dict, dict, long, long]
  @staticmethod
  def export_getProductionFilesForUsers( prod, ftype, sortDict, startItem, maxitems ):
    """more info in the BookkeepingClient.py"""
    res = dataMGMT_.getProductionFilesForWeb( prod, ftype, sortDict, startItem, maxitems )
    return res

  #############################################################################
  types_getProductionFilesForWeb = [int, dict, dict, long, long]
  @staticmethod
  def export_getProductionFilesWeb( prod, ftype, sortDict, startItem, maxitems ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionFilesForWeb( prod, ftype, sortDict, startItem, maxitems )

  #############################################################################
  types_exists = [list]
  @staticmethod
  def export_exists( lfns ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.exists( lfns )

  #############################################################################
  types_addReplica = [basestring]
  @staticmethod
  def export_addReplica( fileName ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.addReplica( fileName )

  #############################################################################
  types_getRunInformations = [int]
  @staticmethod
  def export_getRunInformations( runnb ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getRunInformations( runnb )

  #############################################################################
  types_getRunInformation = [dict]
  @staticmethod
  def export_getRunInformation( runnb ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getRunInformation( runnb )

  #############################################################################
  types_getFileCreationLog = [basestring]
  @staticmethod
  def export_getFileCreationLog( lfn ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileCreationLog( lfn )

  #############################################################################
  types_getLogfile = [basestring]
  @staticmethod
  def export_getLogfile( lfn ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileCreationLog( lfn )

  #############################################################################
  types_insertEventType = [long, basestring, basestring]
  @staticmethod
  def export_insertEventType( evid, desc, primary ):
    """more info in the BookkeepingClient.py"""
    result = S_ERROR()

    retVal = dataMGMT_.checkEventType( evid )
    if not retVal['OK']:
      retVal = dataMGMT_.insertEventTypes( evid, desc, primary )
      if retVal['OK']:
        result = S_OK( str( evid ) + ' event type added successfully!' )
      else:
        result = retVal
    else:
      result = S_OK( str( evid ) + ' event type exists' )
    return result

  #############################################################################
  types_addEventType = [long, basestring, basestring]
  def export_addEventType( self, evid, desc, primary ):
    """more info in the BookkeepingClient.py"""
    return self.export_insertEventType( evid, desc, primary )

  #############################################################################
  types_updateEventType = [long, basestring, basestring]
  @staticmethod
  def export_updateEventType( evid, desc, primary ):
    """more info in the BookkeepingClient.py"""
    result = S_ERROR()

    retVal = dataMGMT_.checkEventType( evid )
    if not retVal['OK']:
      result = S_ERROR( str( evid ) + ' event type is missing in the BKK database!' )
    else:
      retVal = dataMGMT_.updateEventType( evid, desc, primary )
      if retVal['OK']:
        result = S_OK( str( evid ) + ' event type updated successfully!' )
      else:
        result = retVal
    return result

  #############################################################################
  types_addFiles = [list]
  @staticmethod
  def export_addFiles( lfns ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.addReplica( lfns )

  #############################################################################
  types_removeFiles = [list]
  @staticmethod
  def export_removeFiles( lfns ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.removeReplica( lfns )

  #############################################################################
  types_getProductionSummary = [dict]
  @staticmethod
  def export_getProductionSummary( in_dict ):
    """more info in the BookkeepingClient.py"""

    cName = in_dict.get( 'ConfigName', default )
    cVersion = in_dict.get( 'ConfigVersion', default )
    production = in_dict.get( 'Production', default )
    simdesc = in_dict.get( 'ConditionDescription', default )
    pgroup = in_dict.get( 'ProcessingPass', default )
    ftype = in_dict.get( 'FileType', default )
    evttype = in_dict.get( 'EventType', default )
    retVal = dataMGMT_.getProductionSummary( cName, cVersion, simdesc, pgroup, production, ftype, evttype )

    return retVal

  #############################################################################
  types_getProductionInformations_new = [long]
  def export_getProductionInformations_new( self, prodid ):
    """more info in the BookkeepingClient.py"""
    return self.export_getProductionInformations( prodid )

  #############################################################################
  types_getProductionInformations = [long]
  @staticmethod
  def export_getProductionInformations( prodid ):
    """more info in the BookkeepingClient.py"""

    nbjobs = None
    nbOfFiles = None
    nbOfEvents = None
    steps = None
    prodinfos = None

    value = dataMGMT_.getProductionNbOfJobs( prodid )
    if value['OK']:
      nbjobs = value['Value']

    value = dataMGMT_.getProductionNbOfFiles( prodid )
    if value['OK']:
      nbOfFiles = value['Value']

    value = dataMGMT_.getProductionNbOfEvents( prodid )
    if value['OK']:
      nbOfEvents = value['Value']

    value = dataMGMT_.getConfigsAndEvtType( prodid )
    if value['OK']:
      prodinfos = value['Value']

    path = '/'

    if len( prodinfos ) == 0:
      return S_ERROR( 'This production does not contains any jobs!' )
    cname = prodinfos[0][0]
    cversion = prodinfos[0][1]
    path += cname + '/' + cversion + '/'

    value = dataMGMT_.getSteps( prodid )
    if value['OK']:
      steps = value['Value']
    else:
      steps = value['Message']
      result = {"Production informations":prodinfos,
                "Steps":steps,
                "Number of jobs":nbjobs,
                "Number of files":nbOfFiles,
                "Number of events":nbOfEvents,
                'Path':path}
      return S_OK( result )

      # return S_ERROR(value['Message'])


    res = dataMGMT_.getProductionSimulationCond( prodid )
    if not res['OK']:
      return S_ERROR( res['Message'] )
    else:
      path += res['Value']
    res = dataMGMT_.getProductionProcessingPass( prodid )
    if not res['OK']:
      return S_ERROR( res['Message'] )
    else:
      path += res['Value']
    prefix = '\n' + path

    for i in nbOfEvents:
      path += prefix + '/' + str( i[2] ) + '/' + i[0]
    result = {"Production informations":prodinfos,
              "Steps":steps,
              "Number of jobs":nbjobs,
              "Number of files":nbOfFiles,
              "Number of events":nbOfEvents,
              'Path':path}
    return S_OK( result )

  #############################################################################
  types_getProductionInformationsFromView = [long]
  @staticmethod
  def export_getProductionInformationsFromView( prodid ):
    """more info in the BookkeepingClient.py"""

    result = S_ERROR()
    value = dataMGMT_.getProductionInformationsFromView( prodid )
    parameters = []
    infos = []
    if value['OK']:
      records = value['Value']
      parameters = ['Production', 'EventType', 'FileType', 'NumberOfEvents', 'NumberOfFiles']
      for record in records:
        infos += [[record[0], record[1], record[2], record[3], record[4]]]
      result = S_OK( {'ParameterNames':parameters, 'Records':infos} )
    else:
      result = value
    return result

  #############################################################################
  types_getFileHistory = [basestring]
  @staticmethod
  def export_getFileHistory( lfn ):
    """more info in the BookkeepingClient.py"""
    retVal = dataMGMT_.getFileHistory( lfn )
    result = {}
    records = []
    if retVal['OK']:
      values = retVal['Value']
      parameterNames = ['FileId', 'FileName',
                        'ADLER32', 'CreationDate',
                        'EventStat', 'Eventtype',
                        'Gotreplica', 'GUI', 'JobId',
                        'md5sum', 'FileSize', 'FullStat',
                        'Dataquality', 'FileInsertDate',
                        'Luminosity', 'InstLuminosity']
      counter = 0
      for record in values:
        value = [record[0], record[1], record[2],
                 record[3], record[4], record[5],
                 record[6], record[7], record[8],
                 record[9], record[10], record[11],
                 record[12], record[13], record[14],
                 record[15]]
        records += [value]
        counter += 1
      result = {'ParameterNames':parameterNames, 'Records':records, 'TotalRecords':counter}
    else:
      result = S_ERROR( retVal['Message'] )
    return S_OK( result )

  #############################################################################
  types_getJobsNb = [long]
  @staticmethod
  def export_getJobsNb( prodid ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionNbOfJobs( prodid )

  #############################################################################
  types_getProductionNbOfJobs = [long]
  @staticmethod
  def export_getProductionNbOfJobs( prodid ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionNbOfJobs( prodid )

  #############################################################################
  types_getNumberOfEvents = [long]
  @staticmethod
  def export_getNumberOfEvents( prodid ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionNbOfEvents( prodid )

  #############################################################################
  types_getProductionNbOfEvents = [long]
  @staticmethod
  def export_getProductionNbOfEvents( prodid ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionNbOfEvents( prodid )

  #############################################################################
  types_getSizeOfFiles = [long]
  @staticmethod
  def export_getSizeOfFiles( prodid ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionSizeOfFiles( prodid )

  #############################################################################
  types_getProductionSizeOfFiles = [long]
  @staticmethod
  def export_getProductionSizeOfFiles( prodid ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionSizeOfFiles( prodid )


  #############################################################################
  types_getNbOfFiles = [long]
  @staticmethod
  def export_getNbOfFiles( prodid ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionNbOfFiles( prodid )

  #############################################################################
  types_getProductionNbOfFiles = [long]
  @staticmethod
  def export_getProductionNbOfFiles( prodid ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionNbOfFiles( prodid )

  #############################################################################
  types_getProductionInformation = [long]
  @staticmethod
  def export_getProductionInformation( prodid ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionInformation( prodid )

  #############################################################################
  types_getNbOfJobsBySites = [long]
  @staticmethod
  def export_getNbOfJobsBySites( prodid ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getNbOfJobsBySites( prodid )

  #############################################################################
  types_getAvailableTags = []
  @staticmethod
  def export_getAvailableTags():
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getAvailableTags()

  #############################################################################
  types_getProcessedEvents = [int]
  @staticmethod
  def export_getProcessedEvents( prodid ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionProcessedEvents( prodid )

  #############################################################################
  types_getProductionProcessedEvents = [int]
  @staticmethod
  def export_getProductionProcessedEvents( prodid ):
    """more info in the BookkeepingClient.py"""
    gLogger.info( 'getProductionProcessedEvents->Production: %d ' % prodid )
    return dataMGMT_.getProductionProcessedEvents( prodid )

  #############################################################################
  types_getRunsForAGivenPeriod = [dict]
  @staticmethod
  def export_getRunsForAGivenPeriod( in_dict ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getRunsForAGivenPeriod( in_dict )

  #############################################################################
  types_getProductiosWithAGivenRunAndProcessing = [dict]
  def export_getProductiosWithAGivenRunAndProcessing( self, in_dict ):
    """more info in the BookkeepingClient.py"""
    return self.export_getProductionsFromView( in_dict )

  #############################################################################
  types_getProductionsFromView = [dict]
  @staticmethod
  def export_getProductionsFromView( in_dict ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionsFromView( in_dict )

  #############################################################################
  types_getDataQualityForRuns = [list]
  def export_getDataQualityForRuns( self, runs ):
    """more info in the BookkeepingClient.py"""
    return self.export_getRunFilesDataQuality( runs )

  #############################################################################
  types_getRunFilesDataQuality = [list]
  @staticmethod
  def export_getRunFilesDataQuality( runs ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getRunFilesDataQuality( runs )

  #############################################################################
  types_setFilesInvisible = [list]
  @staticmethod
  def export_setFilesInvisible( lfns ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.setFilesInvisible( lfns )

  #############################################################################
  types_setFilesVisible = [list]
  @staticmethod
  def export_setFilesVisible( lfns ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.setFilesVisible( lfns )

  #############################################################################
  types_getRunFlag = [long, long]
  def export_getRunFlag( self, runnb, processing ):
    """more info in the BookkeepingClient.py"""
    return self.export_getRunAndProcessingPassDataQuality( runnb, processing )

  #############################################################################
  types_getRunAndProcessingPassDataQuality = [long, long]
  @staticmethod
  def export_getRunAndProcessingPassDataQuality( runnb, processing ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getRunAndProcessingPassDataQuality( runnb, processing )

  #############################################################################
  types_getAvailableConfigurations = []
  @staticmethod
  def export_getAvailableConfigurations():
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getAvailableConfigurations()

  #############################################################################
  types_getRunProcessingPass = [long]
  @staticmethod
  def export_getRunProcessingPass( runnumber ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getRunProcessingPass( runnumber )

  #############################################################################
  types_getProductionFilesStatus = [int, list]
  @staticmethod
  def export_getProductionFilesStatus( productionid = None, lfns = None ):
    """more info in the BookkeepingClient.py"""
    if not lfns:
      lfns = []
    return dataMGMT_.getProductionFilesStatus( productionid, lfns )

  #############################################################################
  types_getFilesWithGivenDataSets = [dict]
  def export_getFilesWithGivenDataSets( self, values ):
    """more info in the BookkeepingClient.py"""
    gLogger.debug( 'getFiles dataset:' + str( values ) )
    return self.export_getFiles( values )

  #############################################################################
  types_getFiles = [dict]
  @staticmethod
  def export_getFiles( values ):
    """more info in the BookkeepingClient.py"""

    simdesc = values.get( 'SimulationConditions', default )
    datataking = values.get( 'DataTakingConditions', default )
    procPass = values.get( 'ProcessingPass', default )
    ftype = values.get( 'FileType', default )
    evt = values.get( 'EventType', 0 )
    configname = values.get( 'ConfigName', default )
    configversion = values.get( 'ConfigVersion', default )
    prod = values.get( 'Production', values.get( 'ProductionID', default ) )
    flag = values.get( 'DataQuality', values.get( 'DataQualityFlag', default ) )
    startd = values.get( 'StartDate', None )
    endd = values.get( 'EndDate', None )
    nbofevents = values.get( 'NbOfEvents', False )
    startRunID = values.get( 'StartRun', None )
    endRunID = values.get( 'EndRun', None )
    runNbs = values.get( 'RunNumber', values.get( 'RunNumbers', [] ) )
    if not isinstance( runNbs, list ):
      runNbs = [runNbs]
    replicaFlag = values.get( 'ReplicaFlag', 'Yes' )
    visible = values.get( 'Visible', default )
    filesize = values.get( 'FileSize', False )
    tck = values.get( 'TCK' )

    if 'ProductionID' in values:
      gLogger.verbose( 'ProductionID will be removed. It will changed to Production' )

    if 'DataQualityFlag' in values:
      gLogger.verbose( 'DataQualityFlag will be removed. It will changed to DataQuality' )

    if 'RunNumbers' in values:
      gLogger.verbose( 'RunNumbers will be removed. It will changed to RunNumbers' )

    result = []
    retVal = dataMGMT_.getFiles( simdesc, datataking,
                                 procPass, ftype, evt,
                                 configname, configversion,
                                 prod, flag, startd, endd,
                                 nbofevents, startRunID,
                                 endRunID, runNbs,
                                 replicaFlag, visible,
                                 filesize, tck )
    if not retVal['OK']:
      return S_ERROR( retVal['Message'] )
    else:
      values = retVal['Value']
      for i in values:
        result += [i[0]]

    return S_OK( result )

  #############################################################################
  types_getFilesWithGivenDataSetsForUsers = [dict]
  def export_getFilesWithGivenDataSetsForUsers( self, values ):
    """more info in the BookkeepingClient.py"""
    return self.export_getVisibleFilesWithMetadata( values )

  #############################################################################
  types_getVisibleFilesWithMetadata = [dict]
  @staticmethod
  def export_getVisibleFilesWithMetadata( in_dict ):
    """more info in the BookkeepingClient.py"""

    conddescription = in_dict.get( 'SimulationConditions', in_dict.get( 'DataTakingConditions', default ) )
    procPass = in_dict.get( 'ProcessingPass', default )
    ftype = in_dict.get( 'FileType', default )
    evt = in_dict.get( 'EventType', default )
    configname = in_dict.get( 'ConfigName', default )
    configversion = in_dict.get( 'ConfigVersion', default )
    prod = in_dict.get( 'Production', in_dict.get( 'ProductionID', default ) )
    dqflag = in_dict.get( 'DataQuality', in_dict.get( 'DataQualityFlag', default ) )
    startd = in_dict.get( 'StartDate', None )
    endd = in_dict.get( 'EndDate', None )
    startRunID = in_dict.get( 'StartRun', None )
    endRunID = in_dict.get( 'EndRun', None )
    runNbs = in_dict.get( 'RunNumber', in_dict.get( 'RunNumbers', [] ) )
    replicaFlag = in_dict.get( 'ReplicaFlag', 'Yes' )
    tck = in_dict.get( 'TCK', [] )
    visible = in_dict.get( 'Visible', 'Y' )

    if ftype == default:
      return S_ERROR( 'FileType is missing!' )

    if 'ProductionID' in in_dict:
      gLogger.verbose( 'ProductionID will be removed. It will changed to Production' )

    if 'DataQualityFlag' in in_dict:
      gLogger.verbose( 'DataQualityFlag will be removed. It will changed to DataQuality' )

    if 'RunNumbers' in in_dict:
      gLogger.verbose( 'RunNumbers will be removed. It will changed to RunNumbers' )

    gLogger.info( "getVisibleFilesWithMetadata->%s", in_dict )
    result = {}
    retVal = dataMGMT_.getFilesWithMetadata( configName = configname,
                                             configVersion = configversion,
                                             conddescription = conddescription,
                                             processing = procPass,
                                             evt = evt,
                                             production = prod,
                                             filetype = ftype,
                                             quality = dqflag,
                                             visible = visible,
                                             replicaflag = replicaFlag,
                                             startDate = startd,
                                             endDate = endd,
                                             runnumbers = runNbs,
                                             startRunID = startRunID,
                                             endRunID = endRunID,
                                             tcks = tck ) 
                                            
    summary = 0
    
    parameters = ['FileName', 'EventStat', 'FileSize',
                  'CreationDate', 'JobStart', 'JobEnd',
                  'WorkerNode', 'FileType', 'RunNumber',
                  'FillNumber', 'FullStat', 'DataqualityFlag',
                  'EventInputStat', 'TotalLuminosity', 'Luminosity',
                  'InstLuminosity', 'TCK', 'GUID', 'ADLER32', 'EventType', 'MD5SUM',
                  'VisibilityFlag', 'JobId', 'GotReplica', 'InsertTimeStamp']
     
    if not retVal['OK']:
      return retVal
    else:
      values = retVal['Value']
      nbfiles = 0
      nbevents = 0
      evinput = 0
      fsize = 0
      tLumi = 0
      lumi = 0
      ilumi = 0
      for i in values:
        nbfiles = nbfiles + 1
        row = dict( zip( parameters , i ) )
        if row['EventStat'] != None:
          nbevents += row['EventStat']
        if row['EventInputStat'] != None:
          evinput += row['EventInputStat']
        if row['FileSize'] != None:
          fsize += row['FileSize']
        if row['TotalLuminosity'] != None:
          tLumi += row['TotalLuminosity']
        if row['Luminosity'] != None:
          lumi += row['Luminosity']
        if row['InstLuminosity'] != None:
          ilumi += row['InstLuminosity']
        result[row['FileName']] = {'EventStat':row['EventStat'],
                                   'EventInputStat':row['EventInputStat'],
                                   'Runnumber':row['RunNumber'],
                                   'Fillnumber':row['FillNumber'],
                                   'FileSize':row['FileSize'],
                                   'TotalLuminosity':row['TotalLuminosity'],
                                   'Luminosity':row['Luminosity'],
                                   'InstLuminosity':row['InstLuminosity'],
                                   'TCK':row['TCK']}
      if nbfiles > 0:
        summary = {'Number Of Files':nbfiles,
                   'Number of Events':nbevents,
                   'EventInputStat':evinput,
                   'FileSize':fsize / 1000000000.,
                   'TotalLuminosity':tLumi,
                   'Luminosity':lumi,
                   'InstLuminosity':ilumi}
    return S_OK( {'LFNs' : result, 'Summary': summary} )

  #############################################################################
  types_addProduction = [dict]
  @staticmethod
  def export_addProduction( infos ):
    """more info in the BookkeepingClient.py"""

    gLogger.debug( infos )
    result = None
    simcond = infos.get( 'SimulationConditions', None )
    daqdesc = infos.get( 'DataTakingConditions', None )
    production = None

    if simcond == None and daqdesc == None:
      result = S_ERROR( 'SimulationConditions or DataTakingConditins is missing!' )

    if 'Steps' not in infos:
      result = S_ERROR( "Missing Steps!" )
    if 'Production' not in infos:
      result = S_ERROR( 'Production is missing!' )

    if not result:
      steps = infos['Steps']
      inputProdTotalProcessingPass = ''
      production = infos['Production']
      inputProdTotalProcessingPass = infos.get( 'InputProductionTotalProcessingPass', '' )
      configName = infos.get("ConfigName")
      configVersion = infos.get("ConfigVersion")
      result = dataMGMT_.addProduction( production = production,
                                        simcond = simcond,
                                        daq = daqdesc,
                                        steps = steps,
                                        inputproc = inputProdTotalProcessingPass,
                                        configName = configName,
                                        configVersion = configVersion )
    return result

  #############################################################################
  types_getEventTypes = [dict]
  @staticmethod
  def export_getEventTypes( in_dict ):
    """more info in the BookkeepingClient.py"""

    configName = in_dict.get( 'ConfigName', default )
    configVersion = in_dict.get( 'ConfigVersion', default )
    production = in_dict.get( 'Production', default )
    return  dataMGMT_.getEventTypes( configName, configVersion, production )

  #############################################################################
  types_getProcessingPassSteps = [dict]
  @staticmethod
  def export_getProcessingPassSteps( in_dict ):
    """more info in the BookkeepingClient.py"""

    stepname = in_dict.get( 'StepName', default )
    cond = in_dict.get( 'ConditionDescription', default )
    procpass = in_dict.get( 'ProcessingPass', default )

    return dataMGMT_.getProcessingPassSteps( procpass, cond, stepname )

  #############################################################################
  types_getProductionProcessingPassSteps = [dict]
  @staticmethod
  def export_getProductionProcessingPassSteps( in_dict ):
    """more info in the BookkeepingClient.py"""

    if 'Production' in in_dict:
      return dataMGMT_.getProductionProcessingPassSteps( in_dict['Production'] )
    else:
      return S_ERROR( 'The Production dictionary key is missing!!!' )

  #############################################################################
  types_getProductionOutputFiles = [dict]
  def export_getProductionOutputFiles( self, in_dict ):
    """more info in the BookkeepingClient.py"""

    return self.export_getProductionOutputFileTypes( in_dict )

  #############################################################################
  types_getProductionOutputFileTypes = [dict]
  @staticmethod
  def export_getProductionOutputFileTypes( in_dict ):
    """more info in the BookkeepingClient.py"""

    production = in_dict.get( 'Production', default )
    if production != default:
      return dataMGMT_.getProductionOutputFileTypes( production )
    else:
      return S_ERROR( 'The Production dictionary key is missing!!!' )

  #############################################################################
  types_getRunQuality = [basestring, basestring]
  def export_getRunQuality( self, procpass, flag = default ):
    """more info in the BookkeepingClient.py"""

    return self.export_getRunWithProcessingPassAndDataQuality( procpass, flag )

  #############################################################################
  types_getRunWithProcessingPassAndDataQuality = [basestring, basestring]
  @staticmethod
  def export_getRunWithProcessingPassAndDataQuality( procpass, flag = default ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getRunWithProcessingPassAndDataQuality( procpass, flag )

  #############################################################################
  types_getRuns = [dict]
  @staticmethod
  def export_getRuns( in_dict ):
    """more info in the BookkeepingClient.py"""
    result = S_ERROR()
    cName = in_dict.get( 'ConfigName', default )
    cVersion = in_dict.get( 'ConfigVersion', default )
    if cName != default and cVersion != default:
      result = dataMGMT_.getRuns( cName, cVersion )
    else:
      result = S_ERROR( 'The configuration name and version have to be defined!' )
    return result

  #############################################################################
  types_getRunProcPass = [dict]
  def export_getRunProcPass( self, in_dict ):
    """more info in the BookkeepingClient.py"""
    return self.export_getRunAndProcessingPass( in_dict )

  #############################################################################
  types_getRunAndProcessingPass = [dict]
  @staticmethod
  def export_getRunAndProcessingPass( in_dict ):
    """more info in the BookkeepingClient.py"""
    run = in_dict.get( 'RunNumber', default )
    result = S_ERROR()
    if run != default:
      result = dataMGMT_.getRunAndProcessingPass( run )
    else:
      result = S_ERROR( 'The run number has to be specified!' )
    return result

  #############################################################################
  types_getProcessingPassId = [basestring]
  @staticmethod
  def export_getProcessingPassId( fullpath ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProcessingPassId( fullpath )

  #############################################################################
  types_getRunNbFiles = [dict]
  def export_getRunNbFiles( self, in_dict ):
    """more info in the BookkeepingClient.py"""
    return self.export_getNbOfRawFiles( in_dict )

  #############################################################################
  types_getNbOfRawFiles = [dict]
  @staticmethod
  def export_getNbOfRawFiles( in_dict ):
    """more info in the BookkeepingClient.py"""

    runnb = in_dict.get( 'RunNumber', default )
    evt = in_dict.get( 'EventType', in_dict.get( 'EventTypeId', default ) )
    if 'EventTypeId' in in_dict:
      gLogger.verbose( 'The EventTypeId has to be replaced by EventType!' )


    replicaFlag = in_dict.get( 'ReplicaFlag', 'Yes' )
    visible = in_dict.get( 'Visible', 'Y' )
    isFinished = in_dict.get("Finished", 'ALL')
    result = S_ERROR()
    if runnb == default and evt == default:
      result = S_ERROR( 'Run number or event type must be given!' )
    else:
      retVal = dataMGMT_.getNbOfRawFiles( runnb, evt, replicaFlag,  visible, isFinished)
      if retVal['OK']:
        result = S_OK( retVal['Value'][0][0] )
      else:
        result = retVal
    return result


  #############################################################################
  types_getTypeVersion = [list]
  def export_getTypeVersion( self, lfn ):
    """more info in the BookkeepingClient.py"""
    return self.export_getFileTypeVersion( lfn )

  #############################################################################
  types_getFileTypeVersion = [list]
  @staticmethod
  def export_getFileTypeVersion( lfn ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFileTypeVersion( lfn )

  #############################################################################
  types_getTCKs = [dict]
  @staticmethod
  def export_getTCKs( in_dict ):
    """more info in the BookkeepingClient.py"""
    configName = in_dict.get( 'ConfigName', default )
    configVersion = in_dict.get( 'ConfigVersion', default )
    conddescription = in_dict.get( 'ConditionDescription', default )
    processing = in_dict.get( 'ProcessingPass', default )
    evt = in_dict.get( 'EventType', in_dict.get( 'EventTypeId', default ) )
    production = in_dict.get( 'Production', default )
    filetype = in_dict.get( 'FileType', default )
    quality = in_dict.get( 'DataQuality', in_dict.get( 'Quality', default ) )
    runnb = in_dict.get( 'RunNumber', default )
    result = S_ERROR()
    if 'Quality' in  in_dict:
      gLogger.verbose( 'The Quality has to be replaced by DataQuality!' )

    if 'EventTypeId' in in_dict:
      gLogger.verbose( 'The EventTypeId has to be replaced by EventType!' )

    retVal = dataMGMT_.getTCKs( configName,
                                configVersion,
                                conddescription,
                                processing,
                                evt,
                                production,
                                filetype,
                                quality,
                                runnb )
    if retVal['OK']:
      records = []
      for record in retVal['Value']:
        records += [record[0]]
      result = S_OK( records )
    else:
      result = retVal
    return result

  #############################################################################
  types_getAvailableTcks = [dict]
  def export_getAvailableTcks( self, in_dict ):
    """more info in the BookkeepingClient.py"""
    return self.export_getTCKs( in_dict )

  #############################################################################
  types_getStepsMetadata = [dict]
  @staticmethod
  def export_getStepsMetadata( in_dict ):
    """more info in the BookkeepingClient.py"""
    gLogger.debug( 'getStepsMetadata' + str( in_dict ) )
    configName = in_dict.get( 'ConfigName', default )
    configVersion = in_dict.get( 'ConfigVersion', default )
    cond = in_dict.get( 'ConditionDescription', default )
    procpass = in_dict.get( 'ProcessingPass', default )
    evt = in_dict.get( 'EventType', in_dict.get( 'EventTypeId', default ) )
    production = in_dict.get( 'Production', default )
    filetype = in_dict.get( 'FileType', default )
    runnb = in_dict.get( 'RunNumber', default )

    if 'EventTypeId' in in_dict:
      gLogger.verbose( 'The EventTypeId has to be replaced by EventType!' )

    if 'Quality' in in_dict:
      gLogger.verbose( 'The Quality has to be replaced by DataQuality!' )

    return dataMGMT_.getStepsMetadata( configName, configVersion, cond, procpass, evt, production, filetype, runnb )

  #############################################################################
  types_getDirectoryMetadata_new = [list]
  @staticmethod
  def export_getDirectoryMetadata_new( lfn ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getDirectoryMetadata_new( lfn )

    #############################################################################
  types_getDirectoryMetadata = [basestring]
  @staticmethod
  def export_getDirectoryMetadata( lfn ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getDirectoryMetadata( lfn )

  #############################################################################
  types_getFilesForGUID = [basestring]
  @staticmethod
  def export_getFilesForGUID( guid ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getFilesForGUID( guid )

  #############################################################################
  types_getRunsGroupedByDataTaking = []
  @staticmethod
  def export_getRunsGroupedByDataTaking():
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getRunsGroupedByDataTaking()

  #############################################################################
  types_getListOfFills = [dict]
  @staticmethod
  def export_getListOfFills( in_dict ):
    """
    It returns a list of FILL numbers for a given Configuration name,
    Configuration version and data taking description.
    """
    configName = in_dict.get( 'ConfigName', default )
    configVersion = in_dict.get( 'ConfigVersion', default )
    conddescription = in_dict.get( 'ConditionDescription', default )
    return dataMGMT_.getListOfFills( configName, configVersion, conddescription )

  #############################################################################
  types_getRunsForFill = [long]
  @staticmethod
  def export_getRunsForFill( fillid ):
    """
    It returns a list of runs for a given FILL
    """
    return dataMGMT_.getRunsForFill( fillid )

  #############################################################################
  types_getListOfRuns = [dict]
  @staticmethod
  def export_getListOfRuns( in_dict ):
    """return the runnumbers for a given dataset"""
    result = S_ERROR()
    configName = in_dict.get( 'ConfigName', default )
    configVersion = in_dict.get( 'ConfigVersion', default )
    conddescription = in_dict.get( 'ConditionDescription', default )
    processing = in_dict.get( 'ProcessingPass', default )
    evt = in_dict.get( 'EventType', default )
    quality = in_dict.get( 'DataQuality', default )

    retVal = dataMGMT_.getListOfRuns( configName, configVersion, conddescription, processing, evt, quality )
    if not retVal['OK']:
      result = retVal
    else:
      result = S_OK( [i[0] for i in retVal['Value']] )
    return result

  #############################################################################
  types_getSimulationConditions = [dict]
  @staticmethod
  def export_getSimulationConditions( in_dict ):
    """It returns a list of simulation conditions"""
    return dataMGMT_.getSimulationConditions( in_dict )

  #############################################################################
  types_updateSimulationConditions = [dict]
  @staticmethod
  def export_updateSimulationConditions( in_dict ):
    """it updates a given simulation condition"""
    return dataMGMT_.updateSimulationConditions( in_dict )

  #############################################################################
  types_deleteSimulationConditions = [long]
  @staticmethod
  def export_deleteSimulationConditions( simid ):
    """deletes a given simulation conditions"""
    return dataMGMT_.deleteSimulationConditions( simid )

  #############################################################################
  types_getProductionSummaryFromView = [dict]
  @staticmethod
  def export_getProductionSummaryFromView( in_dict ):
    """it returns a summary for a given condition."""
    return dataMGMT_.getProductionSummaryFromView( in_dict )

  types_getJobInputOutputFiles = [list]
  @staticmethod
  def export_getJobInputOutputFiles( diracjobids ):
    """It returns the input and output files for a given DIRAC jobid"""
    return dataMGMT_.getJobInputOutputFiles( diracjobids )

  types_setRunOnlineFinished = [long]
  @staticmethod
  def export_setRunOnlineFinished( runnumber ):
    """You can set the runs finished"""
    return dataMGMT_.setRunStatusFinished( runnumber, 'Y' )

  types_setRunOnlineNotFinished = [long]
  @staticmethod
  def export_setRunOnlineNotFinished( runnumber ):
    """You can set the runs not finished"""
    return dataMGMT_.setRunStatusFinished( runnumber, 'N' )

  types_getRunStatus = [list]
  @staticmethod
  def export_getRunStatus( runnumbers ):
    """ it returns the status of the runs"""
    return dataMGMT_.getRunStatus( runnumbers )

  types_bulkupdateFileMetaData = [dict]
  @staticmethod
  def export_bulkupdateFileMetaData( lfnswithmeta ):
    return dataMGMT_.bulkupdateFileMetaData( lfnswithmeta )

  types_fixRunLuminosity = [list]
  @staticmethod
  def export_fixRunLuminosity( runnumbers ):
    return dataMGMT_.fixRunLuminosity( runnumbers )

  #############################################################################
  types_getProductionProducedEvents = [int]
  @staticmethod
  def export_getProductionProducedEvents( prodid ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getProductionProducedEvents( prodid )

  #############################################################################
  types_bulkinsertEventType = [list]
  @staticmethod
  def export_bulkinsertEventType( eventtypes ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.bulkinsertEventType( eventtypes )

  #############################################################################
  types_bulkupdateEventType = [list]
  @staticmethod
  def export_bulkupdateEventType( eventtypes ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.bulkupdateEventType( eventtypes )
  
  #############################################################################
  types_getRunConfigurationsAndDataTakingCondition = [int]
  @staticmethod
  def export_getRunConfigurationsAndDataTakingCondition( runnumber ):
    """more info in the BookkeepingClient.py"""
    return dataMGMT_.getRunConfigurationsAndDataTakingCondition( runnumber )
