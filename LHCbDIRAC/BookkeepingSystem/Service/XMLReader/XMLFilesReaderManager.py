"""
It interprets the XML reports and make a job, file, or replica object
"""

########################################################################
# $Id$
########################################################################

from xml.parsers.expat                                                                  import ExpatError
from xml.dom.minidom                                                                    import parse, parseString
from LHCbDIRAC.BookkeepingSystem.Service.XMLReader.JobReader                            import JobReader
from LHCbDIRAC.BookkeepingSystem.Service.XMLReader.ReplicaReader                        import ReplicaReader
from DIRAC                                                                              import gLogger, S_OK, S_ERROR
from LHCbDIRAC.BookkeepingSystem.DB.BookkeepingDatabaseClient                           import BookkeepingDatabaseClient
from DIRAC.DataManagementSystem.Client.DataManager                             import DataManager
from LHCbDIRAC.BookkeepingSystem.Service.XMLReader.Job.FileParam                  import FileParam
from LHCbDIRAC.BookkeepingSystem.Service.XMLReader.Job.JobParameters              import JobParameters
from LHCbDIRAC.BookkeepingSystem.DB.DataTakingConditionInterpreter                import  BeamEnergyCondition, \
                                                                                          VeloCondition, \
                                                                                          MagneticFieldCondition, \
                                                                                          EcalCondition, \
                                                                                          HcalCondition, \
                                                                                          HltCondition, \
                                                                                          ItCondition, \
                                                                                          LoCondition, \
                                                                                          MuonCondition, \
                                                                                          OtCondition, \
                                                                                          Rich1Condition, \
                                                                                          Rich2Condition, \
                                                                                          Spd_prsCondition, \
                                                                                          TtCondition, \
                                                                                          VeloPosition, \
                                                                                          Context

__RCSID__ = "$Id$"

global dataManager_
dataManager_ = BookkeepingDatabaseClient()
# dataManager_ = BookkeepingClient()

class XMLFilesReaderManager:
  """
  XMLFilesReaderManager class
  """
  #############################################################################
  def __init__( self ):
    """initialize the member of class"""
    self.jobReader_ = JobReader()
    self.replicaReader_ = ReplicaReader()

    # self.dataManager_ = BookkeepingDatabaseClient()
    self.dm_ = DataManager()
    self.fileTypeCache = {}


  #############################################################################
  @staticmethod
  def readFile( filename ):
    """reads an file content which format is XML"""
    try:
      stream = open( filename )
      doc = parse( stream )
      stream.close()


      docType = doc.doctype  # job or replica
      xmltype = docType.name.encode( 'ascii' )
    except NameError, ex:
      gLogger.error( "XML reading error", filename )
      return S_ERROR( ex )

    return xmltype, doc, filename

  #############################################################################
  def readXMLfromString( self, xmlString ):
    """read the xml string"""
    try:
      doc = parseString( xmlString )

      docType = doc.doctype  # job or replica
      xmltype = docType.name.encode( 'ascii' )

      if xmltype == 'Replicas':
        replica = self.replicaReader_.readReplica( doc, "IN Memory" )
        result = self.processReplicas( replica )
        del replica
        return result
      else:
        if xmltype == 'Job':
          job = self.jobReader_.readJob( doc, "IN Memory" )
          result = self.processJob( job )
          del job
          return result
        else:
          gLogger.error( "unknown XML file!!!" )
    except ExpatError, ex:
      gLogger.error( "XML reading error", ex )
      return S_ERROR( ex )


  #############################################################################
  def processJob( self, job ):
    """interprets the xml content"""
    gLogger.info( "Start Processing" )

    # #checking
    inputFiles = job.getJobInputFiles()
    for lfn in inputFiles:
      name = lfn.getFileName()
      result = dataManager_.checkfile( name )
      if result['OK']:
        fileID = long( result['Value'][0][0] )
        lfn.setFileID( fileID )
      else:
        errorMessage = "The file %s does not exist in the BKK database!!" % name
        return S_ERROR( errorMessage )

    outputFiles = job.getJobOutputFiles()
    dqvalue = None
    for outputfile in outputFiles:
      
      typeName = outputfile.getFileType()
      typeVersion = outputfile.getFileVersion()
      cahedTypeNameVersion = typeName + '<<' + typeVersion
      if cahedTypeNameVersion in self.fileTypeCache:
        gLogger.debug( cahedTypeNameVersion + ' in the cache!' )
        typeID = self.fileTypeCache[cahedTypeNameVersion]
        outputfile.setTypeID( typeID )
      else:
        result = dataManager_.checkFileTypeAndVersion( typeName, typeVersion )
        if not result['OK']:
          errorMessage = "The type:%s, version:%s is missing." % ( str( typeName ),
                                                                  str( typeVersion ) )
          return S_ERROR( errorMessage )
        else:
          gLogger.debug( cahedTypeNameVersion + " added to the cache!" )
          typeID = long( result['Value'] )
          outputfile.setTypeID( typeID )
          self.fileTypeCache[cahedTypeNameVersion] = typeID

      if job.getParam( 'JobType' ) and \
         job.getParam( 'JobType' ).getValue() == 'DQHISTOMERGING':  # all the merged histogram files have to be visible
        newFileParams = FileParam()
        newFileParams.setParamName( 'VisibilityFlag' )
        newFileParams.setParamValue( 'Y' )
        outputfile.addFileParam( newFileParams )
        gLogger.debug( 'The Merged histograms visibility flag has to be Y!' )

      params = outputfile.getFileParams()
      evtExists = False

      for param in params:
        paramName = param.getParamName()

        if paramName == "EventStat":
          if param.getParamValue() == '' and \
             outputfile.getFileType().upper().endswith( 'HIST' ):
            param.setParamValue( None )  # default value
          elif param.getParamValue() == '':
            return S_ERROR( 'EventStat value is null' )
          else:
            eventNb = long( param.getParamValue() )
            if eventNb < 0:
              return S_ERROR( "The event number not greater 0!" )

        if paramName == "FullStat":
          fullStat = long( param.getParamValue() )
          if fullStat <= 0:
            return S_ERROR( "The fullStat not greater 0!" )

        if paramName == "EventType":
          value = long( param.getParamValue() )
          result = dataManager_.checkEventType( value )
          if not result['OK']:
            errorMessage = "The event type %s is missing!" % ( str( value ) )
            return S_ERROR( errorMessage )

        gLogger.debug( 'EventTypeId checking!' )
        if paramName == "EventTypeId":
          gLogger.debug( 'ParamName:' + str( paramName ) )
          if param.getParamValue() != '':
            value = long( param.getParamValue() )
            result = dataManager_.checkEventType( value )
            if not result['OK']:
              errorMessage = "The event type %s is missing!" % ( str( value ) )
              return S_ERROR( errorMessage )
            evtExists = True

      if not evtExists and outputfile.getFileType() not in ['LOG']:
        inputFiles = job.getJobInputFiles()
        if len( inputFiles ) > 0:
          fileName = inputFiles[0].getFileName()
          res = dataManager_.getFileMetadata( [fileName] )
          if res['OK']:
            value = res['Value']
            if value[fileName].has_key( 'EventTypeId' ):
              if outputfile.exists( 'EventTypeId' ):
                param = outputfile.getParam( 'EventTypeId' )
                param.setParamValue( str( value[fileName]['EventTypeId'] ) )
              else:
                newFileParams = FileParam()
                newFileParams.setParamName( 'EventTypeId' )
                newFileParams.setParamValue( str( value[fileName]['EventTypeId'] ) )
                outputfile.addFileParam( newFileParams )
          else:
            return S_ERROR( res['Message'] )
        elif job.getOutputFileParam( 'EventTypeId' ) != None:
          param = job.getOutputFileParam( 'EventTypeId' )
          newFileParams = FileParam()
          newFileParams.setParamName( 'EventTypeId' )
          newFileParams.setParamValue( param.getParamValue() )
          outputfile.addFileParam( newFileParams )
        else:
          return S_ERROR( 'It can not fill the EventTypeId because there is no input files!' )

      infiles = job.getJobInputFiles()
      if not job.exists( 'RunNumber' ) and len( infiles ) > 0:
        runnumber = -1
        tck = -2
        runnumbers = []
        tcks = []
        for i in infiles:
          fileName = i.getFileName()
          retVal = dataManager_.getRunNbAndTck( fileName )

          if not retVal['OK']:
            return S_ERROR( retVal['Message'] )
          if len( retVal['Value'] ) > 0:
            gLogger.debug( 'RunTCK:' + str( retVal['Value'] ) )

            for i in retVal['Value']:
              if i[0] not in runnumbers:
                runnumbers += [i[0]]
              if i[1] not in tcks:
                tcks += [i[1]]

          if len( runnumbers ) > 1:
            gLogger.warn( 'Different runs are reconstructed: ' + str( runnumbers ) )
            runnumber = -1
          else:
            runnumber = runnumbers[0]

          if len( tcks ) > 1:
            gLogger.warn( 'Different TCKs are reconstructed: ' + str( tcks ) )
            tck = -2
          else:
            tck = tcks[0]

          gLogger.debug( 'The output files of the job inherits the following run: ' + str( runnumber ) )
          gLogger.debug( 'The output files of the job inherits the following TCK: ' + str( tck ) )

          if not job.exists( 'Tck' ):
            newJobParams = JobParameters()
            newJobParams.setName( 'Tck' )
            newJobParams.setValue( tck )
            job.addJobParams( newJobParams )

          if runnumber != None:
            prod = None
            newJobParams = JobParameters()
            newJobParams.setName( 'RunNumber' )
            newJobParams.setValue( str( runnumber ) )
            job.addJobParams( newJobParams )

            if job.getParam( 'JobType' ) and job.getParam( 'JobType' ).getValue() == 'DQHISTOMERGING':
              gLogger.debug( 'DQ merging!' )
              retVal = dataManager_.getJobInfo( fileName )
              if retVal['OK']:
                prod = retVal['Value'][0][18]
                newJobParams = JobParameters()
                newJobParams.setName( 'Production' )
                newJobParams.setValue( str( prod ) )
                job.addJobParams( newJobParams )
                gLogger.debug( 'Production inherited from input:' + str( prod ) )
            else:
              prod = job.getParam( 'Production' ).getValue()
              gLogger.debug( 'Production:' + str( prod ) )

            retVal = dataManager_.getProductionProcessingPassID( prod )
            if retVal['OK']:
              proc = retVal['Value']

              retVal = dataManager_.getRunAndProcessingPassDataQuality( runnumber, proc )
              if retVal['OK']:
                dqvalue = retVal['Value']
              else:
                dqvalue = None
                message = "The rundataquality table does not contains %d %s. Consequently, the Dq flag is inherited from the ancestor file!" % ( long( runnumber ), proc )
                gLogger.warn( message )
            else:
              dqvalue = None
              gLogger.warn( 'Bkk can not set the quality flag because the processing pass is missing for % d production (run number: %d )!' % ( long( prod ), long( runnumber ) ) )

    inputfiles = job.getJobInputFiles()

    sumEventInputStat = 0
    sumEvtStat = 0
    sumLuminosity = 0

    if job.exists( 'JobType' ):
      job.removeParam( 'JobType' )

    ### This must be replaced by a single call!!!!
    # ## It is not urgent as we do not have a huge load on the database
    for i in inputfiles:
      fname = i.getFileName()
      res = dataManager_.getJobInfo( fname )

      if res['OK']:
        value = res["Value"]
        if len( value ) > 0 and value[0][2] != None:
          sumEventInputStat += value[0][2]
      else:
        return S_ERROR( res['Message'] )
      res = dataManager_.getFileMetadata( [fname] )
      if res['OK']:
        value = res['Value']
        if value[fname]['EventStat'] != None:
          sumEvtStat += value[fname]['EventStat']
        if value[fname]['Luminosity'] != None:
          sumLuminosity += value[fname]['Luminosity']   
        dqvalue = value[fname].get( 'DataqualityFlag', value[fname].get( 'DQFlag', None ) )

    evtinput = 0
    if long( sumEvtStat ) > long( sumEventInputStat ):
      evtinput = sumEvtStat
    else:
      evtinput = sumEventInputStat

    if len( inputfiles ) > 0:
      if not job.exists( 'EventInputStat' ):
        newJobParams = JobParameters()
        newJobParams.setName( 'EventInputStat' )
        newJobParams.setValue( str( evtinput ) )
        job.addJobParams( newJobParams )
      else:
        currentEventInputStat = job.getParam( 'EventInputStat' )
        currentEventInputStat.setValue( evtinput )

    gLogger.debug( 'Luminosity: ' + str( sumLuminosity ) )
    outputFiles = job.getJobOutputFiles()
    for outputfile in outputFiles:
      if outputfile.getFileType() not in ['LOG'] and \
         sumLuminosity > 0 and not outputfile.exists( 'Luminosity' ):
        newFileParams = FileParam()
        newFileParams.setParamName( 'Luminosity' )
        newFileParams.setParamValue( sumLuminosity )
        outputfile.addFileParam( newFileParams )
        gLogger.debug( 'Luminosity added to ' + outputfile.getFileName() )
      ################

    config = job.getJobConfiguration()
    params = job.getJobParams()

    for param in params:
      if param.getName() == "RunNumber":
        value = long( param.getValue() )
        if value <= 0 and len( job.getJobInputFiles() ) == 0:
          # The files which inherits the runs can be entered to the database
          return S_ERROR( 'The run number not greater 0!' )

    result = self.__insertJob( job )

    if not result['OK']:
      errorMessage = "Unable to create Job : %s , %s, %s .\n Error: %s" % ( str( config.getConfigName() ),
                                                                           str( config.getConfigVersion() ),
                                                                           str( config.getDate() ),
                                                                           str( result['Message'] ) )
      return S_ERROR( errorMessage )
    else:
      jobID = long( result['Value'] )
      job.setJobId( jobID )
    inputFiles = job.getJobInputFiles()
    for inputfile in inputFiles:
      result = dataManager_.insertInputFile( job.getJobId(), inputfile.getFileID() )
      if not result['OK']:
        dataManager_.deleteJob( job.getJobId() )
        errorMessage = "Unable to add %s " % ( str( inputfile.getFileName() ) )
        return S_ERROR( errorMessage )

    outputFiles = job.getJobOutputFiles()
    prod = job.getParam( 'Production' ).getValue()
    retVal = dataManager_.getProductionOutputFileTypes( prod )
    if not retVal['OK']:
      return retVal
    outputFileTypes = retVal['Value']
    for outputfile in outputFiles:
      if dqvalue != None:
        newFileParams = FileParam()
        newFileParams.setParamName( 'QualityId' )
        newFileParams.setParamValue( dqvalue )
        outputfile.addFileParam( newFileParams )
      if not job.exists( 'RunNumber' ):  # if it is MC
        newFileParams = FileParam()
        newFileParams.setParamName( 'QualityId' )
        newFileParams.setParamValue( 'OK' )
        outputfile.addFileParam( newFileParams )
      ftype = outputfile.getFileType()
      if outputFileTypes.has_key( ftype ):
        vFileParams = FileParam()
        vFileParams.setParamName( 'VisibilityFlag' )
        vFileParams.setParamValue( outputFileTypes[ftype] )
        outputfile.addFileParam( vFileParams )
        gLogger.debug( 'The visibility flag is:' + outputFileTypes[ftype] )

      result = self.__insertOutputFiles( job, outputfile )
      if not result['OK']:
        dataManager_.deleteInputFiles( job.getJobId() )
        dataManager_.deleteJob( job.getJobId() )
        errorMessage = "Unable to create file %s ! ERROR: %s" % ( str( outputfile.getFileName() ),
                                                                 result["Message"] )
        return S_ERROR( errorMessage )
      else:
        fileid = long( result['Value'] )
        outputfile.setFileID( fileid )

      replicas = outputfile.getReplicas()
      for replica in replicas:
        params = replica.getaprams()
        for param in params:  # just one param exist in params list, because JobReader only one param add to Replica
          name = param.getName()
        result = dataManager_.updateReplicaRow( outputfile.getFileID(), 'No' )  # , name, location)
        if not result['OK']:
          errorMessage = "Unable to create Replica %s !" % ( str( name ) )
          return S_ERROR( errorMessage )

    if runnumber != None and runnumber > 0:
      retVal = dataManager_.insertRunStatus( runnumber, jobID, 'N' )
      if not retVal['OK']:
        gLogger.error( "Can not register the run status", retVal["Message"] )
    gLogger.info( "End Processing!" )

    return S_OK()

  @staticmethod
  def __insertJob( job ):
    """Inserts the job to the database"""
    config = job.getJobConfiguration()

    production = None

    condParams = job.getDataTakingCond()
    if condParams != None:
      datataking = condParams.getParams()
      config = job.getJobConfiguration()

      ver = config.getConfigVersion()  # online bug fix
      ver = ver.capitalize()
      config.setConfigVersion( ver )
      gLogger.debug( str( datataking ) )
      context = Context( datataking, config.getConfigName() )
      conditions = [BeamEnergyCondition(), VeloCondition(),
                    MagneticFieldCondition(), EcalCondition(),
                    HcalCondition(), HltCondition(),
                    ItCondition(), LoCondition(),
                    MuonCondition(), OtCondition(),
                    Rich1Condition(), Rich2Condition(),
                     Spd_prsCondition(), TtCondition(),
                     VeloPosition()]
      for condition in conditions:
        condition.interpret( context )

      gLogger.debug( context.getOutput() )
      datataking['Description'] = context.getOutput()

      res = dataManager_.getDataTakingCondDesc( datataking )
      dataTackingPeriodDesc = None
      if res['OK']:
        daqid = res['Value']
        if len( daqid ) != 0:  # exist in the database datataking
          dataTackingPeriodDesc = res['Value'][0][0]
          gLogger.debug( 'Data taking condition id', dataTackingPeriodDesc )
        else:
          res = dataManager_.insertDataTakingCond( datataking )
          if not res['OK']:
            return S_ERROR( "DATA TAKING Problem:" + str( res['Message'] ) )
          else:
            dataTackingPeriodDesc = datataking['Description']
            # The new data taking condition inserted. The name should be the generated name.
      else:
        # Note we allow to insert data quality tags when only the description is different.
        res = dataManager_.insertDataTakingCond( datataking )
        if not res['OK']:
          return S_ERROR( "DATA TAKING Problem:" + str( res['Message'] ) )
        else:
          dataTackingPeriodDesc = datataking['Description']
          # The new data taking condition inserted. The name should be the generated name.

      # insert processing pass
      programName = None
      programVersion = None
      conddb = None
      dddb = None
      found = False
      for param in job.getJobParams():
        if param.getName() == 'ProgramName':
          programName = param.getValue()
        elif param.getName() == 'ProgramVersion':
          programVersion = param.getValue()
        elif param.getName() == 'CondDB':
          conddb = param.getValue()
        elif param.getName() == 'DDDB':
          dddb = param.getValue()
        elif param.getName() == 'RunNumber':
          production = long( param.getValue() ) * -1
          found = True

      if job.exists( 'CondDB' ):
        job.removeParam( 'CondDB' )
      if job.exists( 'DDDB' ):
        job.removeParam( 'DDDB' )

      if not found:
        gLogger.error( 'Runn number is missing!' )
        return S_ERROR( 'Runn number is missing!' )

      retVal = dataManager_.getStepIdandNameForRUN( programName, programVersion, conddb, dddb )

      if not retVal['OK']:
        return S_ERROR( retVal['Message'] )
      stepid = retVal['Value'][0]
      steps = {'Steps':
               [{'StepId':stepid,
                 'StepName':retVal['Value'][1],
                 'ProcessingPass':retVal['Value'][1],
                 'Visible':'Y'}]}
      
      gLogger.debug( 'Pass_indexid', steps )
      gLogger.debug( 'Data taking', dataTackingPeriodDesc )
      gLogger.debug( 'production', production )
     
      newJobParams = JobParameters()
      newJobParams.setName( 'StepID' )
      newJobParams.setValue( str( stepid ) )
      job.addJobParams( newJobParams )
      
      message = "StepID for run: %s" % ( str( production ) )
      gLogger.info( message, stepid )

      res = dataManager_.addProduction( production,
                                       simcond = None,
                                       daq = dataTackingPeriodDesc,
                                       steps = steps['Steps'],
                                       inputproc = '' )

      if res['OK']:
        gLogger.info( "New processing pass has been created!" )
        gLogger.info( "New production is:", production )
      elif job.exists( 'RunNumber' ):
        gLogger.warn( 'The run already registered!' )
      else:
        retVal = dataManager_.deleteSetpContiner( production )
        if not retVal['OK']:
          return retVal
        gLogger.error( 'Unable to create processing pass!', res['Message'] )
        return S_ERROR( 'Unable to create processing pass!' )


    attrList = {'ConfigName':config.getConfigName(), \
                 'ConfigVersion':config.getConfigVersion(), \
                 'JobStart':None}

    for param in job.getJobParams():
      attrList[str( param.getName() )] = param.getValue()

    res = dataManager_.checkProcessingPassAndSimCond( attrList['Production'] )
    if not res['OK']:
      gLogger.error( 'check processing pass and simulation condition error', res['Message'] )
    else:
      value = res['Value']
      if value[0][0] == 0:
        errorMessage = "Missing processing pass and simulation conditions!\
        (Please fill it!) Production=%s" % ( str( attrList['Production'] ) )
        gLogger.warn( errorMessage )


    if attrList['JobStart'] == None:
      # date = config.getDate().split('-')
      # time = config.getTime().split(':')
      # dateAndTime = datetime.datetime(int(date[0]), int(date[1]), int(date[2]), int(time[0]), int(time[1]), 0, 0)
      attrList['JobStart'] = config.getDate() + ' ' + config.getTime()

    if production != None:  # for the online registration
      attrList['Production'] = production

    res = dataManager_.insertJob( attrList )

    if not res['OK'] and production < 0:
      retVal = dataManager_.deleteProductionsContiner( production )
      if not retVal['OK']:
        gLogger.error( retVal['Message'] )
    return res

  #############################################################################
  @staticmethod
  def __insertOutputFiles( job, outputfile ):
    """insert the files produced by a job"""
    attrList = {  'FileName':outputfile.getFileName(), \
                  'FileTypeId':outputfile.getTypeID(), \
                  'JobId':job.getJobId()}


    fileParams = outputfile.getFileParams()
    for param in fileParams:
      attrList[str( param.getParamName() )] = param.getParamValue()
    res = dataManager_.insertOutputFile( attrList )
    return res

  #############################################################################
  def processReplicas( self, replica ):
    """process the replica registration request"""
    outputfile = replica.getFileName()
    gLogger.info( "Processing replicas: " + str( outputfile ) )
    fileID = -1

    params = replica.getaprams()
    delete = True

    replicaFileName = ""
    for param in params:
      replicaFileName = param.getFile()
      location = param.getLocation()
      delete = param.getAction() == "Delete"

      result = dataManager_.checkfile( replicaFileName )
      if not result['OK']:
        message = "No replica can be "
        if delete:
          message += "removed"
        else:
          message += "added"
        message += " to file " + str( replicaFileName ) + " for " + str( location ) + ".\n"
        return S_ERROR( message )
      else:
        fileID = long( result['Value'][0][0] )
        gLogger.info( fileID )

      if delete:
        result = self.dm_.getReplicas( replicaFileName )
        replicaList = result['Value']['Successful']
        if len( replicaList ) == 0:
          result = dataManager_.updateReplicaRow( fileID, "No" )
          if not result['OK']:
            gLogger.warn( "Unable to set the Got_Replica flag for " + str( replicaFileName ) )
            return S_ERROR( "Unable to set the Got_Replica flag for " + str( replicaFileName ) )
      else:
        result = dataManager_.updateReplicaRow( fileID, "Yes" )
        if not result['OK']:
          return S_ERROR( "Unable to set the Got_Replica flag for " + str( replicaFileName ) )


    gLogger.info( "End Processing replicas!" )

    return S_OK()

