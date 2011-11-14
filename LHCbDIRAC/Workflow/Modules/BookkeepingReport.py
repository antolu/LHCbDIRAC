########################################################################
# $Id$
########################################################################
"""Bookkeeping Report class"""

__RCSID__ = "$Id$"

from DIRAC                                   import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.Utilities.Subprocess         import shellCall
from DIRAC.Resources.Catalog.PoolXMLFile     import getGUID, getType

from LHCbDIRAC.Core.Utilities.ProductionData import constructProductionLFNs
from LHCbDIRAC.Workflow.Modules.ModuleBase   import ModuleBase

from xml.dom.minidom                         import Document, DocumentType

import DIRAC
import os, time, re, socket

class BookkeepingReport( ModuleBase ):

  def __init__( self ):
    #TODO check which variables are really needed here
    '''
    Dunno why there are so maaaaaany variables here. Probably not all are
    really needed at this level...
    '''

    self.log = gLogger.getSubLogger( "BookkeepingReport" )

    super( BookkeepingReport, self ).__init__( self.log )

    self.version = __RCSID__

    self.configName = ''
    self.configVersion = ''
    self.run_number = 0
    self.firstEventNumber = 1
    self.numberOfEvents = ''
    self.numberOfEventsInput = ''
    self.numberOfEventsOutput = ''
    self.simDescription = 'NoSimConditions'
    self.eventType = ''
    self.poolXMLCatName = ''
    self.inputData = ''
    self.InputData = ''
    self.sourceData = ''
    self.applicationName = ''
    self.applicationLog = ''
    self.firstStepInput = ''
    self.jobType = ''
    self.listoutput = []

    self.ldate = None
    self.ltime = None
    self.ldatestart = None
    self.ltimestart = None

  ################################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None, saveOnFile = True ):

    try:

      super( BookkeepingReport, self ).execute( self.version, production_id, prod_job_id, wms_job_id,
                                               workflowStatus, stepStatus,
                                               wf_commons, step_commons, step_number, step_id )

      if not self._checkWFAndStepStatus():
        return S_OK()

      result = self._resolveInputVariables()
      if not result['OK']:
        self.log.error( result['Message'] )
        return result

      doc = self.__makeBookkeepingXML()

      if saveOnFile:
        self.root = gConfig.getValue( '/LocalSite/Root', os.getcwd() )
        bfilename = 'bookkeeping_' + self.step_id + '.xml'
        #bfilename = '%s_Bookkeeping_Step%d.xml' % ( self.applicationName, self.step_number )
        #bfilename = '%s_%s_Bookkeeping.xml' % ( self.step_id, self.applicationName )
        bfile = open( bfilename, 'w' )
        print >> bfile, doc
        bfile.close()
      else:
        print doc

      return S_OK()

    except Exception, e:
      self.log.exception( e )
      return S_ERROR( e )

    finally:
      super( BookkeepingReport, self ).finalize( self.version )

################################################################################
# AUXILIAR FUNCTIONS
################################################################################

  def _resolveInputVariables( self ):

    super( BookkeepingReport, self )._resolveInputVariables()

    ## VARS FROM WORKFLOW_COMMONS ##

    wf_vars = {
               'FirstStepInputEvents' : 'firstStepInput',
               'sourceData'           : 'sourceData'    ,
               'simDescription'       : 'simDescription',
               'poolXMLCatName'       : 'poolXMLCatName',
               'InputData'            : 'InputData'     ,
               'JobType'              : 'jobType'
                }

    for k, v in wf_vars.items():
      if self.workflow_commons.has_key( k ):
        setattr( self, v, self.workflow_commons[ k ] )

    if self.workflow_commons.has_key( 'outputList' ):
      for outputItem in self.listoutput:
        if outputItem not in self.workflow_commons['outputList']:
          self.workflow_commons['outputList'].append( outputItem )
    else:
      self.workflow_commons['outputList'] = self.listoutput

    if self.workflow_commons.has_key( 'BookkeepingLFNs' ) and \
        self.workflow_commons.has_key( 'LogFilePath' )    and \
        self.workflow_commons.has_key( 'ProductionOutputData' ):

      self.logFilePath = self.workflow_commons['LogFilePath']
      self.bkLFNs = self.workflow_commons['BookkeepingLFNs']
      self.prodOutputLFNs = self.workflow_commons['ProductionOutputData']

      if not type( self.bkLFNs ) == type( [] ):
        self.bkLFNs = [i.strip() for i in self.bkLFNs.split( ';' )]
      if not type( self.prodOutputLFNs ) == type( [] ):
        self.prodOutputLFNs = [i.strip() for i in self.prodOutputLFNs.split( ';' )]

    else:
      self.log.info( 'LogFilePath / BookkeepingLFNs parameters not found, creating on the fly' )
      result = constructProductionLFNs( self.workflow_commons )
      if not result['OK']:
        self.log.error( 'Could not create production LFNs', result['Message'] )
        return result
      self.bkLFNs = result['Value']['BookkeepingLFNs']
      self.logFilePath = result['Value']['LogFilePath'][0]
      self.prodOutputLFNs = result['Value']['ProductionOutputData']

    ## VARS FROM STEP_COMMONS ##

    step_vars = {
                 'eventType'            : 'eventType'           ,
                 'numberOfEvents'       : 'numberOfEvents'      ,
                 'numberOfEventsOutput' : 'numberOfEventsOutput',
                 'inputData'            : 'inputData'           ,
                 'listoutput'           : 'listoutput'
                 }

    for k, v in step_vars.items():
      if self.step_commons.has_key( k ):
        setattr( self, v, self.step_commons[ k ] )

    if self.step_commons.has_key( 'applicationName' ):
      self.applicationName = self.step_commons['applicationName']
      self.applicationVersion = self.step_commons['applicationVersion']
      self.applicationLog = self.step_commons['applicationLog']

    self.ldate = time.strftime( "%Y-%m-%d", time.localtime( time.time() ) )
    self.ltime = time.strftime( "%H:%M", time.localtime( time.time() ) )

    if self.step_commons.has_key( 'StartTime' ):
      startTime = self.step_commons['StartTime']
      self.ldatestart = time.strftime( "%Y-%m-%d", time.localtime( startTime ) )
      self.ltimestart = time.strftime( "%H:%M", time.localtime( startTime ) )

    return S_OK()

################################################################################
################################################################################

  def __makeBookkeepingXML( self ):

    ''' Bookkeeping xml looks like this:
    
        <Job ConfigName="" ConfigVersion="" Date="" Time="">
          <TypedParameter Name="" Type="" Value=""/>
          ...
          <InputFile Name=""/>
          ...
          <OutputFile Name="" TypeName="" TypeVersion="">
            <Parameter Name="" Value=""/>
            ...
            <Replica Location="" Name=""/>
            ....
          </OutputFile>
          ...
          <SimulationCondition>
            <Parameter Name="" Value=""/>
          </SimulationCondition>
        </Job>  

    '''
    # Generate XML document
    doc = Document()
    docType = DocumentType( "Job" )
    docType.systemId = "book.dtd"
    doc.appendChild( docType )

    # Generate JobNode
    doc, jobNode = self.__generateJobNode( doc )
    # Generate TypedParams
    jobNode = self.__generateTypedParams( jobNode )
    # Generate InputFiles
    jobNode = self.__generateInputFiles( jobNode )
    # Generate OutputFiles
    jobNode = self.__generateOutputFiles( jobNode )
    # Generate SimulationConditions
    jobNode = self.__generateSimulationCondition( jobNode )

    return doc.toprettyxml( indent = "    ", encoding = "ISO-8859-1" )

################################################################################

  def __generateJobNode( self, doc ):
    ''' Node looks like
        <Job ConfigName="" ConfigVersion="" Date="" Time="">
    '''

    # Get the Config name from the environment if any
    if self.workflow_commons.has_key( 'configName' ):
      configName = self.workflow_commons[ 'configName' ]
      configVersion = self.workflow_commons[ 'configVersion' ]
    else:
      configName = self.applicationName
      configVersion = self.applicationVersion

    jobAttributes = ( configName, configVersion, self.ldate, self.ltime )

    return self.__addChildNode( doc, "Job", 1, *jobAttributes )

################################################################################

  def __generateTypedParams( self, jobNode ):
    ''' TypedParameter looks like
        <TypedParameter Name="" Type="" Value="">
        
        List of possible TypedParameter names
        - CPUTIME
        - ExecTime
        - WNMODEL
        - WNMEMORY
        - WNCPUPOWER
        - WNCACHE
        - WNCPUHS06
        - Production
        - DiracJobId
        - Name
        - JobStart
        - JobEnd
        - Location
        - JobType
        - WorkerNode
        - GeometryVersion
        - ProgramName
        - ProgramVersion
        - DiracVersion
        - FirstEventNumber
        - StatisticsRequested
        - NumberOfEvents
    '''

    typedParams = []

    # Timing
    exectime = 0
    if self.step_commons.has_key( 'StartTime' ):
      exectime = time.time() - self.step_commons['StartTime']
    cputime = 0
    if self.step_commons.has_key( 'StartStats' ):
      stats = os.times()
      cputime = stats[ 0 ] + stats[ 2 ] - self.step_commons[ 'StartStats' ][ 0 ] - self.step_commons[ 'StartStats' ][ 2 ]

    typedParams.append( ( "CPUTIME", cputime ) )
    typedParams.append( ( "ExecTime", exectime ) )

    nodeInfo = self.__getNodeInformation()
    if nodeInfo['OK']:

      typedParams.append( ( "WNMODEL", nodeInfo[ "ModelName" ] ) )
      typedParams.append( ( "WNMEMORY", nodeInfo[ "Memory(kB)" ] ) )
      typedParams.append( ( "WNCPUPOWER", nodeInfo[ "CPU(MHz)" ] ) )
      typedParams.append( ( "WNCACHE", nodeInfo[ "CacheSize(kB)" ] ) )

    tempVar = gConfig.getValue( "/LocalSite/CPUNormalizationFactor", "1" )
    typedParams.append( ( "WNCPUHS06", tempVar ) )
    typedParams.append( ( "Production", self.production_id ) )
    typedParams.append( ( "DiracJobId", self.jobID ) )
    typedParams.append( ( "Name", self.step_id ) )
    typedParams.append( ( "JobStart", '%s %s' % ( self.ldatestart, self.ltimestart ) ) )
    typedParams.append( ( "JobEnd", '%s %s' % ( self.ldate, self.ltime ) ) )
    typedParams.append( ( "Location", DIRAC.siteName() ) )
    typedParams.append( ( "JobType", self.jobType ) )

    host = None
    if os.environ.has_key( "HOSTNAME" ):
      host = os.environ[ "HOSTNAME" ]
    elif os.environ.has_key( "HOST" ):
      host = os.environ[ "HOST" ]
    if host is not None:
      typedParams.append( ( "WorkerNode", host ) )

    if os.environ.has_key( 'XMLDDDB_VERSION' ):
      typedParams.append( ( "GeometryVersion", os.environ[ "XMLDDDB_VERSION" ] ) )

    typedParams.append( ( "ProgramName", self.applicationName ) )
    typedParams.append( ( "ProgramVersion", self.applicationVersion ) )

    # DIRAC version
    tempVar = "v%dr%dp%d" % ( DIRAC.majorVersion, DIRAC.minorVersion, DIRAC.patchLevel )
    typedParams.append( ( "DiracVersion", tempVar ) )

    if self.firstEventNumber != None and self.firstEventNumber != '':
      typedParams.append( ( "FirstEventNumber", self.firstEventNumber ) )
    else:
      typedParams.append( ( "FirstEventNumber", 1 ) )

    if self.numberOfEvents != None and self.numberOfEvents != '':
      typedParams.append( ( "StatisticsRequested", self.numberOfEvents ) )

    if self.numberOfEventsInput != None and self.numberOfEventsInput != '':
      typedParams.append( ( "NumberOfEvents", self.numberOfEventsInput ) )
    else:
      typedParams.append( ( "NumberOfEvents", self.numberOfEvents ) )

    # Add TypedParameters to the XML file
    for typedParam in typedParams:
      jobNode = self.__addChildNode( jobNode, "TypedParameter", 0, *typedParam )

    return jobNode

################################################################################

  def __generateInputFiles( self, jobNode ):
    ''' InputData looks like this
        <InputFile Name=""/>
    '''

    if self.inputData:
      intermediateInputs = False
      for inputname in self.inputData.split( ';' ):
        for bkLFN in self.bkLFNs:
          if os.path.basename( bkLFN ) == os.path.basename( inputname ):
            jobNode = self.__addChildNode( jobNode, "InputFile", 0, bkLFN )
            intermediateInputs = True
        if not intermediateInputs:
          jobNode = self.__addChildNode( jobNode, "InputFile", 0, inputname.replace( 'LFN:', '' ) )

    return jobNode

################################################################################

  def __generateOutputFiles( self, jobNode ):
    '''OutputFile looks like this:  
      
       <OutputFile Name="" TypeName="" TypeVersion="">
         <Parameter Name="" Value=""/>
         ...
         <Replica Location="" Name=""/>
         ....
       </OutputFile>
      
      What this exactly does, it is a mystery to me.
    '''


    dataTypes = gConfig.getValue( '/Operations/Bookkeeping/FileTypes', [] )
    gLogger.info( 'DataTypes retrieved from /Operations/Bookkeeping/FileTypes are:\n%s' % ( ', '.join( dataTypes ) ) )

    ####################################################################
    # Output files
    # Define DATA TYPES - ugly! should find another way to do that

    statistics = "0"

    if self.eventType != None:
      eventtype = self.eventType
    else:
      self.log.warn( 'BookkeepingReport: no eventType specified' )
      eventtype = 'Unknown'
    self.log.info( 'Event type = %s' % ( str( self.eventType ) ) )
    self.log.info( 'stats = %s' % ( str( self.numberOfEventsOutput ) ) )

    if self.numberOfEventsOutput != '':
      statistics = self.numberOfEventsOutput
    elif self.numberOfEventsInput != '':
      statistics = self.numberOfEventsInput
    elif self.numberOfEvents != '':
      statistics = self.numberOfEvents
    else:
      self.log.warn( 'BookkeepingReport: no numberOfEvents specified' )
      statistics = "0"

    outputs = []
    count = 0
    bkTypeDict = {}
    while ( count < len( self.listoutput ) ):
      if self.listoutput[count].has_key( 'outputDataName' ):
        outputs.append( ( ( self.listoutput[ count ][ 'outputDataName' ] ),
                          ( self.listoutput[ count ][ 'outputDataSE' ] ), ( self.listoutput[ count ][ 'outputDataType' ] ) ) )
      if self.listoutput[ count ].has_key( 'outputBKType' ):
        bkTypeDict[ self.listoutput[ count ][ 'outputDataName' ]] = self.listoutput[ count ][ 'outputBKType' ]
      count = count + 1
    outputs.append( ( ( self.applicationLog ), ( 'LogSE' ), ( 'LOG' ) ) )
    self.log.info( outputs )
    if type( self.logFilePath ) == type( [] ):
      self.logFilePath = self.logFilePath[ 0 ]

    for output, outputse, outputtype in list( outputs ):
      self.log.info( 'Looking at output %s %s %s' % ( output, outputse, outputtype ) )
      typeName = outputtype.upper()
      typeVersion = '1'
      fileStats = statistics
      if bkTypeDict.has_key( output ):
        typeV = getType( output )
        typeVersion = ''
        if not typeV['OK']:
          self.log.error( 'Could not find Type for %s with message' % ( output ), typeV['Message'] )
          raise NameError, 'No Type in XML catalog'
        else:
          typeVersion = typeV['Value'][output]
          self.log.info( 'Setting POOL XML catalog type for %s to %s' % ( output, typeVersion ) )
        typeName = bkTypeDict[ output ].upper()
        self.log.info( 'Setting explicit BK type version for %s to %s and file type to %s' % ( output, typeVersion, typeName ) )
        if self.workflow_commons.has_key( 'StreamEvents' ):
          streamEvents = self.workflow_commons['StreamEvents']
          if streamEvents.has_key( typeName ):
            fileStats = streamEvents[typeName]
            self.log.info( 'Found explicit number of events = %s for file %s, type %s' % ( fileStats, output, typeName ) )

      if not os.path.exists( output ):
        self.log.error( 'File does not exist:' , output )
        continue
      # Output file size
      if not self.step_commons.has_key( 'size' ) or output not in self.step_commons[ 'size' ]:
        try:
          outputsize = str( os.path.getsize( output ) )
        except:
          outputsize = '0'
      else:
        outputsize = self.step_commons[ 'size' ][ output ]

      if not self.step_commons.has_key( 'md5' ) or output not in self.step_commons[ 'md5' ]:
        comm = 'md5sum ' + str( output )
        resultTuple = shellCall( 0, comm )
        status = resultTuple[ 'Value' ][ 0 ]
        out = resultTuple[ 'Value' ][ 1 ]
      else:
        status = 0
        out = self.step_commons[ 'md5' ][ output ]

      if status:
        self.log.info( "Failed to get md5sum of %s" % str( output ) )
        self.log.info( str( out ) )
        md5sum = '000000000000000000000000000000000000'
      else:
        md5sum = out.split()[ 0 ]

      if not self.step_commons.has_key( 'guid' ) or output not in self.step_commons[ 'guid' ]:
        guidResult = getGUID( output )
        guid = ''
        if not guidResult[ 'OK' ]:
          self.log.error( 'Could not find GUID for %s with message' % ( output ), guidResult[ 'Message' ] )
        elif guidResult[ 'generated' ]:
          self.log.warn( 'PoolXMLFile generated GUID(s) for the following files ', ', '.join( guidResult[ 'generated' ] ) )
          guid = guidResult[ 'Value' ][ output ]
        else:
          guid = guidResult[ 'Value' ][ output ]
          self.log.info( 'Setting POOL XML catalog GUID for %s to %s' % ( output, guid ) )
      else:
        guid = self.step_commons[ 'guid' ][ output ]

      if not guid:
        raise NameError, 'No GUID found for %s' % output

      # find the constructed lfn
      lfn = ''
      if not re.search( '.log$', output ):
        for outputLFN in self.bkLFNs:
          if os.path.basename( outputLFN ) == output:
            lfn = outputLFN
        if not lfn:
          raise NameError, 'Could not find LFN for %s' % output
      else:
        lfn = '%s/%s' % ( self.logFilePath, self.applicationLog )

      #TODO: Fix for histograms: probably not needed anymore
      oldTypeName = None
      if typeName.upper() == 'HIST':
        typeVersion = '0'
        oldTypeName = typeName
        typeName = '%sHIST' % ( self.applicationName.upper() )

      # Add Output to the XML file
      oFileAttributes = ( lfn, typeName, typeVersion )
      jobNode, oFile = self.__addChildNode( jobNode, "OutputFile", 1, *oFileAttributes )

      #HIST is in the dataTypes e.g. we may have new names in the future ;)
      if oldTypeName:
        typeName = oldTypeName

      if typeName in dataTypes:
        oFile = self.__addChildNode( oFile, "Parameter", 0, *( "EventTypeId", eventtype ) )
        oFile = self.__addChildNode( oFile, "Parameter", 0, *( "EventStat", str( fileStats ) ) )

      if typeName in dataTypes or 'HIST' in typeName:
        oFile = self.__addChildNode( oFile, "Parameter", 0, *( "FileSize", outputsize ) )

      ############################################################
      # Log file replica information
#      if typeName == "LOG":
      if self.applicationLog != None:
          logfile = self.applicationLog
          if logfile == output:
            logurl = 'http://lhcb-logs.cern.ch/storage'
            url = logurl + self.logFilePath + '/' + self.applicationLog
            oFile = self.__addChildNode( oFile, "Replica", 0, url )

      oFile = self.__addChildNode( oFile, "Parameter", 0, *( "MD5Sum", md5sum ) )
      oFile = self.__addChildNode( oFile, "Parameter", 0, *( "Guid", guid ) )

    return jobNode

################################################################################        

  def __generateSimulationCondition( self, jobNode ):
    '''SimulationCondition looks like this:
       <SimulationCondition>
         <Parameter Name="" Value=""/>
       </SimulationCondition>
    '''
    if self.applicationName == "Gauss":
      jobNode, sim = self.__addChildNode( jobNode, "SimulationCondition", 1 )
      sim = self.__addChildNode( sim, "Parameter", 0, *( "SimDescription", self.simDescription ) )

    return jobNode

################################################################################

  def __getNodeInformation( self ):
    """Try to obtain system HostName, CPU, Model, cache and memory.  This information
       is not essential to the running of the jobs but will be reported if
       available.
    """
    result = S_OK()
    try:
      result[ "HostName"  ] = socket.gethostname()

      file = open ( "/proc/cpuinfo", "r" )
      info = file.readlines()
      file.close()
#      result[ "CPU(MHz)"  ]     = string.replace( string.replace( string.split( info[6], ":" )[1], " ", "" ), "\n", "" )
      result[ "CPU(MHz)"  ] = info[6].split( ":" )[1].replace( " ", "" ).replace( "\n", "" )
#      result[ "ModelName" ]     = string.replace( string.replace( string.split( info[4], ":" )[1], " ", "" ), "\n", "" )
      result[ "ModelName" ] = info[4].split( ":" )[1].replace( " ", "" ).replace( "\n", "" )
#      result[ "CacheSize(kB)" ] = string.replace( string.replace( string.split( info[7], ":" )[1], " ", "" ), "\n", "" )
      result[ "CacheSize(kB)" ] = info[7].split( ":" )[1].replace( " ", "" ).replace( "\n", "" )

      file = open ( "/proc/meminfo", "r" )
      info = file.readlines()
      file.close()
#      result["Memory(kB)"] = string.replace( string.replace( string.split( info[3], ":" )[1], " ", "" ), "\n", "" )
      result["Memory(kB)"] = info[3].split( ":" )[1].replace( " ", "" ).replace( "\n", "" )
    except Exception, x:
      self.log.fatal( 'BookkeepingReport failed to obtain node information with Exception:' )
      self.log.fatal( str( x ) )
      result = S_ERROR()
      result['Message'] = 'Failed to obtain system information for ' + self.systemFlag
      return result

    return result

################################################################################
# END AUXILIAR FUNCTIONS
################################################################################

################################################################################
# XML GENERATION FUNCTION
################################################################################

  def __addChildNode( self, parentNode, tag, returnChildren, *args ):
    '''
    Params
      :parentNode:
        node where the new node is going to be appended
      :tag: 
        name if the XML element to be created
      :returnChildren:
        flag to return or not the children node, used to avoid unused variables
      :*args:
        possible attributes of the element 
    '''

    ALLOWED_TAGS = [ 'Job', 'TypedParameter', 'InputFile', 'OutputFile',
                     'Parameter', 'Replica', 'SimulationCondition' ]

    def genJobDict( configName, configVersion, ldate, ltime ):
      return {
              "ConfigName"   : configName,
              "ConfigVersion": configVersion,
              "Date"         : ldate,
              "Time"         : ltime
             }
    def genTypedParameterDict( name, value, type = "Info" ):
      return {
              "Name"  : name,
              "Value" : value,
              "Type"  : type
              }
    def genInputFileDict( name ):
      return {
              "Name" : name
              }
    def genOutputFileDict( name, typeName, typeVersion ):
      return {
              "Name"        : name,
              "TypeName"    : typeName,
              "TypeVersion" : typeVersion
              }
    def genParameterDict( name, value ):
      return {
              "Name"  : name,
              "Value" : value
              }
    def genReplicaDict( name, location = "Web" ):
      return {
              "Name"     : name,
              "Location" : location
              }
    def genSimulationConditionDict():
      return {}

    if not tag in ALLOWED_TAGS:
      # We can also return S_ERROR, but this let's the job keep running.
      dict = {}
    else:
      dict = locals()[ 'gen%sDict' % tag ]( *args )

    childNode = Document().createElement( tag )
    for k, v in dict.items():
      childNode.setAttribute( k, str( v ) )
    parentNode.appendChild( childNode )

    if returnChildren:
      return ( parentNode, childNode )
    return parentNode

################################################################################
# END XML GENERATION FUNCTIONS
################################################################################

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
