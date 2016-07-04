"""  Bookkeeping Reporting module (just prepare the files, do not send them
    (which is done in the uploadOutput)
"""

import os
import time
import re
import socket
from xml.dom.minidom import Document, DocumentType

import DIRAC

from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Resources.Catalog.PoolXMLFile import getGUID
from DIRAC.Workflow.Utilities.Utils import getStepCPUTimes

from LHCbDIRAC.Resources.Catalog.PoolXMLFile import getOutputType
from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from LHCbDIRAC.Core.Utilities.ProductionData import constructProductionLFNs
from LHCbDIRAC.Core.Utilities.XMLSummaries import XMLSummary, XMLSummaryError
from LHCbDIRAC.Core.Utilities.XMLTreeParser import addChildNode

__RCSID__ = "$Id$"

class BookkeepingReport( ModuleBase ):
  """ BookkeepingReport class
  """

  def __init__( self, bkClient = None, dm = None ):
    """ Usual c'tor
    """

    self.log = gLogger.getSubLogger( "BookkeepingReport" )

    super( BookkeepingReport, self ).__init__( self.log, bkClientIn = bkClient, dm = dm )

    self.version = __RCSID__

    self.simDescription = 'NoSimConditions'
    self.eventType = ''
    self.poolXMLCatName = ''
    self.stepInputData = []
    self.applicationName = ''
    self.applicationLog = ''
    self.firstStepInput = ''
    self.jobType = ''
    self.stepOutputs = []
    self.histogram = False
    self.xf_o = None

    self.ldate = None
    self.ltime = None
    self.ldatestart = None
    self.ltimestart = None

  ################################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None, saveOnFile = True ):
    """ Usual executor
    """

    try:

      super( BookkeepingReport, self ).execute( self.version, production_id, prod_job_id, wms_job_id,
                                                workflowStatus, stepStatus,
                                                wf_commons, step_commons, step_number, step_id )

      if not self._checkWFAndStepStatus():
        return S_OK()

      bkLFNs, logFilePath = self._resolveInputVariables()

      doc = self.__makeBookkeepingXML( bkLFNs, logFilePath )

      if saveOnFile:
        bfilename = 'bookkeeping_' + self.step_id + '.xml'
        with open( bfilename, 'w' ) as bfile:
          print >> bfile, doc
      else:
        print doc

      return S_OK()

    except Exception as e:
      self.log.exception( e )
      return S_ERROR( e )

    finally:
      super( BookkeepingReport, self ).finalize( self.version )

################################################################################
# AUXILIAR FUNCTIONS
################################################################################

  def _resolveInputVariables( self ):
    """ By convention the module parameters are resolved here.
    """

    super( BookkeepingReport, self )._resolveInputVariables()
    super( BookkeepingReport, self )._resolveInputStep()

    self.stepOutputs, _sot, _hist = self._determineOutputs()

    # # VARS FROM WORKFLOW_COMMONS ##

    if self.workflow_commons.has_key( 'outputList' ):
      for outputItem in self.stepOutputs:
        if outputItem not in self.workflow_commons['outputList']:
          self.workflow_commons['outputList'].append( outputItem )
    else:
      self.workflow_commons['outputList'] = self.stepOutputs

    if self.workflow_commons.has_key( 'BookkeepingLFNs' ) and \
        self.workflow_commons.has_key( 'LogFilePath' )    and \
        self.workflow_commons.has_key( 'ProductionOutputData' ):

      logFilePath = self.workflow_commons['LogFilePath']
      bkLFNs = self.workflow_commons['BookkeepingLFNs']

      if not isinstance( bkLFNs, list ):
        bkLFNs = [i.strip() for i in bkLFNs.split( ';' )]

    else:
      self.log.info( 'LogFilePath / BookkeepingLFNs parameters not found, creating on the fly' )
      result = constructProductionLFNs( self.workflow_commons, self.bkClient )
      if not result['OK']:
        self.log.error( 'Could not create production LFNs', result['Message'] )
        raise ValueError( result['Message'] )

      bkLFNs = result['Value']['BookkeepingLFNs']
      logFilePath = result['Value']['LogFilePath'][0]

    self.ldate = time.strftime( "%Y-%m-%d", time.localtime( time.time() ) )
    self.ltime = time.strftime( "%H:%M", time.localtime( time.time() ) )

    if self.step_commons.has_key( 'StartTime' ):
      startTime = self.step_commons['StartTime']
      self.ldatestart = time.strftime( "%Y-%m-%d", time.localtime( startTime ) )
      self.ltimestart = time.strftime( "%H:%M", time.localtime( startTime ) )

    try:
      self.xf_o = self.step_commons['XMLSummary_o']
    except KeyError:
      self.log.warn( 'XML Summary object not found, will try to create it (again?)' )
      try:
        xmlSummaryFile = self.step_commons['XMLSummary']
      except KeyError:
        self.log.warn( 'XML Summary file name not found, will try to guess it' )
        xmlSummaryFile = 'summary%s_%s_%s_%s.xml' % ( self.applicationName,
                                                      self.production_id,
                                                      self.prod_job_id,
                                                      self.step_number )
        self.log.warn( 'Trying %s' % xmlSummaryFile )
        if not xmlSummaryFile in os.listdir( '.' ):
          self.log.warn( 'XML Summary file %s not found, will try to guess a second time' % xmlSummaryFile )
          xmlSummaryFile = 'summary%s_%s.xml' % ( self.applicationName,
                                                  self.step_id )
          self.log.warn( 'Trying %s' % xmlSummaryFile )
          if not xmlSummaryFile in os.listdir( '.' ):
            self.log.warn( 'XML Summary file %s not found, will try to guess a third and last time' % xmlSummaryFile )
            xmlSummaryFile = 'summary%s_%s.xml' % ( self.applicationName,
                                                    self.step_number )
            self.log.warn( 'Trying %s' % xmlSummaryFile )
      try:
        self.xf_o = XMLSummary( xmlSummaryFile )
      except XMLSummaryError as e:
	self.log.warn( 'No XML summary available: %s' % repr( e ) )
        self.xf_o = None

    return bkLFNs, logFilePath

################################################################################
################################################################################

  def __makeBookkeepingXML( self, bkLFNs, logFilePath ):

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
    jobNode = self.__generateInputFiles( jobNode, bkLFNs )
    # Generate OutputFiles
    jobNode = self.__generateOutputFiles( jobNode, bkLFNs, logFilePath )
    # Generate SimulationConditions
    jobNode = self.__generateSimulationCondition( jobNode )

    prettyXMLDoc = doc.toprettyxml( indent = "    ", encoding = "ISO-8859-1" )

    # horrible, necessary hack!
    prettyXMLDoc = prettyXMLDoc.replace( '\'book.dtd\'', '\"book.dtd\"' )

    return prettyXMLDoc

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

    return addChildNode( doc, "Job", 1, jobAttributes )

################################################################################

  def __generateTypedParams( self, jobNode ):
    """ TypedParameter looks like
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
        - StepID
    """

    typedParams = []

    # Timing
    exectime, cputime = getStepCPUTimes( self.step_commons )

    typedParams.append( ( "CPUTIME", cputime ) )
    typedParams.append( ( "ExecTime", exectime ) )

    nodeInfo = self.__getNodeInformation()
    if nodeInfo['OK']:

      typedParams.append( ( "WNMODEL", nodeInfo[ "ModelName" ] ) )
      typedParams.append( ( "WNCPUPOWER", nodeInfo[ "CPU(MHz)" ] ) )
      typedParams.append( ( "WNCACHE", nodeInfo[ "CacheSize(kB)" ] ) )
      # THIS OR THE NEXT
      # typedParams.append( ( "WorkerNode", nodeInfo['HostName'] ) )

    host = None
    if os.environ.has_key( "HOSTNAME" ):
      host = os.environ[ "HOSTNAME" ]
    elif os.environ.has_key( "HOST" ):
      host = os.environ[ "HOST" ]
    if host is not None:
      typedParams.append( ( "WorkerNode", host ) )

    try:
      memory = self.xf_o.memory
    except AttributeError:
      memory = nodeInfo[ "Memory(kB)" ]

    typedParams.append( ( "WNMEMORY", memory ) )

    tempVar = gConfig.getValue( "/LocalSite/CPUNormalizationFactor", "1" )
    typedParams.append( ( "WNCPUHS06", tempVar ) )
    tempVar = gConfig.getValue( "/LocalSite/CPUScalingFactor", "1" )
    typedParams.append( ( "WNMJFHS06", tempVar ) )
    typedParams.append( ( "Production", self.production_id ) )
    typedParams.append( ( "DiracJobId", str( self.jobID ) ) )
    typedParams.append( ( "Name", self.step_id ) )
    typedParams.append( ( "JobStart", '%s %s' % ( self.ldatestart, self.ltimestart ) ) )
    typedParams.append( ( "JobEnd", '%s %s' % ( self.ldate, self.ltime ) ) )
    typedParams.append( ( "Location", self.siteName ) )
    typedParams.append( ( "JobType", self.jobType ) )

    if os.environ.has_key( 'XMLDDDB_VERSION' ):
      typedParams.append( ( "GeometryVersion", os.environ[ "XMLDDDB_VERSION" ] ) )

    typedParams.append( ( "ProgramName", self.applicationName ) )
    typedParams.append( ( "ProgramVersion", self.applicationVersion ) )

    # DIRAC version
    tempVar = "v%dr%dp%d" % ( DIRAC.majorVersion, DIRAC.minorVersion, DIRAC.patchLevel )
    typedParams.append( ( "DiracVersion", tempVar ) )

    typedParams.append( ( "FirstEventNumber", 1 ) )

    typedParams.append( ( "StatisticsRequested", self.numberOfEvents ) )

    typedParams.append( ( "StepID", self.BKstepID ) )

    try:
      noOfEvents = self.xf_o.inputEventsTotal if self.xf_o.inputEventsTotal else self.xf_o.outputEventsTotal
    except AttributeError:
      #This happens iff the XML summary can't be created (e.g. for merging MDF files)
      res = self.bkClient.getFileMetadata( self.stepInputData )
      if not res['OK']:
        raise AttributeError( "Can't get the BKK file metadata" )
      noOfEvents = sum(fileMeta['EventStat'] for fileMeta in res['Value']['Successful'].itervalues())

    typedParams.append( ( "NumberOfEvents", noOfEvents) )

    # Add TypedParameters to the XML file
    for typedParam in typedParams:
      jobNode = addChildNode( jobNode, "TypedParameter", 0, typedParam )

    return jobNode

################################################################################

  def __generateInputFiles( self, jobNode, bkLFNs ):
    ''' InputData looks like this
        <InputFile Name=""/>
    '''

    if self.stepInputData:
      intermediateInputs = False
      for inputname in self.stepInputData:
        for bkLFN in bkLFNs:
          if os.path.basename( bkLFN ) == os.path.basename( inputname ):
            jobNode = addChildNode( jobNode, "InputFile", 0, ( bkLFN, ) )
            intermediateInputs = True
        if not intermediateInputs:
          jobNode = addChildNode( jobNode, "InputFile", 0, ( inputname, ) )

    return jobNode

################################################################################

  def __generateOutputFiles( self, jobNode, bkLFNs, logFilePath ):
    '''OutputFile looks like this:

       <OutputFile Name="" TypeName="" TypeVersion="">
         <Parameter Name="" Value=""/>
         ...
         <Replica Location="" Name=""/>
         ....
       </OutputFile>
    '''

    if self.eventType != None:
      eventtype = self.eventType
    else:
      self.log.warn( 'BookkeepingReport: no eventType specified' )
      eventtype = 'Unknown'
    self.log.info( 'Event type = %s' % ( str( self.eventType ) ) )

    outputs = []
    count = 0
    bkTypeDict = {}
    while count < len( self.stepOutputs ):
      if self.stepOutputs[count].has_key( 'outputDataName' ):
        outputs.append( ( ( self.stepOutputs[ count ][ 'outputDataName' ] ),
                          ( self.stepOutputs[ count ][ 'outputDataType' ] ) ) )
      if self.stepOutputs[ count ].has_key( 'outputBKType' ):
        bkTypeDict[ self.stepOutputs[ count ][ 'outputDataName' ]] = self.stepOutputs[ count ][ 'outputBKType' ]
      count = count + 1
    outputs.append( ( ( self.applicationLog ), ( 'LOG' ) ) )
    self.log.info( outputs )
    if isinstance( logFilePath, list ):
      logFilePath = logFilePath[ 0 ]

    for output, outputtype in list( outputs ):
      self.log.info( 'Looking at output %s %s' % ( output, outputtype ) )
      typeName = outputtype.upper()
      typeVersion = '1'
      fileStats = '0'
      if bkTypeDict.has_key( output ):
        typeVersion = getOutputType( output, self.stepInputData )[output]
        self.log.info( 'Setting POOL XML catalog type for %s to %s' % ( output, typeVersion ) )
        typeName = bkTypeDict[ output ].upper()
        self.log.info( 'Setting explicit BK type version for %s to %s and file type to %s' % ( output,
                                                                                               typeVersion,
                                                                                               typeName ) )

        try:
          fileStats = str( self.xf_o.outputsEvents[output] )
	except AttributeError:
	  #This happens iff the XML summary can't be created (e.g. for merging MDF files)
	  self.log.warn( "XML summary not created, unable to determine the output events" )
	  fileStats = 'Unknown'
        except KeyError as e:
          if ( 'hist' in outputtype.lower() ) or ( '.root' in outputtype.lower() ):
            self.log.warn( "HIST file %s not found in XML summary, event stats set to 'Unknown'" % output )
            fileStats = 'Unknown'
          else:
            raise KeyError( e )

      if not os.path.exists( output ):
        self.log.error( 'File does not exist:' , output )
        continue
      # Output file size
      if not self.step_commons.has_key( 'size' ) or output not in self.step_commons[ 'size' ]:
        try:
          outputsize = str( os.path.getsize( output ) )
        except OSError:
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
          self.log.warn( 'PoolXMLFile generated GUID(s) for the following files ',
                         ', '.join( guidResult[ 'generated' ] ) )
          guid = guidResult[ 'Value' ][ output ]
        else:
          guid = guidResult[ 'Value' ][ output ]
          self.log.info( 'Setting POOL XML catalog GUID for %s to %s' % ( output, guid ) )
      else:
        guid = self.step_commons[ 'guid' ][ output ]

      if not guid:
        raise NameError( 'No GUID found for %s' % output )

      # find the constructed lfn
      lfn = ''
      if not re.search( '.log$', output ):
        for outputLFN in bkLFNs:
          if os.path.basename( outputLFN ) == output:
            lfn = outputLFN
        if not lfn:
          self.log.error( 'Could not find LFN for %s' % output )
          raise NameError( 'Could not find LFN of output file' )
      else:
        lfn = '%s/%s' % ( logFilePath, self.applicationLog )

      oldTypeName = None
      if 'HIST' in typeName.upper():
        typeVersion = '0'

      # PROTECTION for old production XMLs
      if typeName.upper() == 'HIST':
        typeName = '%sHIST' % ( self.applicationName.upper() )

      # Add Output to the XML file
      oFileAttributes = ( lfn, typeName, typeVersion )
      jobNode, oFile = addChildNode( jobNode, "OutputFile", 1, oFileAttributes )

      # HIST is in the dataTypes e.g. we may have new names in the future ;)
      if oldTypeName:
        typeName = oldTypeName

      if outputtype != 'LOG':
        oFile = addChildNode( oFile, "Parameter", 0, ( "EventTypeId", eventtype ) )
        if fileStats != 'Unknown':
          oFile = addChildNode( oFile, "Parameter", 0, ( "EventStat", fileStats ) )

      oFile = addChildNode( oFile, "Parameter", 0, ( "FileSize", outputsize ) )

      ############################################################
      # Log file replica information
#      if typeName == "LOG":
      if self.applicationLog != None:
        logfile = self.applicationLog
        if logfile == output:
          logurl = 'http://lhcb-logs.cern.ch/storage'
          url = logurl + logFilePath + '/' + self.applicationLog
          oFile = addChildNode( oFile, "Replica", 0, ( url, ) )

      oFile = addChildNode( oFile, "Parameter", 0, ( "MD5Sum", md5sum ) )
      oFile = addChildNode( oFile, "Parameter", 0, ( "Guid", guid ) )

    return jobNode

################################################################################

  def __generateSimulationCondition( self, jobNode ):
    '''SimulationCondition looks like this:
       <SimulationCondition>
         <Parameter Name="" Value=""/>
       </SimulationCondition>
    '''
    if self.applicationName == "Gauss":
      jobNode, sim = addChildNode( jobNode, "SimulationCondition", 1, () )
      sim = addChildNode( sim, "Parameter", 0, ( "SimDescription", self.simDescription ) )

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

      with open ( "/proc/cpuinfo", "r" ) as cpuInfo:
        info = cpuInfo.readlines()
      result[ "CPU(MHz)"  ] = info[6].split( ":" )[1].replace( " ", "" ).replace( "\n", "" )
      result[ "ModelName" ] = info[4].split( ":" )[1].replace( " ", "" ).replace( "\n", "" )
      result[ "CacheSize(kB)" ] = info[7].split( ":" )[1].replace( " ", "" ).replace( "\n", "" )

      with open( "/proc/meminfo", "r" ) as memInfo:
        info = memInfo.readlines()
      result["Memory(kB)"] = info[3].split( ":" )[1].replace( " ", "" ).replace( "\n", "" )
    except Exception as x:
      self.log.fatal( 'BookkeepingReport failed to obtain node information with Exception:' )
      self.log.fatal( str( x ) )
      result = S_ERROR()
      result['Message'] = 'Failed to obtain system information'
      return result

    return result

################################################################################
# END AUXILIAR FUNCTIONS
################################################################################

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
