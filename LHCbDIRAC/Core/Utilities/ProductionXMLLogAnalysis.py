########################################################################
# $Id: ProductionLogAnalysis.py 41213 2011-07-08 07:54:43Z fstagni $
########################################################################
""" Production log analysis is a utility to simplify the maintenance of log file
    analysis.  The primary client of this is AnalyseLogFile but the aim
    is to create a standalone utility for checking the sanity of log files
    that can also be used outside of workflows.

    Hopefully this will make use of File Summary Records at some point and as a
    standalone utility this makes future development a lot easier.
"""

__RCSID__ = "$Id: ProductionXMLLogAnalysis.py 41213 2011-07-08 07:54:43Z fstagni $"

from DIRAC import S_OK, S_ERROR, gLogger

# XMLSummaryBase is part of the `LHCb` package ( included on all applications )
# Something not imported, and it crashes, let's use the home made one.
#from XMLSummaryBase import summary
from LHCbDIRAC.Core.Utilities.XMLTreeParser import XMLTreeParser

import string, re, os

#import re, os

#gLogger = gLogger.getSubLogger( 'ProductionXMLLogAnalysis' )
#There is no point in the below being a configuration option since new projects require code changes...
#projectList = ['Boole', 'Gauss', 'Brunel', 'DaVinci', 'LHCb', 'Moore']
#dataSummary = {}
#numberOfEventsInput = 0
#numberOfEventsOutput = 0
#firstStepInputEvents = 0
                                
def analyseXMLLogFile( fileName, applicationName = '', stepName = '', prod = '', job = '', jobType = '' ):
  
  analyser = AnalyseXMLLogFile( fileName, applicationName, stepName, prod, job, jobType )
  return analyser.analise()
  
class AnalyseXMLLogFile:
   
  ''' Allowed application names ''' 
  __APPLICATION_NAMES__  = ['Boole', 'Gauss', 'Brunel', 'DaVinci', 'LHCb', 'Moore']
 
  ''' Well known application Errors '''
  __APPLICATION_ERRORS__ = {
    'Terminating event processing loop due to errors' : 'Event Loop Not Terminated'
  }
 
  ''' Regex used to find last event processed '''
  __APPLICATION_LAST_EVENT__ = {
    'Boole'  :r"Nr. in job = ([0-9]+)",
    'Gauss'  :r"Nr. in job = ([0-9]+)",
    'Brunel' :r"Nr. in job = ([0-9]+)",
    'DaVinci':r"Reading Event record ([0-9]+)",
    'LHCb'   :'',
    'Moore'  :r"Reading Event record ([0-9]+)"                           
  }
 
  ''' Well known Gaudi Errors '''
  __GAUDI_ERRORS__       = {
    'Cannot connect to database'                                     : 'error database connection',
    'Could not connect'                                              : 'CASTOR error connection',
    'SysError in <TDCacheFile::ReadBuffer>: error reading from file' : 'DCACHE connection error',
    'Failed to resolve'                                              : 'IODataManager error',
    'Error: connectDataIO'                                           : 'connectDataIO error',
    'Error:connectDataIO'                                            : 'connectDataIO error',
    ' glibc '                                                        : 'Problem with glibc',
    'segmentation violation'                                         : 'segmentation violation',
    'GaussTape failed'                                               : 'GaussTape failed',
    'Writer failed'                                                  : 'Writer failed',
    'Bus error'                                                      : 'Bus error',
    'Standard std::exception is caught'                              : 'Exception caught',
    'User defined signal 1'                                          : 'User defined signal 1',
    'Not found DLL'                                                  : 'Not found DLL'
  }
  
  ''' Possible services used to get events processed '''
  __POSSIBLE_SERVICES__ = [ 
    'DaVinciInit', 'DaVinciInitAlg', 'DaVinciMonitor', 'BrunelInit', 
    'BrunelEventCount', 'ChargedProtoPAlg', 'BooleInit', 'GaussGen', 
    'GaussSim', 'L0Muon', 'LbAppInit'
  ]
  
  ''' Possible writers used to get events output '''
  __POSSIBLE_WRITERS__ = [                    
    'Writer', 'DigiWriter', 'RawWriter', 'GaussTape', 
    'DstWriter', 'InputCopyStream'
  ]
  
  def __init__( self, fileName, applicationName, stepName, prod, job, jobType ):
    
    self.fileName             = fileName
    self.fileString           = ''
    
    self.xmlFileName          = ''
    self.xmlTree              = None
    
    self.applicationName      = applicationName
    self.stepName             = stepName
    self.jobType              = jobType
    self.prodName             = prod
    self.jobName              = job
    
    self.dataSummary          = {}
    self.numberOfEventsInput  = 0
    self.numberOfEventsOutput = 0
    
    self.gLogger              = gLogger.getSubLogger( 'AnalyseXMLLogFile' )

  def analise( self ):
    
    #For the production case the application name will always be given, for the
    #standalone utility this may not always be true so try to guess
    res = self.__guessAppName()
    if not res[ 'OK' ]:
      return res
    
    res = self.__guessStepID()
    if not res[ 'OK' ]:
      return res
    
    # Check the log file exists and get the contents
    res = self.__openFile()
    if not res[ 'OK' ]:
      return res  

    # Check the xml log file exists and get it as a tree
    res = self.__parseXML()
    if not res[ 'OK' ]:
      return res  
    
    # Check that no errors were seen in the log
    res = self.__checkGaudiErrors()
    if not res['OK']:
      res['Data'] = self.dataSummary
      return res

    # Check that on the XML summary : success = True
    res = self.__checkSuccess()
    if not res['OK']:
      res['Data'] = self.dataSummary
      return res

    # Check that on the XML summary : step = finalize
    res = self.__checkStep()
    if not res['OK']:
      res['Data'] = self.dataSummary
      return res
 
    # Checks that all input files have been read
    res = self.__checkReadInputFiles()
    if not res['OK']:
      res['Data'] = self.dataSummary
      return res

    # Check that the number of events handled is correct
    res = self.__checkApplicationEvents()
    if not res['OK']:
      res['Data'] = self.dataSummary
      return res

    res = S_OK( '%s Completed Successfully' % self.applicationName )
    
    res['numberOfEventsOutput'] = self.numberOfEventsOutput
    res['numberOfEventsInput']  = self.numberOfEventsInput
    #result['FirstStepInputEvents'] = firstStepInputEvents
    
    return res
################################################################################

  def __openFile( self ):
    
    self.gLogger.info( "Attempting to open log file: %s" % self.fileName )
  
    if not os.path.exists( self.fileName ):
      self.gLogger.error( 'Requested log file "%s" is not available' % self.fileName )
      return S_ERROR( 'Log File Not Available' )
    
    if os.stat( self.fileName )[6] == 0:
      self.gLogger.error( 'Requested log file "%s" is empty' % self.fileName )
      return S_ERROR( 'Log File Is Empty' )

    fopen           = open( self.fileName, 'r' )
    self.fileString = fopen.read()
    fopen.close()

    return S_OK()

  def __parseXML( self ):
   
    # It was used application name, but we can have multiple steps with the
    # same application name, so we'd better use the stepName, which is
    # <appName>_<stepID> : eg. Gauss_1, Boole_2, Moore_3, Moore_4
#    xmlFileName = '%s_%s_%s_%s_XMLSummary.xml' % ( self.prodName, self.jobName,self.stepName, self.applicationName )
    
    xmlFileName = 'summary%s_%s_%s_%s.xml' % ( self.applicationName, self.prodName, self.jobName,self.stepName )
    
    if '/' in self.fileName:
      self.xmlFileName = '%s/' % self.fileName.rplit('/',1)
      
    self.xmlFileName += xmlFileName        
    self.gLogger.info( "Attempting to parse xml log file: %s" % self.xmlFileName )

    if not os.path.exists( self.xmlFileName ):
      self.gLogger.error( 'Requested xml log file "%s" is not available' % self.xmlFileName )
      return S_ERROR( 'XML Log File Not Available' )
    
    if os.stat( self.xmlFileName )[6] == 0:
      self.gLogger.error( 'Requested xml log file "%s" is empty' % self.xmlFileName )
      return S_ERROR( 'XML Log File Is Empty' )
   
    summary      = XMLTreeParser()
    
    try:
      self.xmlTree = summary.parse( self.xmlFileName )
    except Exception, e:
      return S_ERROR( 'Error parsing xml summary: %s' % e )
          
    return S_OK()

  def __guessAppName( self ):
    
    #For the production case the application name will always be given, for the
    #standalone utility this may not always be true so try to guess
    if not self.applicationName:
      for line in self.fileString.split( '\n' ):
        if re.search( 'Welcome to', line ) and re.search( 'version', line ):
          try:
            self.applicationName = line.split()[2]
          except Exception:
            return S_ERROR( 'Could not obtain application name' )
          self.gLogger.info( 'Guessing application name is "%s" from log file %s' % ( self.applicationName, self.fileName ) )

    if not self.applicationName in self.__APPLICATION_NAMES__:
      appNames = ', '.join( self.__APPLICATION_NAMES__ )
      self.gLogger.error( 'Application name "%s" is not in allowed list: %s' % ( self.applicationName, appNames ) )
      return S_ERROR( 'Log Analysis of %s Not Supported' % ( self.applicationName ) )

    return S_OK()

  def __guessStepID( self ):
    ''' If we have no stepName, prodName and jobName ( typically from the
        script ), we try to get them from the log file name. This is a horrible
        practice, and if the syntax changes, this will crash.
    '''
    
    if self.prodName and self.jobName and self.stepName:
      return S_OK()
    
    self.gLogger.info( 'Guessing production, job and step from %s' % self.fileName )
    
    # We know that the log file names look like this:
    # <AppName>_<prodName>_<jobName>_<stepName>.log
       
    guess = self.fileName
    guess = guess.replace( '.log', '')
    guess = guess.split( '_' )
    
    if len(guess) != 4:
      self.gLogger.error( 'Could not guess production, job and step from %s' % self.fileName )
      return S_ERROR( 'Production, job and step params missing' )
    
    self.prodName = guess[ 1 ]
    self.jobName  = guess[ 2 ]
    self.stepName = guess[ 3 ]
    
    self.gLogger.info( 'Guessed prod: %s, job: %s and step: %s' % ( self.prodName, self.jobName, self.stepName ) )
    
    return S_OK()

################################################################################
## CHECKS    
################################################################################

  def __checkGaudiErrors( self ):
    
    """ This method stores Gaudi strings that are well known and determines
        success / failure based on conventions.
    """
    # Check if the application finish successfully
    toFind = 'Application Manager Finalized successfully'
    if self.applicationName.lower() == 'moore':
      #toFind = 'Service finalized successfully'
      toFind = ''
    else:
      self.gLogger.info( 'Check application ended successfully e.g. searching for "%s"' % toFind )

    if toFind:
      okay = re.findall( toFind, self.fileString )
      if not okay:
        self.gLogger.info( '"%s" was not found in log...' % toFind )
        return S_ERROR( 'Finalization Error' )
 
    appErrors   = self.__checkErrors( 'APPLICATION' )
    if not appErrors[ 'OK' ]:
      return appErrors
 
    gaudiErrors = self.__checkErrors( 'GAUDI' )
    if not gaudiErrors[ 'OK' ]:
      return gaudiErrors
 
    return S_OK( 'All checks passed' )    

  def __checkErrors( self, type ):
 
    if type not in [ 'APPLICATION', 'GAUDI' ]:
      return S_ERROR( 'Wrong error type name' )
 
    errors = getattr( self, '__%s_ERRORS__' % type )
 
    for errString, description in errors.items():
      self.gLogger.info( 'Checking for "%s" meaning job would fail with "%s"' % ( errString, description ) )
      found = re.findall( errString, self.fileString )
      if found:
        self.gLogger.error( 'Found error in log file => "%s"' % errString )
        
        if type == 'APPLICATION':
          result = self.__getLastFile()
          if result[ 'OK' ]:
            lastFile = result[ 'Value' ]
            self.gLogger.info( 'Determined last file before crash to be: %s => ApplicationCrash' % lastFile )
            self.dataSummary[ lastFile ] = 'ApplicationCrash'

        return S_ERROR( description )

    return S_OK()

################################################################################

  def __checkSuccess( self ):
    """Checks that the XML summary reports success == True
    """
    
    res = self.__getSuccess()
    if not res[ 'OK' ]:
      self.gLogger.error( 'XMLSummary success bad formated %s' % res[ 'Message' ] )  
      return res
    
    if not res['Value'] == 'True':  
      self.gLogger.error( 'XMLSummary did NOT succeed ( success = False )' )
      return S_ERROR( False )
    
    self.gLogger.info( 'XMLSummary reports success = True ' )  
    return res  

################################################################################

  def __checkStep( self ):
    """Checks that the XML summary reports step == finalize
    """

    res = self.__getStep()
    if not res[ 'OK' ]:
      self.gLogger.error( 'XMLSummary step bad formated %s' % res[ 'Message' ] )  
      return res
    
    if res['Value'] != 'finalize':  
      self.gLogger.error( 'XMLSummary reports step did not finalize' )
      return S_ERROR( res['Value'] )
    
    self.gLogger.info( 'XMLSummary reports step finalized' )  
    return res  

################################################################################

  def __checkReadInputFiles( self ):
    """Checks that every input file has reached the full status.
       Four possible statuses of the files:
       - full : the file has been fully read
       - part : the file has been partially read
       - mult : the file has been read multiple times
       - fail : failure while reading the file
    """

    res = self.__getInputStatus()
    if not res[ 'OK' ]:
      self.gLogger.error( 'XMLSummary bad formated %s' % res[ 'Message' ] )  
      return res

    fileCounter = {
                   'full'  : 0,
                   'part'  : 0,
                   'mult'  : 0,
                   'fail'  : 0,
                   'other' : 0
                   }

    for file,status in res[ 'Value' ]:
      
      if status == 'fail':
        self.gLogger.error( 'File %s is on status %s.' % ( file, status ) )
        fileCounter[ 'fail' ] += 1  

      elif status == 'mult':
        self.gLogger.error( 'File %s is on status %s.' % ( file, status ) )
        fileCounter[ 'mult' ] += 1  

      elif status == 'part':
        self.gLogger.error( 'File %s is on status %s.' % ( file, status ) )
        fileCounter[ 'part' ] += 1  

      elif status == 'full':
        #If it is Ok, we do not print anything
        #self.gLogger.error( 'File %s is on status %s.' % ( file, status ) )
        fileCounter[ 'full' ] += 1  
  
      # This should never happen, but just in case
      else:
        self.gLogger.error( 'File %s is on unknown status: %s' % ( file,status ) )
        fileCounter[ 'other'] += 1    
     
    files = [ '%d file(s) on %s status' % (v,k) for k,v in fileCounter.items()]
    filesMsg =  ', '.join( files )
    self.gLogger.info( filesMsg )

    if fileCounter[ 'full' ] != len( fileCounter ):
      return S_ERROR( filesMsg )
    
    return S_OK()

################################################################################

  def __checkApplicationEvents( self ):
    """ Internally calls the correctly named method to check the number of
        events in an application log file.
    """
    
    self.numberOfEventsInput  = self.__getInputEvents()[ 'Value' ]
    self.numberOfEventsOutput = self.__getOutputEvents()[ 'Value' ]   
    
    # 'private' functions starting with '__' are mangled by Python
    try:
      appCheck = getattr( self, '_AnalyseXMLLogFile__check%sEvents' % self.applicationName )
    except:
      return S_ERROR( 'No checker for application %s' % self.applicationName )
    
    return appCheck()

  def __checkGaussEvents( self ):
    '''
      Check 1:
        ? 
    '''
    return S_OK()
    
  def __checkBooleEvents( self ):
    '''
      Check 1:
        Input == Output
      
    '''
    # Check 1
    if self.numberOfEventsInput != self.numberOfEventsOutput:
      return S_ERROR( 'InputEvents(%d) != OutputEvents(%d)' % 
                      ( self.numberOfEventsInput, self.numberOfEventsOutput ) )
    return S_OK()
  
  def __checkMooreEvents( self ):
    '''
      Check 1:
        Input >= Output
      
    '''
    # Check 1
    if self.numberOfEventsInput < self.numberOfEventsOutput:
      return S_ERROR( 'InputEvents(%d) < OutputEvents(%d)' % 
                      ( self.numberOfEventsInput, self.numberOfEventsOutput ) )
    return S_OK()
  
  def __checkBrunelEvents( self ):
    '''
      Check 1:
        Input == Output
      
    '''
    # Check 1
    if self.numberOfEventsInput != self.numberOfEventsOutput:
      return S_ERROR( 'InputEvents(%d) != OutputEvents(%d)' % 
                      ( self.numberOfEventsInput, self.numberOfEventsOutput ) )
    return S_OK()
    
  def __checkDaVinciEvents( self ):
    '''
      Check 1:
        Input >= Output
      Check 2:
        Input == Output
        ( Only applies for a particular JobType, in this case MCSimulation )   
    '''
    # Check 1
    if self.numberOfEventsInput < self.numberOfEventsOutput:
      return S_ERROR( 'InputEvents(%d) < OutputEvents(%d)' % 
                      ( self.numberOfEventsInput, self.numberOfEventsOutput ) )
    return S_OK()
    # Check 2
    if self.jobType == 'MCsimulation' and self.numberOfEventsInput != self.numberOfEventsOutput:
      return S_ERROR( 'InputEvents(%d) < OutputEvents(%d)' % 
                      ( self.numberOfEventsInput, self.numberOfEventsOutput ) )
    return S_OK()
    
    
  def __checkLHCbEvents( self ):
    '''
      Check 1:
        Input == Output
        ( Only applies for a particular JobType, in this case MCSimulation )   
    '''
    # Check 1
    if self.jobType == 'MCsimulation' and self.numberOfEventsInput != self.numberOfEventsOutput:
      return S_ERROR( 'InputEvents(%d) != OutputEvents(%d)' % 
                      ( self.numberOfEventsInput, self.numberOfEventsOutput ) )
    return S_OK()

######################## OLD LOG PARSERS #######################################

  def __checkGaussEventsLog( self ):
    """ Obtain event information from the application log and determine whether
        the Gauss job generated the correct number of events.
    """
    # Get the number of requested events
    res = self.__getRequestedEvents()
    if not res['OK']:
      return res
    requestedEvents = res['Value']
    # You must give a requested number of events for Gauss 
    # (this is pointless as the job will run forever)
    if not requestedEvents:
      return S_ERROR( "Missing Requested Events For Gauss" )

    # Get the last event processed
    lastEvent = self.__getLastEvent()['Value']
    # Get the number of events generated by Gauss
    res = self.__getEventsProcessed( 'GaussGen' )
    if not res['OK']:
      self.gLogger.error( "Crash in event %s" % lastEvent )
      return S_ERROR( 'Crash During Execution' )

    generatedEvents = res['Value']
        
    # Get the number of events processed by Gauss
    res = self.__getEventsOutput( 'GaussTape' )
    if not res['OK']:
      return S_ERROR( 'No Events Output' )
    outputEvents = res['Value']    
    
    # Check that the correct number of events were generated
    if generatedEvents != requestedEvents:
      return S_ERROR( 'Too Few Events Generated' )
    # Check that more than 90% of generated events are output
    if outputEvents < 0.9 * requestedEvents:
      return S_ERROR( 'Not Enough Events Generated' )
    
    ## PARAMS FOR BKK
    self.numberOfEventsInput  = str( generatedEvents )
    self.numberOfEventsOutput = str( outputEvents )
    
    return S_OK()

  def __checkBooleEventsLog( self ):
    """ Obtain event information from the application log and determine whether the
        Boole job processed the correct number of events.
    """
    # Get the number of requested events
    res = self.__getRequestedEvents()
    if not res['OK']:
      return res
    requestedEvents = res['Value']

    # Get the last event processed
    lastEvent = self.__getLastEvent()['Value']

    # Get the number of events processed by Boole
    res = self.__getEventsProcessed( 'BooleInit' )
    if not res['OK']:
      res = self.__getLastFile()
      if res['OK']:
        lastFile = res['Value']
        self.dataSummary[lastFile] = 'ApplicationCrash'
      self.gLogger.error( "Crash in event %s" % lastEvent )
      return S_ERROR( 'Crash During Execution' )
    processedEvents = res['Value']
    
    # Get the number of events output by Boole
    res = self.__getEventsOutput( 'DigiWriter' )
    if not res['OK']:
      res = self.__getEventsOutput( 'RawWriter' )
      if not res['OK']:
        return S_ERROR( 'No Events Output' )
    outputEvents = res['Value']   
    
    # Get whether all events in the input file were processed
    noMoreEvents = re.findall( 'No more events in event selection', self.fileString )

    # If were are to process all the files in the input then ensure that all were read
    if ( not requestedEvents ) and ( not noMoreEvents ):
      return S_ERROR( "Not All Input Events Processed" )
    # If we are to process a given number of events ensure the target was met
    if requestedEvents:
      if requestedEvents != processedEvents:
        return S_ERROR( 'Too Few Events Processed' )
    # Check that the final reported processed events match those logged as processed during execution
    if lastEvent != processedEvents:
      self.gLogger.verbose( 'Last reported event %s != processed events %s' % ( lastEvent, processedEvents ) )
      if processedEvents > 100 and lastEvent < 0.9 * processedEvents:
        return S_ERROR( "Processed Events Do Not Match" )
    # If there were no events processed
    if processedEvents == 0:
      return S_ERROR( "No Events Processed" )
    # If the output events are not equal to the processed events be sure there were no failed events
    if outputEvents != processedEvents:
      self.gLogger.warn( 'Number of processed events %s does not match output events %s (considered OK for Boole)' % ( processedEvents, outputEvents ) )

    ## PARAMS FOR BKK
    self.numberOfEventsInput  = str( processedEvents )
    self.numberOfEventsOutput = str( outputEvents )

    return S_OK()
  
  def __checkMooreEventsLog( self ):
    """ Obtain event information from the application log and determine whether
        the Moore job generated the correct number of events.
    """

    # Get the last event processed
    lastEvent = self.__getLastEvent()[ 'Value' ]
 
    # Get the number of events processed by Moore
    res = self.__getEventsProcessed( 'L0Muon' )
    if not res['OK']:
      res = self.__getLastFile()
      if res['OK']:
        lastFile = res['Value']
        self.dataSummary[ lastFile ] = 'ApplicationCrash'
      self.gLogger.error( "Crash in event %s" % lastEvent )
      return S_ERROR( 'Crash During Execution' )
    processedEvents = res[ 'Value' ]
    
    ## PARAMS TO BKK
    self.numberOfEventsInput = str( processedEvents )
    
    return S_OK()
  
  def __checkBrunelEventsLog( self ):
    """ Obtain event information from the application log and determine whether the
        Brunel job processed the correct number of events.
    """
    # Get the number of requested events
    res = self.__getRequestedEvents()
    if not res['OK']:
      return res
    requestedEvents = res['Value']

    # Get the last event processed
    lastEvent = self.__getLastEvent()['Value']

    # Get the number of events processed by Brunel
    res = self.__getEventsProcessed( 'BrunelInit' )
    if not res['OK']:
      res = self.__getLastFile()
      if res['OK']:
        lastFile = res['Value']
        self.dataSummary[lastFile] = 'ApplicationCrash'
      self.gLogger.error( "Crash in event %s" % lastEvent )
      return S_ERROR( 'Crash During Execution' )
    processedEvents = res['Value']
    
    # Get the number of events output by Brunel
    res = self.__getEventsOutput( 'DstWriter' )
    if not res['OK']:
      return S_ERROR( 'No Events Output' )
    outputEvents = res['Value']
    
    # Get whether all events in the input file were processed
    noMoreEvents = re.findall( 'No more events in event selection', self.fileString )

    # If were are to process all the files in the input then ensure that all were read
    if ( not requestedEvents ) and ( not noMoreEvents ):
      return S_ERROR( "Not All Input Events Processed" )
    # If we are to process a given number of events ensure the target was met
    if requestedEvents:
      if requestedEvents != processedEvents:
        return S_ERROR( 'Too Few Events Processed' )
    # Check that the final reported processed events match those logged as processed during execution
    if lastEvent != processedEvents:
      self.gLogger.verbose( 'Last reported event %s != processed events %s' % ( lastEvent, processedEvents ) )
      if processedEvents > 100 and lastEvent < 0.9 * processedEvents:
        return S_ERROR( "Processed Events Do Not Match" )
    # If there were no events processed
    if processedEvents == 0:
      return S_ERROR( "No Events Processed" )
    # If the output events are not equal to the processed events be sure there were no failed events
    if outputEvents != processedEvents:
      self.gLogger.warn( 'Number of processed events %s does not match output events %s (considered OK for Brunel)' % ( processedEvents, outputEvents ) )

    ## PARAMS FOR BKK
    self.numberOfEventsInput  = str( processedEvents )
    self.numberOfEventsOutput = str( outputEvents )

    return S_OK()

  def __checkDaVinciEventsLog( self ):
    """ Obtain event information from the application log and determine whether the
        DaVinci job processed the correct number of events.
    """

    # Get the number of requested events
    res = self.__getRequestedEvents()
    if not res['OK']:
      return res
    requestedEvents = res['Value']

    # Get the last event processed
    lastEvent = self.__getLastEvent()[ 'Value' ]    

    # Get the number of events processed by DaVinci
    res = self.__getEventsProcessed( 'DaVinciInit' )
    if not res['OK']:
      res = self.__getLastFile()
      if res['OK']:
        lastFile = res['Value']
        self.dataSummary[lastFile] = 'ApplicationCrash'
      self.gLogger.error( "Crash in event %s" % lastEvent )
      return S_ERROR( 'Crash During Execution' )
    processedEvents = res['Value']
        
    # Get whether all events in the input file were processed
    noMoreEvents = re.findall( 'No more events in event selection', self.fileString )

    # If were are to process all the files in the input then ensure that all were read
    if ( not requestedEvents ) and ( not noMoreEvents ):
      return S_ERROR( "Not All Input Events Processed" )
    # If we are to process a given number of events ensure the target was met
    if requestedEvents:
      if requestedEvents != processedEvents:
        return S_ERROR( "Too Few Events Processed" )

    result = self.__getEventsOutput( 'InputCopyStream' )
    if result['OK']:
      self.gLogger.info( 'Found "%s" events output for DaVinci with InputCopyStream' % ( result['Value'] ) )
    outputEvents = result[ 'Value' ]
    
    ## PARAMS TO BKK
    self.numberOfEventsInput  = str( processedEvents )
    self.numberOfEventsOutput = str( outputEvents )
    
    return S_OK()

  def __checkLHCbEventsLog( self ):
    return S_OK()

################################################################################
# AUXILIAR FUNCTIONS 
################################################################################

#### BEGIN: XML - FSR functions

  def __getSuccess( self ):
    '''
       Checks that the element success is unique and returns
       its value
       
       <success>True/False</success>    
    '''
    
    sum = self.xmlTree[ 0 ]
    
    successXML = sum.childrens( 'success' )
    if len( successXML ) != 1:
      return S_ERROR( 'Nr of success items != 1' ) 
    
    return S_OK( successXML[ 0 ].value )

  def __getStep( self ):
    '''
       Checks that the element step is unique and returns
       its value
       
       <step>finalize/x/y...</step>    
    '''
    
    sum = self.xmlTree[ 0 ]
    
    stepXML = sum.childrens( 'step' )
    if len( stepXML ) != 1:
      return S_ERROR( 'Nr of step items != 1' ) 
    
    return S_OK( stepXML[ 0 ].value )

  def __getInputStatus( self ):
    '''
      We know beforehand the structure of the XML, which makes our life
      easier.
      
      < summary >
        ...
        < input >
        ...
    '''
    
    files = []
    
    sum = self.xmlTree[ 0 ]
    
    for input in sum.childrens( 'input' ):
      for file in input.childrens( 'file' ):
        try:
          files.append( ( file.attributes[ 'name' ], file.attributes[ 'status' ] ) ) 
        except Exception, e:
          return S_ERROR( 'Bad formatted file keys. %s' % e ) 
 
    return S_OK( files )
 
  def __getInputEvents( self ):
    '''
      We know beforehand the structure of the XML, which makes our life
      easier.
      
      < summary >
        ...
        < input >
        ...
    '''
    inputEvents = 0   

    sum = self.xmlTree[ 0 ]
    
    for input in sum.childrens( 'input' ):
      for file in input.childrens( 'file' ):
        inputEvents += int( file.value )    
    
    return S_OK( inputEvents )
  
####  

  def __getOutputEvents( self ):
    '''
      We know beforehand the structure of the XML, which makes our life
      easier.
      
      < summary >
        ...
        < output >
        ...
    '''
    
    outputEvents = 0

    sum = self.xmlTree[ 0 ]

    for output in sum.childrens( 'input' ):
      for file in output.childrens( 'file' ):
        outputEvents += int( file.value )     
    
    return S_OK( outputEvents )

#### END: XML - FSR functions
#
#  def __getRequestedEvents( self ):
#    """ Determine the number of requested events from the application log. The log should contain one of two strings:
#  
#        Requested to process all events on input file   or
#        Requested to process x events
#  
#        If neither of these strings are found an error is returned
#    """
#    exp = re.compile( r"Requested to process ([0-9]+|all)" )
#    findline = re.search( exp, self.fileString )
#    if not findline:
#      self.gLogger.error( "Could not determine requested events." )
#      return S_ERROR( "Could Not Determine Requested Events" )
#    events = findline.group( 1 )
#    if events == 'all':
#      requestedEvents = 0
#      self.gLogger.info( 'Determined the number of requested events to be "all".' )
#    else:
#      requestedEvents = int( events )
#      self.gLogger.info( 'Determined the number of requested events to be %s.' % requestedEvents )
#    return S_OK( requestedEvents )    
#
#####
#
#  def __getLastEvent( self ):
#    """ Determine the last event handled from the application log. The log should contain the string:
#
#         Nr. in job = x
#
#        If this string is not found then 0 is returned
#    """
#    """ DaVinci does not write out each event but instead give a summary every x events. The log should contain the string:
#
#        Reading Event record
#
#        If this string is not found then 0 is returned
#    """
#    
#    regex = self.__APPLICATION_LAST_EVENT__[ self.applicationName ]
#    
#    exp = re.compile( regex )
#    list = re.findall( exp, self.fileString )
#    if not list:
#      event = 0
#    else:
#      event = int( list[-1] )
#    self.gLogger.info( "Determined the number of events to be %s." % event )
#    return S_OK( event )
#
#####
#
#  def __getEventsProcessed( self, service ):
#    """ Determine the number of events reported processed by the supplied service. The log should contain the string:
#
#        Service          SUCCESS x events processed
#
#        If the string is not found an error is returned
#    """
#    
#    if not service in self.__POSSIBLE_SERVICES__:
#      msg = "Requested service '%s' not available" % service
#      self.gLogger.error( msg )
#      return S_ERROR( msg )
#    
#    exp = re.compile( r"%s\s+SUCCESS (\d+) events processed" % service )
#    if service == 'L0Muon':
#      exp = re.compile( r"%s\s+INFO - Total number of events processed\s+:\s+(\d+)" % service )
#
#    findline = re.search( exp, self.fileString )
#    if not findline:
#      if not re.search( 'Alg$', service ):
#        return self.__getEventsProcessed( '%sAlg' % service )
#      
#      msg = "Could not determine events processed."
#      self.gLogger.error( msg )
#      return S_ERROR( msg )
#
#    eventsProcessed = int( findline.group( 1 ) )
#    self.gLogger.info( "Determined the number of events processed to be %s." % eventsProcessed )
#    
#    return S_OK( eventsProcessed )
#
#####
#
#  def __getEventsOutput( self, writer ):
#    """  Determine the number of events written out by the supplied writer. The log should contain the string:
#  
#         Writer            INFO Events output: x
#  
#         If the string is not found an error is returned
#    """
#    
#    if not writer in self.__POSSIBLE_WRITERS__:
#      msg = 'Requested writer "%s" not available.' % writer
#      self.gLogger.error( msg )
#      return S_ERROR( msg )
#    
#    exp = re.compile( r"%s\s+INFO Events output: (\d+)" % writer )
#    findline = re.search( exp, self.fileString )
#    
#    if not findline:
#      msg = 'Could not determine events output.'
#      self.gLogger.warn( msg )
#      return S_ERROR( msg )
#    
#    writtenEvents = int( findline.group( 1 ) )
#    self.gLogger.info( "Determined the number of events written to be %s." % writtenEvents )
#    
#    return S_OK( writtenEvents )
#
#####
#
#  def __getLastFile( self ):
#    """ Determine the last input file opened from the application log.
#    """
#    files = self.__getInputFiles()[ 'Value' ]
#    if files:
#      return S_OK( files[ -1 ] )
#    return S_ERROR( "No input files opened" )
#
#####
#
#  def __getInputFiles( self ):
#    """ Determine the list of input files accessed from the application log. The log should contain a string like:
#
#        Stream:EventSelector.DataStreamTool_1 Def:DATAFILE='filename'
#
#        In the event that the file name contains LFN: this is removed.
#    """
#    exp = re.compile( r"Stream:EventSelector.DataStreamTool_1 Def:DATAFILE='(\S+)'" )
#    files = re.findall( exp, self.fileString )
#    strippedFiles = []
#    for file in files: 
#      strippedFiles.append( file.replace( 'LFN:', '' ) )
#    return S_OK( strippedFiles )
#  
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################

'''This function is still using the "old" style.
'''

def getDaVinciStreamEvents( logFile, bkFileTypes ):
  """ Get the number of stream file events, intended to work for both MDST
      and DST streams.  The arguments are the path to a log file, and list of BK
      file types to check for.

      Initially this was a workaround in GaudiApplication but is now the default
      way of handling the streams.  With the method in this utility at least the
      event counting can now be tested outside of running jobs.
  """

  gLogger = gLogger.getSubLogger( 'ProductionXMLLogAnalysis' )

  fopen = open( logFile, 'r' )
  lines = fopen.readlines()
  fopen.close()

  checkStreams = {}
  for bk in bkFileTypes:
    if re.search( '.', bk ):
      bk = bk.split( '.' )[0]
    checkStreams[bk] = string.lower( bk )

  #Here we rely on there being a string "Events output" in the lines we are interested in
  davinciStringsToCheck = ['Events output']

  candidateLines = []
  #First get the candidate lines (DaVinci logs can be huge)
  for line in lines:
    for check in davinciStringsToCheck:
      if re.search( check, line ):
        candidateLines.append( line )

  if not candidateLines:
    gLogger.warn( 'No candidate lines were found to match the following BK types: %s' % ( string.join( bkFileTypes, ', ' ) ) )
    return S_ERROR( 'No matching lines found in log file' )

  streamEvents = {}

  #The lines to match frequently change for DaVinci but those being used are currently:
  #2010-10-23 03:28:35 UTC Bhadron_OStream      INFO Events output: 196
  #N.B. OStream gets shortened e.g. so can't be used as a hook...
  #2010-10-12 10:12:31 UTC Bd2JpsiKstar_OS...   INFO Events output: 399

  for line in candidateLines:
    loweredLine = string.lower( line )
    for bkType, typeToMatch in checkStreams.items():
      # N.B. the full stream name must at least be present in the log line
      if re.search( '%s\S+\s+\S+\s+events output' % ( typeToMatch ), loweredLine ):
        try:
          gLogger.verbose( 'Checking " %s "' % ( line.strip() ) )
          eventsForStream = line.strip().split( ' ' )[-1]
          streamEvents[bkType] = eventsForStream
          gLogger.info( 'Found %s events for output stream %s' % ( eventsForStream, bkType ) )
        except Exception, x:
          gLogger.error( 'Could not extract stream events from DaVinci log file... something has changed\n"%s" => "%s"' % ( line, x ) )

  for bk in checkStreams.values():
    bk = bk.upper()
    if not streamEvents.has_key( bk ):
      gLogger.warn( 'No number of events was found for output stream "%s", check the DaVinci log' % ( bk ) )

  return S_OK( streamEvents )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
