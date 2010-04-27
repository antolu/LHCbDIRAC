########################################################################
# $Id: ProductionLogAnalysis.py 24215 2010-04-19 11:21:11Z paterson $
########################################################################
""" Production log analysis is a utility to simplify the maintenance of log file
    analysis.  The primary client of this is AnalyseLogFile but the aim
    is to create a standalone utility for checking the sanity of log files
    that can also be used outside of workflows.
    
    Hopefully this will make use of File Summary Records at some point and as a
    standalone utility this makes future development a lot easier. 
"""

__RCSID__ = "$Id: ProductionLogAnalysis.py 24215 2010-04-19 11:21:11Z paterson $"

from DIRAC import S_OK, S_ERROR, gLogger, gConfig

import string,re,os

gLogger = gLogger.getSubLogger('ProductionLogAnalysis')
#There is no point in the below being a configuration option since new projects require code changes...
projectList = ['Boole','Gauss','Brunel','DaVinci','LHCb','Moore']
dataSummary = {}
numberOfEventsInput = 0
numberOfEventsOutput = 0
firstStepInputEvents = 0

#############################################################################
def analyseLogFile(fileName,applicationName=''):
  """ This method uses all the below methods to perform comprehensive checks
      on the supplied log file if it exists.
  """
  # Check the log file exists and get the contents
  gLogger.info("Attempting to open log file: %s" % (fileName))
  if not os.path.exists(fileName):
    self.log.error('Requested log file %s is not available' %(fileName))
    return S_ERROR('Log File Not Available')
  if os.stat(fileName)[6] == 0:
    self.log.error('Requested log file %s is empty' %(fileName))
    return S_ERROR('Log File Is Empty')

  fopen = open(fileName,'r')
  logString = fopen.read()
  fopen.close()
  
  # Check that no errors were seen in the log
  result = checkGaudiErrors(logString,applicationName)
  if not result['OK']:
    result['Data']=dataSummary
    return result

  #For the production case the application name will always be given, for the
  #standalone utility this may not always be true so try to guess
  if not applicationName:
    for line in logString.split('\n'):
      if re.search('Welcome to',line) and re.search('version',line):
        try:
           applicationName = line.split()[2]      
        except Exception,x:
          return S_ERROR('Could not obtain application name')
        gLogger.info('Guessing application name is "%s" from log file %s' %(applicationName,fileName))
  
  if not applicationName in projectList:
    gLogger.error('Application name "%s" is not in allowed list: %s' %(applicationName,string.join(projectList,', ')))
    return S_ERROR('Log Analysis of %s Not Supported' %(applicationName))
  
  # Check that the number of events handled is correct
  result = checkApplicationEvents(applicationName,logString)
  if not result['OK']:
    result['Data']=dataSummary
    return result
  
  result = S_OK('%s Completed Successfully' %(applicationName))
  result['numberOfEventsOutput']=numberOfEventsOutput
  result['numberOfEventsInput']=numberOfEventsInput
  result['FirstStepInputEvents']=firstStepInputEvents
  return result

#############################################################################
def checkApplicationEvents(applicationName,logString):
  """ Internally calls the correctly named method to check the number of 
      events in an application log file. 
  """ 
  # Check that the number of events handled is correct
  if applicationName.lower() == 'lhcb':
    return checkLHCbEvents(logString)
  if applicationName.lower() == 'gauss':
    return checkGaussEvents(logString)
  if applicationName.lower() == 'boole':
    return checkBooleEvents(logString)
  if applicationName.lower() == 'brunel':
    return checkBrunelEvents(logString)
  if applicationName.lower() == 'davinci':
    return checkDaVinciEvents(logString)
  if applicationName.lower() == 'moore':
    return checkMooreEvents(logString)
  return S_ERROR("Application Not Known")

#############################################################################
def getInputFiles(logString):
  """ Determine the list of input files accessed from the application log. The log should contain a string like:

      Stream:EventSelector.DataStreamTool_1 Def:DATAFILE='filename'

      In the event that the file name contains LFN: this is removed.
  """
  exp = re.compile(r"Stream:EventSelector.DataStreamTool_1 Def:DATAFILE='(\S+)'")
  files = re.findall(exp,logString)
  strippedFiles = []
  for file in files: strippedFiles.append(file.replace('LFN:',''))
  return S_OK(strippedFiles)

#############################################################################
def getLastFile(logString):
  """ Determine the last input file opened from the application log.
  """
  files = getInputFiles(logString)['Value']
  if files:
    return S_OK(files[-1])
  return S_ERROR("No input files opened")

#############################################################################
def checkGaudiErrors(logString,applicationName):
  """ This method stores Gaudi strings that are well known and determines
      success / failure based on conventions.
  """
  global dataSummary
  # Check if the application finish successfully
  toFind = 'Application Manager Finalized successfully'
  if applicationName.lower()=='moore':
    toFind = 'Service finalized successfully'
  gLogger.info('Check application ended successfully e.g. searching for "%s"' %(toFind))
  
  okay = re.findall(toFind,logString)
  if not okay:
    gLogger.info('"%s" was not found in log...' %(toFind))
    return S_ERROR('Finalization Error')

  # Check whether there were errors completing the event loop
  applicationErrors = {'Terminating event processing loop due to errors':'Event Loop Not Terminated'}
  for errString,description in applicationErrors.items():
    gLogger.info('Checking for "%s" meaning job would fail with "%s"' %(errString,description))
    found = re.findall(errString,logString)
    if found:
      gLogger.error('Found error in log file => "%s"' %(errString))
      result = getLastFile(logString)
      if result['OK']:
        lastFile = result['Value']
        gLogger.info('Determined last file before crash to be: %s => ApplicationCrash' %(lastFile))
        dataSummary[lastFile]='ApplicationCrash'
        
      return S_ERROR(description)

  # Check for a known list of problems in the application logs
  gaudiErrors = {\
  'Cannot connect to database'                                      :     'error database connection',
  'Could not connect'                                               :     'CASTOR error connection',
  'SysError in <TDCacheFile::ReadBuffer>: error reading from file'  :     'DCACHE connection error',
  'Failed to resolve'                                               :     'IODataManager error',
  'Error: connectDataIO'                                            :     'connectDataIO error',
  'Error:connectDataIO'                                             :     'connectDataIO error',
  ' glibc '                                                         :     'Problem with glibc',
  'segmentation violation'                                          :     'segmentation violation',
  'GaussTape failed'                                                :     'GaussTape failed',
  'Writer failed'                                                   :     'Writer failed',
  'Bus error'                                                       :     'Bus error',
  'Standard std::exception is caught'                               :     'Exception caught',
  'User defined signal 1'                                           :     'User defined signal 1',
  'Not found DLL'                                                   :     'Not found DLL'}

  for errString,description in gaudiErrors.items():
    gLogger.info('Checking for "%s" meaning job would fail with "%s"' %(errString,description))
    found = re.findall(errString,logString)
    if found:
      gLogger.error('Found error in log file => "%s"' %(errString))
      return S_ERROR(description)

  return S_OK('All checks passed')

#############################################################################
def getRequestedEvents(logString):
  """ Determine the number of requested events from the application log. The log should contain one of two strings:

      Requested to process all events on input file   or
      Requested to process x events

      If neither of these strings are found an error is returned
  """
  exp = re.compile(r"Requested to process ([0-9]+|all)")
  findline = re.search(exp,logString)
  if not findline:
    gLogger.error("Could not determine requested events.")
    return S_ERROR("Could Not Determine Requested Events")
  events = findline.group(1)
  if events == 'all':
    requestedEvents = 0
    gLogger.info('Determined the number of requested events to be "all".')
  else:
    requestedEvents = int(events)
    gLogger.info('Determined the number of requested events to be %s.' %requestedEvents)
  return S_OK(requestedEvents)

#############################################################################
def getLastEvent(logString):
  """ Determine the last event handled from the application log. The log should contain the string:

       Nr. in job = x

      If this string is not found then 0 is returned
  """
  exp = re.compile(r"Nr. in job = ([0-9]+)")
  list = re.findall(exp,logString)
  if not list:
    lastEvent = 0
  else:
    lastEvent = int(list[-1])
  gLogger.info("Determined the number of events handled to be %s." % lastEvent)
  return S_OK(lastEvent)

#############################################################################
def getLastEventSummary(logString):
  """ DaVinci does not write out each event but instead give a summary every x events. The log should contain the string:

      Reading Event record

      If this string is not found then 0 is returned
  """
  exp = re.compile(r"Reading Event record ([0-9]+)")
  list = re.findall(exp,logString)
  if not list:
    readEvents = 0
  else:
    readEvents = int(list[-1])
  gLogger.info("Determined the number of events read to be %s." % readEvents)
  return S_OK(readEvents)

#############################################################################
def getEventsOutput(logString,writer):
  """  Determine the number of events written out by the supplied writer. The log should contain the string:

       Writer            INFO Events output: x

       If the string is not found an error is returned
  """
  global numberOfEventsOutput
  possibleWriters = ['FSRWriter','DigiWriter','RawWriter','GaussTape','DstWriter','InputCopyStream']
  if not writer in possibleWriters:
    gLogger.error("Requested writer not available.",writer)
    return S_ERROR("Requested writer not available")
  exp = re.compile(r"%s\s+INFO Events output: (\d+)" % writer)
  findline = re.search(exp,logString)
  if not findline:
    gLogger.error("Could not determine events output.")
    return S_ERROR("Could not determine events output")
  writtenEvents = int(findline.group(1))
  gLogger.info("Determined the number of events written to be %s." % writtenEvents)
  numberOfEventsOutput = str(writtenEvents)
  return S_OK(writtenEvents)

#############################################################################
def getEventsProcessed(logString,service):
  """ Determine the number of events reported processed by the supplied service. The log should contain the string:

      Service          SUCCESS x events processed

      If the string is not found an error is returned
  """
  global numberOfEventsInput  
  possibleServices = ['DaVinciInit','DaVinciMonitor','BrunelInit','BrunelEventCount','ChargedProtoPAlg','BooleInit','GaussGen','GaussSim','L0Muon']
  if not service in possibleServices:
    gLogger.error("Requested service not available.",service)
    return S_ERROR("Requested service '%s' not available" % service)
  exp = re.compile(r"%s\s+SUCCESS (\d+) events processed" % service)
  if service.lower()=='l0muon':
    exp = re.compile(r"%s\s+INFO - Total number of events processed\s+:\s+(\d+)" %service)

  findline = re.search(exp,logString)
  if not findline:
    gLogger.error("Could not determine events processed.")
    return S_ERROR("Could not determine events processed")
  eventsProcessed = int(findline.group(1))
  gLogger.info("Determined the number of events processed to be %s." % eventsProcessed)
  numberOfEventsInput = str(eventsProcessed)
  return S_OK(eventsProcessed)

#############################################################################
def checkMooreEvents(logString):
  """ Obtain event information from the application log and determine whether 
      the Moore job generated the correct number of events.
  """
  global firstStepInputEvents
  global dataSummary 
  # Get the last event processed
  lastEvent = getLastEventSummary(logString)['Value']
  firstStepInputEvents = lastEvent

  res = getEventsOutput(logString,'InputCopyStream')
  if not res['OK']:
    return S_ERROR('No Events Processed')

  # Get the number of events processed by Moore
  res = getEventsProcessed(logString,'L0Muon')
  if not res['OK']:
    res = getLastFile(logString)
    if res['OK']:
      lastFile = res['Value']
      dataSummary[lastFile]='ApplicationCrash'
    gLogger.error("Crash in event %s" % lastEvent)
    return S_ERROR('Crash During Execution')
  
  processedEvents = res['Value']
  # Get whether all events in the input file were processed
  noMoreEvents = re.findall('No more events in event selection',logString)

  # If were are to process all the files in the input then ensure that all were read
  if not noMoreEvents:
    return S_ERROR("Not All Input Events Processed")
  # If we are to process a given number of events ensure the target was met
  return S_OK()

#############################################################################
def checkLHCbEvents(logString):
  """ Obtain event information from the application log and determine whether the 
      LHCb job generated the correct number of events.
  """
  global numberOfEventsInput
  global firstStepInputEvents
  # Get the last event processed
  lastEvent = getLastEventSummary(logString)['Value']
  if not lastEvent:
    return S_ERROR('No Events Processed')
  
  numberOfEventsInput = str(lastEvent)
  firstStepInputEvents = numberOfEventsInput

  # Get the number of events output by LHCb
  res = getEventsOutput(logString,'InputCopyStream')
  if not res['OK']:
    return S_ERROR('No Events Processed')
  
  outputEvents = res['Value']
  if outputEvents != lastEvent:
    return S_ERROR("Processed Events Do Not Match")
  # If there were no events processed
  if outputEvents == 0:
    return S_ERROR('No Events Processed')
  return S_OK()  

#############################################################################
def checkGaussEvents(logString):
  """ Obtain event information from the application log and determine whether 
      the Gauss job generated the correct number of events.
  """
  # Get the number of requested events
  res = getRequestedEvents(logString)
  if not res['OK']:
    return res
  requestedEvents = res['Value']
  # You must give a requested number of events for Gauss (this is pointless as the job will run forever)
  if not requestedEvents:
    return S_ERROR("Missing Requested Events For Gauss")

  # Get the last event processed
  lastEvent = getLastEvent(logString)['Value']
  # Get the number of events generated by Gauss
  res = getEventsProcessed(logString,'GaussGen')
  if not res['OK']:
    gLogger.error("Crash in event %s" % lastEvent)
    return S_ERROR('Crash During Execution')
  
  generatedEvents = res['Value']
  # Get the number of events processed by Gauss
  res = getEventsOutput(logString,'GaussTape')
  if not res['OK']:
    result = S_ERROR('No Events Output')
  outputEvents = res['Value']

  # Check that the correct number of events were generated
  if generatedEvents != requestedEvents:
    return S_ERROR('Too Few Events Generated')
  # Check that more than 90% of generated events are output
  if outputEvents < 0.9*requestedEvents:
    return S_ERROR('Not Enough Events Generated')
  return S_OK()

#############################################################################
def checkBooleEvents(logString):
  """ Obtain event information from the application log and determine whether the 
      Boole job processed the correct number of events.
  """
  global firstStepInputEvents
  global dataSummary
  # Get the number of requested events
  res = getRequestedEvents(logString)
  if not res['OK']:
    return res
  requestedEvents = res['Value']

  # Get the last event processed
  lastEvent = getLastEvent(logString)['Value']
  firstStepInputEvents = lastEvent

  # Get the number of events processed by Boole
  res = getEventsProcessed(logString,'BooleInit')
  if not res['OK']:
    res = getLastFile(logString)
    if res['OK']:
      lastFile = res['Value']
      dataSummary[lastFile]='ApplicationCrash'
    gLogger.error("Crash in event %s" % lastEvent)
    return S_ERROR('Crash During Execution')
  
  processedEvents = res['Value']
  # Get the number of events output by Boole
  res = getEventsOutput(logString,'DigiWriter')
  if not res['OK']:
    res = getEventsOutput(logString,'RawWriter')
    if not res['OK']:
      return S_ERROR('No Events Output')
  outputEvents = res['Value']
  # Get whether all events in the input file were processed
  noMoreEvents = re.findall('No more events in event selection',logString)

  # If were are to process all the files in the input then ensure that all were read
  if (not requestedEvents) and (not noMoreEvents):
    return S_ERROR("Not All Input Events Processed")
  # If we are to process a given number of events ensure the target was met
  if requestedEvents:
    if requestedEvents != processedEvents:
      return S_ERROR('Too Few Events Processed')
  # Check that the final reported processed events match those logged as processed during execution
  if lastEvent != processedEvents:
    gLogger.verbose('Last reported event %s != processed events %s' %(lastEvent,processedEvents))    
    if processedEvents>100 and lastEvent<0.9*processedEvents:
      return S_ERROR("Processed Events Do Not Match")
  # If there were no events processed
  if processedEvents == 0:
    return S_ERROR("No Events Processed")
  # If the output events are not equal to the processed events be sure there were no failed events
  if outputEvents != processedEvents:
    gLogger.warn('Number of processed events %s does not match output events %s (considered OK for Boole)' %(processedEvents,outputEvents))

  return S_OK()

#############################################################################
def checkBrunelEvents(logString):
  """ Obtain event information from the application log and determine whether the 
      Brunel job processed the correct number of events.
  """
  global firstStepInputEvents
  global dataSummary 
  # Get the number of requested events
  res = getRequestedEvents(logString)
  if not res['OK']:
    return res
  requestedEvents = res['Value']

  # Get the last event processed
  lastEvent = getLastEvent(logString)['Value']
  firstStepInputEvents = lastEvent
    
  # Get the number of events processed by Brunel
  res = getEventsProcessed(logString,'BrunelInit')
  if not res['OK']:
    res = getLastFile(logString)
    if res['OK']:
      lastFile = res['Value']
      dataSummary[lastFile]='ApplicationCrash'
    gLogger.error("Crash in event %s" % lastEvent)
    return S_ERROR('Crash During Execution')

  processedEvents = res['Value']
  # Get the number of events output by Brunel
  res = getEventsOutput(logString,'DstWriter')
  if not res['OK']:
    return S_ERROR('No Events Output')
  outputEvents = res['Value']
  # Get whether all events in the input file were processed
  noMoreEvents = re.findall('No more events in event selection',logString)

  # If were are to process all the files in the input then ensure that all were read
  if (not requestedEvents) and (not noMoreEvents):
    return S_ERROR("Not All Input Events Processed")
  # If we are to process a given number of events ensure the target was met
  if requestedEvents:
    if requestedEvents != processedEvents:
      return S_ERROR('Too Few Events Processed')
  # Check that the final reported processed events match those logged as processed during execution
  if lastEvent != processedEvents:
    gLogger.verbose('Last reported event %s != processed events %s' %(lastEvent,processedEvents))
    if processedEvents>100 and lastEvent<0.9*processedEvents:
      return S_ERROR("Processed Events Do Not Match")
  # If there were no events processed
  if processedEvents == 0:
    return S_ERROR("No Events Processed")
  # If the output events are not equal to the processed events be sure there were no failed events
  if outputEvents != processedEvents:
    gLogger.warn('Number of processed events %s does not match output events %s (considered OK for Brunel)' %(processedEvents,outputEvents))

  return S_OK()

#############################################################################
def checkDaVinciEvents(logString):
  """ Obtain event information from the application log and determine whether the 
      DaVinci job processed the correct number of events.
  """
  global firstStepInputEvents 
  global dataSummary 
  # Get the number of requested events
  res = getRequestedEvents(logString)
  if not res['OK']:
    return res
  requestedEvents = res['Value']

  # Get the last event processed
  lastEvent = getLastEventSummary(logString)['Value']
  firstStepInputEvents = lastEvent

  # Get the number of events processed by DaVinci
  res = getEventsProcessed(logString,'DaVinciInit')
  if not res['OK']:
    res = getLastFile(logString)
    if res['OK']:
      lastFile = res['Value']
      dataSummary[lastFile]='ApplicationCrash'
    gLogger.error("Crash in event %s" % lastEvent)
    return S_ERROR('Crash During Execution')
  
  processedEvents = res['Value']
  # Get whether all events in the input file were processed
  noMoreEvents = re.findall('No more events in event selection',logString)

  # If were are to process all the files in the input then ensure that all were read
  if (not requestedEvents) and (not noMoreEvents):
    return S_ERROR("Not All Input Events Processed")
  # If we are to process a given number of events ensure the target was met
  if requestedEvents:
    if requestedEvents != processedEvents:
      return S_ERROR("Too Few Events Processed")
  return S_OK()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#