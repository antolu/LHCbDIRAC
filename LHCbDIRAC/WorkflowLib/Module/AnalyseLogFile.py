########################################################################
# $Id: AnalyseLogFile.py,v 1.48 2009/04/17 18:37:05 acsmith Exp $
########################################################################

__RCSID__ = "$Id: AnalyseLogFile.py,v 1.48 2009/04/17 18:37:05 acsmith Exp $"

import commands, os, time, smtplib, re, string

from DIRAC.Core.Utilities.Subprocess                     import shellCall

try:
  from DIRAC.FrameworkSystem.Client.NotificationClient     import NotificationClient
except Exception,x:
  from DIRAC.WorkloadManagementSystem.Client.NotificationClient import NotificationClient

try:
  from LHCbSystem.Utilities.ProductionData  import getLogPath,constructProductionLFNs
except Exception,x:
  from DIRAC.LHCbSystem.Utilities.ProductionData  import getLogPath,constructProductionLFNs

#from DIRAC.Core.Utilities                                import Mail, List
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog
from DIRAC.DataManagementSystem.Client.ReplicaManager    import ReplicaManager
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from WorkflowLib.Utilities.Tools                         import *
from WorkflowLib.Module.ModuleBase                       import *
from DIRAC import                                        S_OK, S_ERROR, gLogger, gConfig


class AnalyseLogFile(ModuleBase):

  def __init__(self):
      self.log = gLogger.getSubLogger("AnalyseLogFile")
      self.version = __RCSID__
      self.site = gConfig.getValue('/LocalSite/Site','localSite')
      self.systemConfig = ''
      self.result = S_ERROR()
      self.mailadress = ''
      self.numberOfEventsInput = ''
      self.numberOfEventsOutput = ''
      self.numberOfEvents = ''
      self.applicationName = ''
      self.inputData = ''
      self.InputData = ''
      self.sourceData = ''
      self.JOB_ID = ''
      self.jobID = ''
      if os.environ.has_key('JOBID'):
        self.jobID = os.environ['JOBID']
      self.poolXMLCatName = 'pool_xml_catalog.xml'
      self.applicationLog = ''
      self.applicationVersion = ''
      self.logFilePath = ''

  def resolveInputVariables(self):
    """ By convention any workflow parameters are resolved here.
    """
    self.log.verbose(self.workflow_commons)
    self.log.verbose(self.step_commons)
    #Use LHCb utility for local running via jobexec
    if self.workflow_commons.has_key('LogFilePath'):
      self.logFilePath = self.workflow_commons['LogFilePath']
    else:
      self.log.info('LogFilePath parameter not found, creating on the fly')
      result = getLogPath(self.workflow_commons)
      if not result['OK']:
        self.log.error('Could not create LogFilePath',result['Message'])
        return result
      self.logFilePath=result['Value']['LogFilePath'][0]
    return S_OK()

  def execute(self):
      self.log.info('Initializing '+self.version)
      result = self.resolveInputVariables()
      if not result['OK']:
        self.log.error(result['Message'])
        return result

      if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
        if self.stepStatus.has_key('Message'):
          if not self.stepStatus['Message'] == 'Application not found':
            self.log.info('Skip this module, failure detected in a previous step :')
            self.log.info('Workflow status : %s' %(self.workflowStatus))
            self.log.info('Step Status %s' %(self.stepStatus))
            return S_OK()
        if self.workflowStatus.has_key('Message'):
          if not self.workflowStatus['Message'] == 'Application not found':
            self.log.info('Skip this module, failure detected in a previous step :')
            self.log.info('Workflow status : %s' %(self.workflowStatus))
            self.log.info('Step Status %s' %(self.stepStatus))
            return S_OK()
      self.log.info( "Analyse Log File for %s" %(self.applicationLog) )
      self.site = gConfig.getValue('/LocalSite/Site','Site')

      # Check the log file exists and get the contents
      res = self.getLogString()
      if not res['OK']:
        self.log.error(result['Message'])
        self.update_status(self.inputs, "Unused")
        return res

      ########## DO SOMETHING HERE ###############
      if self.step_commons.has_key('inputData'):
         self.inputData = self.step_commons['inputData']
      print self.inputData,'!!!!!!!!!!!!!!!!'
      print self.InputData,'????????????????'
      self.inputs = {}
      if self.InputData:
        for f in self.InputData.split(';'):
          self.inputs[f.replace("LFN:","")] = 'OK'

      # Check that no errors were seen in the log
      res = self.goodJob()
      if not res['OK']:
        self.sendErrorMail(res['Message'])
        self.setApplicationStatus('%s Step Failed' % (self.applicationName))
        return self.update_status(self.inputs, "Unused")
      # Check that the number of events handled is correct
      res = self.nbEvent()
      if not res['OK']:
        self.sendErrorMail(res['Message'])
        self.setApplicationStatus('%s Step Failed' % (self.applicationName))
        return self.update_status(self.inputs, "Unused")
      # If the job was successful Update the status of the files to processed
      self.log.info("AnalyseLogFile - %s is OK" % (self.applicationLog))
      self.setApplicationStatus('%s Step OK' % (self.applicationName))
      return self.update_status(self.inputs, "Processed")

#
#-----------------------------------------------------------------------
#
  def grep(self,filename,string,opt=''):
       fd = open(filename)
       file = fd.readlines()
       if opt == '-l': return file[-1]
 
       n=0
       thisline = ''
       for line in file:
          if line.lower().find(string.lower())!= -1:
             n = n+1
             if opt == '-cl':
                thisline = line
             else:
                thisline = thisline+line
             if opt != '-c' and opt != '-cl':
                return line,n
       if n > 0:
          return thisline,n
       else :
          return line,n

  def update_status(self,inputs,fileStatus):
      result = S_OK()
      for f in inputs.keys():
         stat = inputs[f]
         if stat == 'Problematic':
           self.log.info(f+' is problematic at '+self.site+' - reset as Unused')
           try:
             result = self.setReplicaProblematic(f,self.site,'Problematic')
           except:
             self.log.info('LFC not accessible')
             result = S_ERROR('LFC not accessible')

         elif stat == 'Unused':
           self.log.info(f+' was not processed - reset as Unused')

         elif stat == 'AncestorProblem':
           self.log.info(f+' should not be reprocessed - set to '+stat)

         elif stat == 'ApplicationCrash':
           self.log.info(f+' crashed the application - set to '+stat)

         else:
           if fileStatus == "Processed":
             self.log.info(f+" status set as "+fileStatus)
           else:
             self.log.info(f+" status set as "+fileStatus)
           stat = fileStatus
         try:
           result = self.setFileStatus(int(self.PRODUCTION_ID),f,stat)
         except:
           self.log.info('processing DB not accessible')
           result = S_ERROR('processing DB not accessible')
      return result

  def goodJob(self):
      mailto = 'DIRAC_EMAIL'
      result = S_OK()
# check if the application finish successfully
      self.log.info('Check application ended successfully')
      line,n = self.grep(self.applicationLog,'Application Manager Finalized successfully')
      if n == 0:
        if self.applicationName:
            mailto = self.applicationName.upper()+'_EMAIL'
        return S_ERROR('%s Finalization Error' %self.applicationName)

# trap POOL error to open a file through POOL
      if self.poolXMLCatName:
        catalogfile = self.poolXMLCatName
      else:
        catalogfile = 'pool_xml_catalog.xml'

      catalog = PoolXMLCatalog(catalogfile)
      self.log.info('Check POOL connection error')
      line,poolroot = self.grep(self.applicationLog,'Error: connectDatabase>','-c')
      line1,poolroot1 = self.grep(self.applicationLog,'Error: connectDataIO>','-c')
      line += line1
      poolroot += poolroot1
      if poolroot >= 1:
         for file in line.split('\n'):
            if poolroot > 0:
               if (file.count('PFN=')>0):
                  file_input = file.split('PFN=')[1]
                  if (file_input.count('gfal:guid:')>0):
                     result = S_ERROR('Navigation error from guid via LFC for input file')
                     return result
                  else:
                     cat_guid = catalog.getGuidByPfn(file_input)
                     cat_lfn = catalog.getLfnsByGuid(cat_guid)
                     lfn = cat_lfn['Logical'].replace("LFN:","")
                     # Set the the file as problematic in the input list
                     if self.inputs.has_key(lfn):
                        self.inputs[lfn] = 'Problematic'
                     else:
                        # The problematic file is not an input but a derived file
                        ##### Should put here something for setting replica as problematic in the LFC
                        self.log.warn(lfn+" is problematic, but not a job input file - status not changed")
                        # This means the input files cannot be reset yo Unused due to ancestor problems
                        # Get the status of input files for this job
                        prod = self.projectname
                        job = prod + "_" + self.job_id
                        try:
                           result = self.client().getFilesForJob(prod,job)
                        except:
                           result=  {'Status':'Bad'}
                           self.log.error("ProcessingDB not accessible")
                        if result['Status'] == 'OK':
                           files = result['Files']
                           lfns = [f['LFN'] for f in files]
                           self.log.info("    Input files found:"+str(lfns))
                        else:
                           files = []
                           self.log.warn("    Couldn't get input files from procDB")
                        for lfn in self.inputs.keys():
                           status = 'Processed'
                           for f in files:
                              if f['LFN'] == lfn:
                                 # Check the status of the file in the processingDB
                                 status = f['Status']
                                 break
                           if status == 'Processed':
                              self.inputs[lfn] = 'AncestorProblem'
                           else:
                              self.inputs[lfn] = 'Unused'
#                     self.update_status('Bad',cat_lfn['Logical'])

               poolroot = poolroot-1
         for lfn in self.inputs.keys():
           pfn = catalog.getPfnsByLfn(lfn)
           if not pfn['OK'] and self.inputs[lfn] == 'OK':
             self.inputs[lfn] = 'Unused'

         return S_ERROR(mailto + ' error to connectDatabase')

#  error where you need to find the last open file to mark it as crash...
      dict_app_error = {'Terminating event processing loop due to errors':'Event loop not terminated'}

      for type_error in dict_app_error.keys():
          self.log.info('Check %s' %(dict_app_error[type_error]))
          line,founderror = self.grep(self.applicationLog,type_error)
          if founderror >= 1:
              lastfile = self.find_lastfile()
              if lastfile != "":
#                 errmsg += " - File " + lastfile
                 if self.inputs.has_key(lastfile):
                    self.inputs[lastfile] = "ApplicationCrash"

              return S_ERROR(mailto +' '+type_error)

      dict_app_error = {'Cannot connect to database':'error database connection',\
                        'Could not connect':'CASTOR error connection',\
                        'SysError in <TDCacheFile::ReadBuffer>: error reading from file':'DCACHE connection error',\
                        'Failed to resolve':'IODataManager error',\
                        'Error: connectDataIO':'connectDataIO error',\
                        'Error:connectDataIO':'connectDataIO error',\
                        ' glibc ':'Problem with glibc'}

#      if self.applicationName == 'Gauss':
#          dict_app_error['G4Exception'] = 'Geant4 Exception'

      for type_error in dict_app_error.keys():
          self.log.info('Check %s' %(dict_app_error[type_error]))
          line,founderror = self.grep(self.applicationLog,type_error)
          if founderror >= 1:
              return S_ERROR(mailto +' '+type_error)

      writer_error_list = ['G4Exception','GaussTape failed','Writer failed','Bus error','User defined signal 1','Not found DLL']
      for writer_error in writer_error_list:
         line,n = self.grep(self.applicationLog,writer_error)
         if n == 1:
            result = S_ERROR(mailto +' '+writer_error)
            break

      return result

  def sendErrorMail(self,message):
    genmail = message.split()[0]
    subj = message.replace(genmail,'')
    rm = ReplicaManager()
    try:
        if self.workflow_commons.has_key('emailAddress'):
            mailadress = self.workflow_commons['emailAddress']
    except:
        self.log.error('No EMAIL adress supplied')
        return

    self.mode = gConfig.getValue('/LocalSite/Setup','Setup')
    if self.workflow_commons.has_key('configName'):
       configName = self.workflow_commons['configName']
       configVersion = self.workflow_commons['configVersion']
    else:
       configName = self.applicationName
       configVersion = self.applicationVersion

    if self.workflow_commons.has_key('dataType'):
      job_mode = self.workflow_commons['dataType'].lower()
    else:
      job_mode = 'test'

    self.log.info(' Sending Errors by E-mail to %s' %(mailadress))
    subject = '['+self.site+']['+self.applicationName+'] '+ self.applicationVersion + \
              ": "+subj+' '+self.PRODUCTION_ID+'_'+self.JOB_ID+' JobID='+str(self.jobID)
    msg = 'The Application '+self.applicationName+' '+self.applicationVersion+' had a problem \n'
    msg = msg + 'at site '+self.site+' for platform '+self.systemConfig+'\n'
    msg = msg +'JobID is '+str(self.jobID)+'\n'
    msg = msg +'JobName is '+self.PRODUCTION_ID+'_'+self.JOB_ID+'\n'
    if self.inputData:
      msg = msg + '\n\nInput Data:\n'
      result = constructProductionLFNs(self.workflow_commons)
      if not result['OK']:
        self.log.error('Could not create production LFNs',result['Message'])
        return result

      debugLFNs = result['Value']['DebugLFNs']
      for inputname in self.inputData.split(';'):
        if not self.InputData:
          lfninput = ''
          for lfn in debugLFNs:
            if os.path.basename(lfn)==inputname:
              lfninput = lfn
          if not lfninput:
            return S_ERROR('Could not construct LFN for %s' %inputname)

          if self.jobID:
            guidinput = getGuidFromPoolXMLCatalog(self.poolXMLCatName,inputname)
            result = rm.putAndRegister(lfninput,inputname,'CERN-DEBUG',guidinput,catalog='LcgFileCatalogCombined')
            if not result['OK']:
                self.log.error('could not save the INPUT data file')
            else:
                msg = msg +lfninput+'\n'
          else:
            self.log.info('No WMS JOBID environment variable found, not uploading debug outputs')
        else:
          msg = msg +inputname+'\n'

    if self.applicationLog:
      logurl = 'http://lhcb-logs.cern.ch/storage'+self.logFilePath
      msg = msg + '\n\nLog Files directory for the job:\n'
      msg = msg+logurl+'/\n'
      msg = msg +'\n\nLog File for the problematic step:\n'
      msg = msg+logurl+'/'+ self.applicationLog+'\n'
      msg = msg + '\n\nJob StdOut:\n'
      msg = msg+logurl+'/std.out\n'
      msg = msg +'\n\nJob StdErr:\n'
      msg = msg+logurl+'/std.err\n'

    if os.path.exists('corecomm.log'):
      fd = open('corecomm.log')
      msgtmp = fd.readlines()
      msg = msg + '\n\nCore dump:\n\n'
      for j in msgtmp:
          msg = msg + str(j)

    if not self.jobID:
      self.log.info("The self.jobID varible is not defined, not sending mail")
    else:
      notifyClient = NotificationClient()
      mailadress = 'a.smith@cern.ch'
      self.log.info("Sending crash mail for job to %s" % mailadress)
      res = notifyClient.sendMail(mailadress,subject,msg,'a.smith@cern.ch')
      if not res[ 'OK' ]:
        self.log.warn("The mail could not be sent")

#
#-----------------------------------------------------------------------
#
  def getLogString(self):
    self.log.info("Attempting to open %s" % (self.applicationLog))
    if not os.path.exists(self.applicationLog):
      return S_ERROR('%s is not available' % (self.applicationLog))
    if os.stat(self.applicationLog)[6] == 0:
      return S_ERROR('%s is empty' % (self.applicationLog))
    fd = open(self.applicationLog,'r')
    self.logString = fd.read()
    return S_OK()
#
#-----------------------------------------------------------------------
#
  def nbEvent(self):
    if self.applicationName.lower() == 'gauss':
      return self.checkGaussEvents()
    if self.applicationName.lower() == 'boole':
      return self.checkBooleEvents()
    if self.applicationName.lower() == 'brunel':
      return self.checkBrunelEvents()
    if self.applicationName.lower() == 'davinci':
      return self.checkDaVinciEvents()
    return S_ERROR("Application not known") 
#
#-----------------------------------------------------------------------
#
  def checkGaussEvents(self):
    """ Obtain event information from the application log and determine whether the Gauss job generated the correct number of events.
    """
    mailto = self.applicationName.upper()+'_EMAIL'
    # Get the number of requested events
    res = self.getRequestedEvents()
    if not res['OK']:
      return S_ERROR("%s %s" % (mailto,res['Message']))
    requestedEvents = res['Value']
    # You must give a requested number of events for Gauss (this is pointless as the job will run forever)
    if not requestedEvents:
      return S_ERROR("%s Missing requested events option for job" % mailto)

    # Get the last event processed
    lastEvent = self.getLastEvent()['Value']
    # Get the number of events generated by Gauss
    res = self.getEventsProcessed('GaussGen')
    if not res['OK']:
      return S_ERROR("%s Crash in event %s" % (mailto,lastEvent))
    generatedEvents = res['Value']
    # Get the number of events processed by Gauss
    res = getEventsOutput('GaussTape')
    if not res['OK']:
      result = S_ERROR('%s No events output' % mailto)
    outputEvents = res['Value']

    # Check that the correct number of events were generated
    if generatedEvents != requestedEvents:
      return S_ERROR('%s Too few events generated' % mailto)
    # Check that more than 90% of generated events are output
    if outputEvents < 0.9*requestedEvents:
      return S_ERROR('%s Too few events output' % mailto)
    return S_OK()

  def checkBooleEvents(self):
    """ Obtain event information from the application log and determine whether the Boole job processed the correct number of events.
    """
    mailto = self.applicationName.upper()+'_EMAIL'
    # Get the number of requested events
    res = self.getRequestedEvents()
    if not res['OK']:
      return S_ERROR("%s %s" % (mailto,res['Message']))
    requestedEvents = res['Value']

    # Get the last event processed
    lastEvent = self.getLastEvent()['Value']
    # Get the number of events processed by Boole
    res = self.getEventsProcessed('BooleInit')
    if not res['OK']:
      res = self.getLastFile()
      if res['OK']:
        lastFile = res['Value']
        if self.inputFiles.has_key(lastFile):
          self.inputFiles[lastFile] = "ApplicationCrash"
      return S_ERROR("%s Crash in event %s" % (mailto,lastEvent))
    processedEvents = res['Value']
    # Get the number of events output by Boole
    res = getEventsOutput('DigiWriter')
    if not res['OK']:
      res = self.getEventsOutput('RawWriter')
      if not res['OK']:
        return S_ERROR('%s No events output' % mailto)
    outputEvents = res['Value']
    # Get whether all events in the input file were processed
    noMoreEvents = self.noMoreEvents()['Value']

    # If were are to process all the files in the input then ensure that all were read
    if (not requestedEvents) and (not noMoreEvents):
      return S_ERROR("%s Not all input events processed" % mailto)
    # If we are to process a given number of events ensure the target was met
    if requestedEvents:
      if requestedEvents != processedEvents:
        return S_ERROR('%s Too few events processed' % mailto)
    # Check that the final reported processed events match those logged as processed during execution
    if lastEvent != processedEvents:
      return S_ERROR("%s Processed events do not match" % mailto)
    # If the output events are not equal to the processed events be sure there were no failed events
    if outputEvents != processedEvents:
      pass # TODO: Find out whethere there is a way to find failed events
    return S_OK()

  def checkBrunelEvents(self):
    """ Obtain event information from the application log and determine whether the Brunel job processed the correct number of events.
    """
    mailto = self.applicationName.upper()+'_EMAIL'
    # Get the number of requested events
    res = self.getRequestedEvents()
    if not res['OK']:
      return S_ERROR("%s %s" % (mailto,res['Message']))
    requestedEvents = res['Value']

    # Get the last event processed
    lastEvent = self.getLastEvent()['Value']
    # Get the number of events processed by Brunel
    res = self.getEventsProcessed('BrunelInit')
    if not res['OK']:
      res = self.getLastFile()
      if res['OK']:
        lastFile = res['Value']
        if self.inputFiles.has_key(lastFile):
          self.inputFiles[lastFile] = "ApplicationCrash"
      return S_ERROR("%s Crash in event %s" % (mailto,lastEvent))
    processedEvents = res['Value']
    # Get the number of events output by Brunel
    res = self.getEventsOutput('DstWriter')
    if not res['OK']:
      return S_ERROR('%s No events output' % mailto)
    outputEvents = res['Value']
    # Get whether all events in the input file were processed
    noMoreEvents = self.noMoreEvents()['Value']

    # If were are to process all the files in the input then ensure that all were read
    if (not requestedEvents) and (not noMoreEvents):
      return S_ERROR("%s Not all input events processed" % mailto)
    # If we are to process a given number of events ensure the target was met
    if requestedEvents:
      if requestedEvents != processedEvents:
        return S_ERROR('%s Too few events processed' % mailto)
    # Check that the final reported processed events match those logged as processed during execution
    if lastEvent != processedEvents:
      return S_ERROR("%s Processed events do not match" % mailto)
    # If the output events are not equal to the processed events be sure there were no failed events
    if outputEvents != processedEvents:
      return S_ERROR("%s Processed events not all output" % mailto)
    return S_OK()

  def checkDaVinciEvents(self):
    """ Obtain event information from the application log and determine whether the DaVinci job processed the correct number of events.
    """
    mailto = self.applicationName.upper()+'_EMAIL'
    # Get the number of requested events  
    res = self.getRequestedEvents()
    if not res['OK']:
      return S_ERROR("%s %s" % (mailto,res['Message']))
    requestedEvents = res['Value']
    
    # Get the last event processed
    lastEvent = self.getLastEventSummary()['Value']
    # Get the number of events processed by DaVinci
    res = self.getEventsProcessed('DaVinciInit')
    if not res['OK']:
      res = self.getLastFile()
      if res['OK']:
        lastFile = res['Value']
        if self.inputFiles.has_key(lastFile):
          self.inputFiles[lastFile] = "ApplicationCrash"
      return S_ERROR("%s Crash in event %s" % (mailto,lastEvent))
    processedEvents = res['Value']
    # Get the number of events output by DaVinci
    res = self.getEventsOutput('InputCopyStream')
    if not res['OK']:
      return S_ERROR('%s No events output' % mailto)
    outputEvents = res['Value']
    # Get whether all events in the input file were processed
    noMoreEvents = self.noMoreEvents()['Value']

    # If were are to process all the files in the input then ensure that all were read
    if (not requestedEvents) and (not noMoreEvents):
      return S_ERROR("%s Not all input events processed" % mailto)
    # If we are to process a given number of events ensure the target was met
    if requestedEvents:
      if requestedEvents != processedEvents:
        return S_ERROR("%s Too few events processed" % mailto)
    return S_OK()
#
#-----------------------------------------------------------------------
#
  def getInputFiles(self):
    """ Determine the list of input files accessed from the application log. The log should contain a string like:

        Stream:EventSelector.DataStreamTool_1 Def:DATAFILE='filename'

        In the event that the file name contains LFN: this is removed.
    """
    exp = re.compile(r"Stream:EventSelector.DataStreamTool_1 Def:DATAFILE='(\S+)'")
    files = re.findall(exp,self.logString)
    strippedFiles = []
    for file in files: strippedFiles.append(files.replace('LFN:',''))
    return S_OK(strippedFiles)

  def getOutputFiles(self):
    """ Determine the list of output files opened from the application log. The log should contain a string like:

        Data source: EventDataSvc output: DATAFILE='filename'
    """
    exp = re.compile(r"Data source: EventDataSvc output: DATAFILE='(\S+)'")
    files = re.findall(exp,self.logString)
    strippedFiles = []
    for file in files: strippedFiles.append(files.replace('PFN:',''))
    return S_OK(strippedFiles)

  def getLastFile(self):
    """ Determine the last input file opened from the application log.
    """
    files = self.getInputFiles(self.logString)['Value']
    if files:
      return S_OK(files[-1])
    return S_ERROR("No input files opened")

  def getRequestedEvents(self):
    """ Determine the number of requested events from the application log. The log should contain one of two strings:
      
        Requested to process all events on input file   or
        Requested to process x events
    
        If neither of these strings are found an error is returned
    """
    exp = re.compile(r"Requested to process ([0-9]+|all)")
    findline = re.search(exp,self.logString)
    if not findline:
      self.log.error("Could not determine requested events.")
      return S_ERROR("Could not determine requested events")
    events = findline.group(1)
    if events == 'all':
      requestedEvents = 0
    else:
      requestedEvents = int(events)
    self.log.info("Determined the number of requested events to be %s." % requestedEvents)
    return S_OK(requestedEvents)

  def getLastEvent(self):
    """ Determine the last event handled from the application log. The log should contain the string:

       	Nr. in job = x

        If this string is not found then 0 is returned
    """
    exp = re.compile(r"Nr. in job = ([0-9]+)") 
    list = re.findall(exp,self.logString)
    if not list:
      lastEvent = 0 
    else:
      lastEvent = int(list[-1])
    self.log.info("Determined the number of events handled to be %s." % lastEvent)
    return S_OK(lastEvent)

  def getLastEventSummary(self):
    """ DaVinci does not write out each event but instead give a summary every x events. The log should contain the string:

        Reading Event record

        If this string is not found then 0 is returned
    """
    exp = re.compile(r"Reading Event record ([0-9]+)") 
    list = re.findall(exp,self.logString)
    if not list:
      readEvents = 0
    else:
      readEvents = int(list[-1])
    self.log.info("Determined the number of events read to be %s." % readEvents)
    return S_OK(readEvents)

  def getEventsOutput(self,writer):
    """  Determine the number of events written out by the supplied writer. The log should contain the string:

         Writer            INFO Events output: x

         If the string is not found an error is returned
    """
    possibleWriters = ['FSRWriter','DigiWriter','RawWriter','GaussTape','DstWriter','InputCopyStream']
    if not writer in possibleWriters:
      self.log.error("Requested writer not available.",writer)
      return S_ERROR("Requested writer not available")
    exp = re.compile(r"%s\s+INFO Events output: (\d+)" % writer)
    findline = re.search(exp,self.logString)
    if not findline:
      self.log.error("Could not determine events output.")
      return S_ERROR("Could not determine events output")
    writtenEvents = int(findline.group(1))
    self.log.info("Determined the number of events written to be %s." % writtenEvents)
    return S_OK(writtenEvents)

  def getEventsProcessed(self,service):
    """ Determine the number of events reported processed by the supplied service. The log should contain the string:

        Service          SUCCESS x events processed
  
        If the string is not found an error is returned
    """ 
    possibleServices = ['DaVinciInit','DaVinciMonitor','BrunelInit','BrunelEventCount','ChargedProtoPAlg','BooleInit','GaussGen','GaussSim']
    if not service in possibleServices:
      self.log.error("Requested service not available.",service)
      return S_ERROR("Requested service '%s' not available" % service)
    exp = re.compile(r"%s\s+SUCCESS (\d+) events processed" % service)   
    findline = re.search(exp,self.logString)
    if not findline:
      self.log.error("Could not determine events processed.")
      return S_ERROR("Could not determine events processed")
    eventsProcessed = int(findline.group(1))
    self.log.info("Determined the number of events processed to be %s." % eventsProcessed)
    return S_OK(eventsProcessed)

  def noMoreEvents(self):
    """ Determine whether all the events were processed. The log should contain the string:
     
        No more events in event selection
    """
    found = re.search('No more events in event selection',self.logString)
    if found:
      return S_OK(True)
    return S_OK(False)

