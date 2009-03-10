########################################################################
# $Id: AnalyseLogFile.py,v 1.40 2009/03/10 23:43:42 paterson Exp $
########################################################################
""" Script Base Class """

__RCSID__ = "$Id: AnalyseLogFile.py,v 1.40 2009/03/10 23:43:42 paterson Exp $"

import commands, os, time, smtplib, re, string

from DIRAC.Core.Utilities.Subprocess                     import shellCall

try:
  from DIRAC.FrameworkSystem.Client.NotificationClient     import NotificationClient
except Exception,x:
  from DIRAC.WorkloadManagementSystem.Client.NotificationClient import NotificationClient

try:
  from LHCbSystem.Utilities.ProductionData  import getLogPath
except Exception,x:
  from DIRAC.LHCbSystem.Utilities.ProductionData  import getLogPath

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
      self.notify = NotificationClient()

      if self.step_commons.has_key('inputData'):
         self.inputData = self.step_commons['inputData']

      if (int(self.JOB_ID) > 200):
         comm = 'rm -f *monitor*'
         self.log.info("Removing Monitor file(s)")
         res = shellCall(0,comm)

      if self.OUTPUT_MAX:
         self.max_app = self.OUTPUT_MAX
      else:
         self.max_app = 'None'

      self.inputs = {}
#      if self.sourceData:
#        for f in self.sourceData.split(';'):
#          inputs[f.replace("LFN:","")] = 'OK'
      if self.InputData:
        for f in self.InputData.split(';'):
          self.inputs[f.replace("LFN:","")] = 'OK'

# check the is the logfile exist
      result = self.getLogFile()
      if not result['OK'] :
         self.log.info(result['Message'])
         self.update_status( self.inputs, "Unused")
         return S_ERROR(result['Message'])

# check if this is a good job
      result = self.goodJob()
      self.log.info(result)
      if result['OK']:
         resultnb = self.nbEvent()
         if resultnb['OK']:
           self.log.info(' AnalyseLogFile - %s is OK ' % (self.applicationLog))
           self.setApplicationStatus('%s Step OK' % (self.applicationName))
           resultstatus = self.update_status( self.inputs, "Processed")
           if resultstatus['OK']:
             return resultnb
           else:
             return resultstatus
         else:
           self.sendErrorMail(resultnb['Message'])
           self.log.info('Checking number of events returned result:\n%s' %(resultnb))
           self.setApplicationStatus('%s Step Failed' % (self.applicationName))
           resultstatus = self.update_status( self.inputs, "Unused")
           if resultstatus['OK']:
             return resultnb
           else:
             return resultstatus
      else:
         self.sendErrorMail(result['Message'])
         self.setApplicationStatus('%s Step Failed' % (self.applicationName))
         resultstatus = self.update_status( self.inputs, "Unused")
         if not resultstatus['OK']:
            result = resultstatus

      return result

  def update_status(self,inputs,fileStatus):
      result = S_OK()
      for f in inputs.keys():
         stat = inputs[f]
         if stat == 'Problematic':
           stat = 'Unused'
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


#
#-----------------------------------------------------------------------
#
  def find_lastfile(self):
      # find out which was the latest open file
      # For most recent Gaudi
      line,n = self.grep(self.applicationLog,'INFO Stream:EventSelector.DataStream', '-cl')
      if n == 0:
         # For old Gaudi (ex Brunel v30)
         line,n = self.grep(self.applicationLog,'INFO Stream:EventSelector_1', '-cl')
      if n:
         lastfile = line.split("'")[1].replace("LFN:","")
      else:
         lastfile = ""

      return lastfile

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

#
#-----------------------------------------------------------------------
#
  def nbEvent(self):
      lastev = 0
      mailto = self.applicationName.upper()+'_EMAIL'

      lEvtMax,n = self.grep(self.applicationLog,'Requested to process','-cl')
      if n == 0:
          if self.applicationName != 'Gauss':
              EvtMax = -1
          else:
              result = S_ERROR(mailto + ' missing job options')
              return result
      else:
#         EvtMax = int(string.split(string.replace(lEvtMax,';',' '))[2])
         exp = re.compile(r"Requested to process ([0-9]+|all)")
         findline = re.search(exp,lEvtMax)
         if findline.group(1) == 'all':
             EvtMax = -1
         else:
             EvtMax = int(findline.group(1))

      self.log.info('EvtMax = %s' % (str(EvtMax)))

      line,nev = self.grep(self.applicationLog,'Reading Event record','-cl')
      if nev == 0:
         line,nev = self.grep(self.applicationLog,'Nr. in job =','-cl')
         if nev == 0:
            result = S_ERROR(mailto + ' no event')
            return result
         else:
            lastev = string.split(line,'=')[1]
      else:
         lastev = string.split(line)[5]

      lastfile = self.find_lastfile()
      result = S_OK()

      line,nomore = self.grep(self.applicationLog,'No more events')
      lprocessed,n =self.grep(self.applicationLog,'events processed')
      if n == 0:
          if self.applicationName == 'Gauss' or nomore == 0:
            errmsg = mailto + ' crash in event %d' % lastev
            if lastfile != "":
                errmsg += " - File " + lastfile
                if self.inputFiles.has_key(lastfile):
                   self.inputFiles[lastfile] = "ApplicationCrash"
            result = S_ERROR(errmsg)
            self.log.info('nbEvent - result = ',result['Message'])
            return result
          else:
            nprocessed = lastev
      else:
         exp = re.compile(r"([0-9]+) events processed")
         findline = re.search(exp,lprocessed)
         nprocessed = int(findline.group(1))

      self.log.info(" %s events processed " % str(nprocessed))
      self.numberOfEventsInput = str(nprocessed)

      #report job parameter with timestamp
      curTime = time.asctime(time.gmtime())
      report = 'Events processed by %s' %(self.applicationName)
      self.setJobParameter(report,nprocessed)

# find number of events written
      noutput = 0
      loutput,n = self.grep(self.applicationLog,'Events output:')
      if n == 0:
         if self.applicationName == 'Gauss ' or self.applicationName == 'Brunel':
            result = S_ERROR('no events written')
      else:
         exp = re.compile(r"Events output: ([0-9]+)")
         findline = re.search(exp,loutput)
         noutput = int(findline.group(1))
         self.log.info(" %s events written " % str(noutput))
         self.numberOfEventsOutput = str(noutput)

      if nprocessed == EvtMax or nomore == 1:
        if noutput != nprocessed:
          if self.applicationName == 'Gauss' or self.applicationName == 'Brunel':
             result = S_ERROR(mailto + ' too few events on output')
             if self.applicationName == 'Gauss' and (nprocessed-noutput) < EvtMax/10:
               result = S_OK()
      else:
        if EvtMax != -1 and nprocessed != EvtMax:
          self.log.error("Number of events processed "+str(nprocessed)+" is less than requested "+str(EvtMax))
          result = S_ERROR('Too few events processed')
        elif nomore != 1 and EvtMax == -1:
          self.log.error("Number of events processed "+str(nprocessed)+", the end of input not reached")
          file_end = False
          linenextevt,n = self.grep(self.applicationLog,'Failed to receieve the next event')
          self.log.info('failed next event %s' %( str(n)))
          if n == 0:
            file_end = True
          if file_end == False:
            result = S_ERROR('All INPUT events have not been processed')
          else:
            result = S_OK('All INPUT events have been processed')

      return result


#
#-----------------------------------------------------------------------
#
  def goodJob(self):
      mailto = 'DIRAC_EMAIL'
      result = S_OK()
      # check if the logfile contain timestamp information
# check if the application finish successfully
      self.log.info('Check application ended successfully')
      line,n = self.grep(self.applicationLog,'Application Manager Finalized successfully')
      if n == 0:
        if self.applicationName:
            mailto = self.applicationName.upper()+'_EMAIL'
        return S_ERROR(mailto+' not finalized')

# trap POOL error to open a file through POOL
      if not self.poolXMLCatName:
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
      dict_app_error = {'Terminating event processing loop due to errors':'Event loop no terminated'}

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

      Geanterr = 'G4Exception'
      writerr = 'Writer failed'
      if self.applicationName == 'Gauss':
         writerr = 'GaussTape failed'

      writer_error_list = [writerr,'Bus error','User defined signal 1','Not found DLL']
      for writer_error in writer_error_list:
         line,n = self.grep(self.applicationLog,writer_error)
         if n == 1:
            result = S_ERROR(mailto +' '+writer_error)
            break

      return result

#
#-----------------------------------------------------------------------
#
  def getLogFile(self):
    self.log.debug(' OpenLogFile - try to open %s' %(self.applicationLog))

    result = S_OK()
    if not os.path.exists(self.applicationLog):
      if os.path.exists(self.applicationLog+'.gz'):
        fn = self.applicationLog+'.gz'
        result = shellCall(0,"gunzip "+fn)
        resultTuple = result['Value']
        if resultTuple[0] > 0:
          self.log.info(resultTuple[1])
          result = S_ERROR('%s is not available' %(self.applicationLog))
      else:
        result = S_ERROR('%s is not available' %(self.applicationLog))
    elif os.stat(self.applicationLog)[6] == 0:
        result = S_ERROR('%s is empty' %(self.applicationLog))

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

          guidinput = getGuidFromPoolXMLCatalog(self.poolXMLCatName,inputname)
          result = rm.putAndRegister(lfninput,inputname,'CERN-DEBUG',guidinput)
          if not result['OK']:
              self.log.error('could not save the INPUT data file')
          else:
              msg = msg +lfninput+'\n'
        else:
          msg = msg +inputname+'\n'

    if self.applicationLog:
      logse = gConfig.getOptions('/Resources/StorageElements/LogSE')
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

    result = self.notify.sendMail(mailadress,subject,msg,'joel.closier@cern.ch')
    self.log.info(result)
    if not result[ 'OK' ]:
        self.log.warn( "The mail could not be sent" )


  def checkApplicationLog(self,error):
    self.log.debug(' applicationLog - from %s'%(self.applicationLog))
    self.log.info(error)

