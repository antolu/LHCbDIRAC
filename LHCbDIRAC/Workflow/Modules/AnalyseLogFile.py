########################################################################
# $Id$
########################################################################

__RCSID__ = "$Id$"

import os, re, string, glob

from DIRAC.FrameworkSystem.Client.NotificationClient     import NotificationClient
from DIRAC.Resources.Catalog.PoolXMLFile                 import getGUID
from DIRAC.DataManagementSystem.Client.ReplicaManager    import ReplicaManager

from LHCbDIRAC.Core.Utilities.ProductionLogAnalysis      import analyseLogFile
from LHCbDIRAC.Core.Utilities.ProductionData             import getLogPath,constructProductionLFNs
from LHCbDIRAC.Workflow.Modules.ModuleBase               import ModuleBase

from DIRAC import S_OK, S_ERROR, gLogger
import DIRAC

class AnalyseLogFile(ModuleBase):

  #############################################################################
  def __init__(self):
    """Module initialization.
    """
    self.log = gLogger.getSubLogger('AnalyseLogFile')
    self.version = __RCSID__
    self.site = DIRAC.siteName()
    self.systemConfig = ''
    self.numberOfEventsInput = ''
    self.numberOfEventsOutput = ''
    self.numberOfEvents = ''
    self.applicationName = ''
    self.JOB_ID = '' #default for a prod workflow
    self.PRODUCTION_ID = '' #default for a prod workflow
    self.applicationLog = ''
    self.applicationVersion = ''
    self.logFilePath = ''
    self.coreFile = ''
    self.logString = ''
    
    self.jobID = ''
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']    
    #Resolved to be the input data of the current step
    self.stepInputData = []
    #Dict of input data for the job and status
    self.jobInputData = {}

  #############################################################################
  def resolveInputVariables(self):
    """ By convention any workflow parameters are resolved here.
    """
    self.log.debug(self.workflow_commons)
    self.log.debug(self.step_commons)

    if self.workflow_commons.has_key('SystemConfig'):
      self.systemConfig = self.workflow_commons['SystemConfig']

    if not self.step_commons.has_key('applicationName'):
      return S_ERROR('Step does not have an applicationName')

    self.applicationName = self.step_commons['applicationName']

    if self.workflow_commons.has_key('InputData'):
      if self.workflow_commons['InputData']:
        self.jobInputData = self.workflow_commons['InputData']

    if self.step_commons.has_key('inputData'):
      if self.step_commons['inputData']:
        self.stepInputData = self.step_commons['inputData']

    if self.stepInputData:
      self.log.info('Input data defined in workflow for this Gaudi Application step')
      if type(self.stepInputData) != type([]):
        self.stepInputData = self.stepInputData.split(';')
    
    if self.jobInputData:
      self.log.info('All input data for workflow taken from JDL parameter')
      if type(self.jobInputData) != type([]):
        self.jobInputData = self.jobInputData.split(';')
      jobStatusDict = {}
      #clumsy but now make this a dictionary with default "OK" status for all input data
      for lfn in self.jobInputData:
        jobStatusDict[lfn.replace('LFN:','')] = 'OK'      
      self.jobInputData = jobStatusDict
    else:
      self.log.verbose('Job has no input data requirement')

    if self.step_commons.has_key('applicationVersion'):
      self.applicationVersion = self.step_commons['applicationVersion']
    
    if self.step_commons.has_key('applicationLog'):    
      self.applicationLog = self.step_commons['applicationLog']

    if self.step_commons.has_key('inputDataType'):
      self.inputDataType = self.step_commons['inputDataType']

    if self.step_commons.has_key('numberOfEvents'):
      self.numberOfEvents = self.step_commons['numberOfEvents']

    if self.step_commons.has_key('numberOfEventsInput'):
      self.numberOfEventsInput = self.step_commons['numberOfEventsInput']

    if self.step_commons.has_key('numberOfEventsOutput'):
      self.numberOfEventsOutput = self.step_commons['numberOfEventsOutput']

    #Use LHCb utility for local running via jobexec
    if self.workflow_commons.has_key('LogFilePath'):
      self.logFilePath = self.workflow_commons['LogFilePath']
      if type(self.logFilePath)==type([]):
        self.logFilePath = self.logFilePath[0]      
    else:
      self.log.info('LogFilePath parameter not found, creating on the fly')
      result = getLogPath(self.workflow_commons)
      if not result['OK']:
        self.log.error('Could not create LogFilePath',result['Message'])
        return result
      self.logFilePath=result['Value']['LogFilePath'][0]

    return S_OK()

  #############################################################################
  def execute(self):
    """ Main execution method. 
    """
    self.log.info('Initializing %s' %(self.version))
    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.error(result['Message'])
      return result

    if DIRAC.siteName() == 'DIRAC.ONLINE-FARM.ch':
      if not self.numberOfEventsInput or not self.numberOfEventsOutput:
        self.log.error('Number of events input = %s, number of events output = %s' %(self.numberOfEventsInput,self.numberOfEventsOutput))
        return S_ERROR()
      return S_OK()

    if self.workflow_commons.has_key('AnalyseLogFilePreviouslyFinalized'):
      self.log.info('AnalyseLogFile has already run for this workflow and finalized with sending an error email')
      return S_OK()

    self.log.verbose("Performing log file analysis for %s" %(self.applicationLog) )
    # Resolve the step and job input data
    self.log.info('Resolved the step input data to be:\n%s' %(string.join(self.stepInputData,'\n')))    
    self.log.info('Resolved the job input data to be:\n%s' %(string.join(self.jobInputData.keys(),'\n')))

    #First check for the presence of any core dump files caused by an abort of some kind
    for file in os.listdir('.'):
      if re.search('^core.[0-9]+$',file):
        self.coreFile = file
        self.log.error('Found a core dump file in the current working directory: %s' %(self.coreFile))
        self.finalizeWithErrors('Found a core dump file in the current working directory: %s' %(self.coreFile))
        self.updateFileStatus(self.jobInputData,'ApplicationCrash')
        # return S_OK if the Step already failed to avoid overwriting the error
        if not self.stepStatus['OK']: return S_OK()
        
        return S_ERROR('%s %s Core Dump' %(self.applicationName,self.applicationVersion))        

    result = analyseLogFile(self.applicationLog,self.applicationName)
    if not result['OK']:
      self.log.error(result)
      if result.has_key('Data'):
        fNameStatusDict = result['Data']
        fNameLFNs = {}
        for lfn in self.jobInputData.keys():
          for fName,status in fNameStatusDict.items():
            if os.path.basename(lfn)==fName:
              fNameLFNs[lfn]=status
        for lfn,status in fNameLFNs.items():
          self.jobInputData[lfn]=status
      
      self.finalizeWithErrors(result['Message'])
      self.updateFileStatus(self.jobInputData, "Unused")
      # return S_OK if the Step already failed to avoid overwriting the error
      if not self.stepStatus['OK']: return S_OK()
      self.setApplicationStatus(result['Message'])
      return result

    # if the log looks ok but the step already failed, preserve the previous error
    if not self.stepStatus['OK']:
      self.updateFileStatus(self.jobInputData, "Unused")
      return S_OK()

    if result.has_key('numberOfEventsInput'):
      self.numberOfEventsInput=result['numberOfEventsInput']
      self.log.info('Setting numberOfEventsInput to %s' %(self.numberOfEventsInput))
      
    if result.has_key('numberOfEventsOutput'):
      self.numberOfEventsOutput=result['numberOfEventsOutput']
      self.log.info('Setting numberOfEventsOutput to %s' %(self.numberOfEventsOutput))

    if result.has_key('FirstStepInputEvents'):
      if not self.workflow_commons.has_key('FirstStepInputEvents'):
        firstStepInputEvents = result['FirstStepInputEvents']
        self.workflow_commons['FirstStepInputEvents'] = firstStepInputEvents
        self.log.info('Setting FirstStepInputEvents to %s' %(firstStepInputEvents))

    # If the job was successful Update the status of the files to processed
    self.log.info('Log file %s, %s' %(self.applicationLog,result['Value']))
    self.setApplicationStatus('%s Step OK' % (self.applicationName))
    
    self.step_commons['numberOfEventsInput'] = self.numberOfEventsInput
    self.step_commons['numberOfEventsOutput'] = self.numberOfEventsOutput
    return self.updateFileStatus(self.jobInputData, "Processed")

  #############################################################################
  def updateFileStatus(self,inputs,defaultStatus):
    """ Allows to update file status to a given default, important statuses are
        not overwritten.
    """
    for fileName in inputs.keys():
      stat = inputs[fileName]
      if stat == "Problematic":
        res = self.setReplicaProblematic(fileName,self.site,'Problematic')
        if not res['OK']:
          gLogger.error("Failed to update replica status to problematic",res['Message'])
        self.log.info('%s is problematic at %s - reset as Unused' % (fileName,self.site))
        stat = "Unused"
      elif stat in ['Unused','ApplicationCrash']:
        self.log.info("%s will be updated to status '%s'" % (fileName,stat))
      else:
        stat = defaultStatus
        self.log.info("%s will be updated to default status '%s'" % (fileName,defaultStatus))
      self.setFileStatus(int(self.PRODUCTION_ID),fileName,stat)
    return S_OK()

  #############################################################################
  def finalizeWithErrors(self,subj):
    """ Method that sends an email and uploads intermediate job outputs.
    """
    self.workflow_commons['AnalyseLogFilePreviouslyFinalized']=True
    #Have to check that the output list is defined in the workflow commons, this is
    #done by the first BK report module that executes at the end of a step but in 
    #this case the current step 'listoutput' must be added.
    if self.workflow_commons.has_key('outputList'):
      self.workflow_commons['outputList'] = self.workflow_commons['outputList'] + self.step_commons['listoutput']
    else:
      self.workflow_commons['outputList'] = self.step_commons['listoutput']

    result = constructProductionLFNs(self.workflow_commons)
    if not result['OK']:
      self.log.error('Could not create production LFNs with message "%s"' %(result['Message']))
      return result
    
    if not result['Value'].has_key('DebugLFNs'):
      self.log.error('No debug LFNs found after creating production LFNs, result was:%s' %result)
      return S_ERROR('DebugLFNs Not Found')
    
    debugLFNs = result['Value']['DebugLFNs']
    
    #This has to be reviewed... 
    rm = ReplicaManager()
    try:
      if self.workflow_commons.has_key('emailAddress'):
        mailAddress = self.workflow_commons['emailAddress']
    except:
      self.log.error('No email address supplied')
      return S_ERROR()

    self.log.verbose('Will send errors by E-mail to %s' %(mailAddress))
    
    subject = '['+self.site+']['+self.applicationName+'] '+ self.applicationVersion + \
              ": "+subj+' '+self.PRODUCTION_ID+'_'+self.JOB_ID+' JobID='+str(self.jobID)
    msg = 'The Application '+self.applicationName+' '+self.applicationVersion+' had a problem \n'
    msg = msg + 'at site '+self.site+' for platform '+self.systemConfig+'\n'
    msg = msg +'JobID is '+str(self.jobID)+'\n'
    msg = msg +'JobName is '+self.PRODUCTION_ID+'_'+self.JOB_ID+'\n'

    if self.coreFile:
      self.log.info('Will attempt to upload core dump file: %s' %self.coreFile)
      msg += '\n\nCore file found:\n'
      coreLFN = ''
      for lfn in debugLFNs:
        if re.search('[0-9]+_core',os.path.basename(lfn)):
          coreLFN = lfn
      if coreLFN and self.jobID:
        if self.jobID:
          self.log.info('Attempting: rm.putAndRegister("%s","%s","CERN-DEBUG","catalog="LcgFileCatalogCombined"' %(coreLFN,self.coreFile))          
          result = rm.putAndRegister(coreLFN,self.coreFile,'CERN-DEBUG',catalog='LcgFileCatalogCombined')
          self.log.info(result)
          if not result['OK']:
            self.log.error('Could not save core dump file',result['Message'])
            msg += 'Could not save dump file with message "%s"\n' % result['Message']
          else:
            msg += coreLFN+'\n'
        else:
          self.log.info('JOBID is null, would have attempted to upload: LFN:%s, file %s to CERN-DEBUG' %(coreLFN,self.coreFile))

    toUpload = {}
    for lfn in debugLFNs:
      if os.path.exists(os.path.basename(lfn)):
        toUpload[os.path.basename(lfn)]=lfn

    if toUpload:
      msg += '\n\nIntermediate job data files:\n'

    for fname,lfn in toUpload.items():      
      guidResult = getGUID(fname)
      guidInput = ''
      if not guidResult['OK']:
        self.log.error('Could not find GUID for %s with message' %(fname),guidResult['Message'])
      elif guidResult['generated']:
        self.log.info('PoolXMLFile generated GUID(s) for the following files ',string.join(guidResult['generated'],', '))
        guidInput = guidResult['Value'][fname]
      else:
        guidInput = guidResult['Value'][fname]      
      
      if self.jobID:
        self.log.info('Attempting: rm.putAndRegister("%s","%s","CERN-DEBUG","%s","catalog="LcgFileCatalogCombined"' %(fname,lfn,guidInput))                  
        result = rm.putAndRegister(lfn,fname,'CERN-DEBUG',guidInput,catalog='LcgFileCatalogCombined')
        self.log.info(result)
        if not result['OK']:
          self.log.error('Could not save INPUT data file with result',str(result))
          msg += 'Could not save intermediate data file %s with result\n%s\n' %(fname,result)
        else:
          msg = msg+lfn+'\n'+str(result)+'\n'
      else:
        self.log.info('JOBID is null, would have attempted to upload: LFN:%s, file %s, GUID %s to CERN-DEBUG' %(lfn,fname,guidInput))    

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

    globList = glob.glob('*coredump.log')
    for check in globList:
      if os.path.isfile(check):
        self.log.verbose('Found locally existing core dump file: %s' %(check))
        fd = open(check)
        contents = fd.read()
        msg = msg + '\n\nCore dump:\n\n' + contents
        fd.close()

    if not self.jobID:
      self.log.info("JOBID is null, *NOT* sending mail, for information the mail was:\n====>Start\n%s\n<====End" %(msg))
    else:
      notifyClient = NotificationClient()
      self.log.info('Sending crash mail for job to %s' %(mailAddress))
      res = notifyClient.sendMail(mailAddress,subject,msg,'joel.closier@cern.ch',localAttempt=False)
      if not res['OK']:
        self.log.warn("The mail could not be sent")

    return S_OK()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#  