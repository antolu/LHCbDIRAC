########################################################################
# $Id: AnalyseLogFile.py,v 1.16 2008/06/17 09:42:41 joel Exp $
########################################################################
""" Script Base Class """

__RCSID__ = "$Id: AnalyseLogFile.py,v 1.16 2008/06/17 09:42:41 joel Exp $"

import commands, os, time, smtplib

from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.WorkloadManagementSystem.Client.NotificationClient import NotificationClient
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
      self.site = gConfig.getValue('/LocalSite/Site','localSite')
      self.systemConfig = 'None'
      self.result = S_ERROR()
      self.mailadress = 'None'
      self.numberOfEventsInput = None
      self.numberOfEventsOutput = None
      self.numberOfEvents = None
      self.applicationName = None
      self.inputData = None
      self.sourceData = None
      self.JOB_ID = None
      self.jobID = None
      if os.environ.has_key('JOBID'):
        self.jobID = os.environ['JOBID']
      self.timeoffset = 0
      self.jobReport  = RPCClient('WorkloadManagement/JobStateUpdate')
      self.poolXMLCatName = 'pool_xml_catalog.xml'
      self.applicationLog = None
      self.applicationVersion = None

  def execute(self):
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

      inputs = {}
      if self.sourceData:
        for f in self.sourceData.split(';'):
          inputs[f.replace("LFN:","")] = 'OK'

# check the is the logfile exist
      result = self.getLogFile()
      if not result['OK'] :
         self.log.info(result['Message'])
         self.update_status( inputs, "unused")
         return S_ERROR(result['Message'])

# check if this is a good job
      result = self.goodJob()
      self.log.info(result)
      if result['OK']:
         resultnb = self.nbEvent()
         if resultnb['OK']:
           self.log.info(' AnalyseLogFile - %s is OK ' % (self.applicationLog))
           self.setApplicationStatus('%s Step OK' % (self.applicationName))
           resultstatus = self.update_status( inputs, "Processed")
           if resultstatus['OK']:
             return resultnb
           else:
             return resultstatus
         else:
           self.sendErrorMail(resultnb['Message'])
           self.log.info('Checking number of events returned result:\n%s' %(resultnb))
           self.setApplicationStatus('%s Step Failed' % (self.applicationName))
           resultstatus = self.update_status( inputs, "unused")
           if resultstatus['OK']:
             return resultnb
           else:
             return resultstatus
      else:
         self.sendErrorMail(result['Message'])
         self.setApplicationStatus('%s Step Failed' % (self.applicationName))
         resultstatus = self.update_status( inputs, "unused")
         if not resultstatus['OK']:
            result = resultstatus

      return result

  def update_status(self,inputs,fileStatus):
      result = S_OK()
      for f in inputs.keys():
         stat = inputs[f]
         if stat == 'Problematic':
           stat = 'unused'
           self.log.info(f+' is problematic at '+self.site+' - reset as unused')
           try:
             result = self.setReplicaProblematic(f,self.site,'Problematic')
           except:
             self.log.info('LFC not accessible')
             result = S_ERROR('LFC not accessible')
         elif stat == 'unused':
           self.log.info(f+' was not processed - reset as unused')
         elif stat == 'AncestorProblem':
           self.log.info(f+' should not be reprocessed - set to '+stat)
         else:
           if fileStatus == "Processed":
             self.log.info(f+" status set as "+fileStatus)
           else:
             self.log.info(f+" status set as "+fileStatus)
           stat = fileStatus
         try:
           result = self.setFileStatus(self.PRODUCTION_ID,f,stat)
         except:
           self.log.info('processing DB not accessible')
           result = S_ERROR('processing DB not accessible')

      return result

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
         if line.find(string)!= -1:
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
      self.timeoffset = 0
      lastev = 0
      mailto = self.applicationName.upper()+'_EMAIL'
      line,appinit = self.grep(self.applicationLog,'ApplicationMgr    SUCCESS')
      if line.split(' ')[2] == 'UTC':
          self.timeoffset = 3

      lEvtMax,n = self.grep(self.applicationLog,'.EvtMax','-cl')
      if n == 0:
          if self.applicationName != 'Gauss':
              EvtMax = -1
          else:
              result = S_ERROR(mailto + ' missing job options')
              return result
      else:
         EvtMax = int(string.split(string.replace(lEvtMax,';',' '))[2])

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

      result = S_OK()

      line,nomore = self.grep(self.applicationLog,'No more events')
      lprocessed,n =self.grep(self.applicationLog,'events processed')
      if n == 0:
          if self.applicationName == 'Gauss' or nomore == 0:
            result = S_ERROR(mailto + ' crash in event ' + lastev)
            self.log.info('nbEvent - result = ',result['Message'])
            return result
          else:
            nprocessed = lastev
      else:
         if len(string.split(lprocessed))-self.timeoffset == 3:
            nprocessed = int(string.split(lprocessed)[0+self.timeoffset])
         else:
            nprocessed = int(string.split(lprocessed)[2+self.timeoffset])

      self.log.info(" %s events processed " % nprocessed)
      self.numberOfEventsInput = str(nprocessed)

      #report job parameter with timestamp
      curTime = time.asctime(time.gmtime())
      report = 'Events processed by %s on %s [UTC]' %(self.applicationName,curTime)
      self.setJobParameter(report,nprocessed)

# find number of events written
      loutput,n = self.grep(self.applicationLog,'Events output:')
      if n == 0:
         if self.applicationName == 'Gauss ' or self.applicationName == 'Brunel':
            result = S_ERROR('no events written')
      else:
         noutput = int(string.split(loutput)[4+self.timeoffset])
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
      if self.poolXMLCatName != None:
        catalogfile = self.poolXMLCatName
      else:
        catalogfile = 'pool_xml_catalog.xml'

      catalog = PoolXMLCatalog(catalogfile)
      self.log.info('Check POOL connection error')
      line,poolroot = self.grep(self.applicationLog,'Error: connectDatabase>','-c')
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
                     if inputs.has_key(lfn):
                        inputs[lfn] = 'Problematic'
                     else:
                        # The problematic file is not an input but a derived file
                        ##### Should put here something for setting replica as problematic in the LFC
                        self.log.warn(lfn+" is problematic, but not a job input file - status not changed")
                        # This means the input files cannot be reset yo unused due to ancestor problems
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
                           self.log.warn("    Coundn't get input files from procDB")
                        for lfn in inputs.keys():
                           status = 'Processed'
                           for f in files:
                              if f['LFN'] == lfn:
                                 # Check the status of the file in the processingDB
                                 status = f['Status']
                                 break
                           if status == 'Processed':
                              inputs[lfn] = 'AncestorProblem'
                           else:
                              inputs[lfn] = 'unused'
#                     self.update_status('Bad',cat_lfn['Logical'])

               poolroot = poolroot-1
         for lfn in inputs.keys():
           pfn = catalog.getPfnsByLfn(lfn)
           if not pfn['Status'] == 'OK' and inputs[lfn] == 'OK':
             inputs[lfn] = 'unused'

         return S_ERROR(mailto + ' error to connectDatabase')

# trap CASTOR error
      self.log.info('Check error database connection')
      line,castor = self.grep(self.applicationLog,'Cannot connect to database')
      if castor >= 1:
         return S_ERROR(mailto + ' Could not connect to database')

      self.log.info('Check CASTOR error connection')
      line,castor = self.grep(self.applicationLog,'Could not connect')
      if castor >= 1:
         return S_ERROR(mailto + ' Could not connect to a file')

      self.log.info('Check DCACHE connection error')
      line,tread = self.grep(self.applicationLog,'SysError in <TDCacheFile::ReadBuffer>: error reading from file')
      if tread >= 1:
         return S_ERROR(mailto + ' TDCacheFile error')

      self.log.info('Check IODataManager error')
      line,resolv = self.grep(self.applicationLog,'Failed to resolve')
      if resolv >= 1:
         self.log.debug(line)
         return S_ERROR(mailto + ' IODataManager error')

      self.log.info('Check connectionIO error')
      line,cdio = self.grep(self.applicationLog,'Error: connectDataIO')
      if cdio >= 1:
         return S_ERROR(mailto + ' connectDataIO error')

      self.log.info('Check connectionIO error')
      line,cdio = self.grep(self.applicationLog,'Error:connectDataIO')
      if cdio >= 1:
         return S_ERROR(mailto + ' connectDataIO error')

      self.log.info('Check loop errors')
      linenextevt,n = self.grep(self.applicationLog,'Terminating event processing loop due to errors')
      if n != 0:
          return S_ERROR(mailto + 'Event loop no terminated')

      self.log.info('GLIBC error')
      linenextevt,n = self.grep(self.applicationLog,' glibc ')
      if n != 0:
          return S_ERROR(mailto + 'Problem with glibc ')

      writerr = 'Writer failed'
      if self.applicationName == 'Gauss':
         writerr = 'GaussTape failed'

      line,n = self.grep(self.applicationLog,writerr)
      if n == 1:
         result = S_ERROR(mailto + ' POOL error')
      else:
         line,n = self.grep(self.applicationLog,'bus error')
         if n == 1:
            result = S_ERROR(mailto + ' bus error')
         else:
            line,n = self.grep(self.applicationLog,'User defined signal 1')
            if n == 1:
               result = S_ERROR(mailto + ' User defined signal 1')
            else:
               line,n = self.grep(self.applicationLog,'Not found DLL')
               if n == 1:
                  result = S_ERROR(mailto + ' Not found DLL')

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
    try:
        if self.workflow_commons.has_key('emailAddress'):
            mailadress = self.workflow_commons['emailAddress']
    except:
        self.log.error('No EMAIL adress supplied')
        return

    self.log.info(' Sending Errors by E-mail to %s' %(mailadress))
    subject = '['+self.site+']['+self.applicationName+'] '+ self.applicationVersion + \
              ": "+subj+' '+self.PRODUCTION_ID+'_'+self.JOB_ID+' JobID='+str(self.jobID)
    msg = 'new'
    msg = msg + 'The Application '+self.applicationName+' '+self.applicationVersion+' had a problem \n'
    msg = msg + 'at site '+self.site+' for platform '+self.systemConfig+'\n'
    msg = msg +'JobID is '+str(self.jobID)+'\n'
    msg = msg +'JobName is '+self.PRODUCTION_ID+'_'+self.JOB_ID+'\n'
    if self.inputData:
      msg = msg + '\n\nInput Data:\n'
      for inputname in self.inputData.split(';'):
        msg = msg +inputname+'\n'

    self.mode = gConfig.getValue('/LocalSite/Setup','Setup')
    self.LFN_ROOT= getLFNRoot(self.sourceData)
    logpath = makeProductionPath(self.JOB_ID,self.LFN_ROOT,'LOG',self.mode,self.PRODUCTION_ID,log=True)

    if self.applicationLog:
      logse = gConfig.getOptions('/Resources/StorageElements/LogSE')
      logurl = 'http://lhcb-logs.cern.ch/storage'+logpath
      msg = msg + '\n\nLog Files directory for the job:\n'
      msg = msg+logurl+'/'+ self.JOB_ID+'/\n'
      msg = msg +'\n\nLog File for the problematic step:\n'
      msg = msg+logurl+'/'+ self.JOB_ID+'/'+ self.applicationLog+'\n'
      msg = msg + '\n\nJob StdOut:\n'
      msg = msg+logurl+'/'+ self.JOB_ID+'/std.out\n'
      msg = msg +'\n\nJob StdErr:\n'
      msg = msg+logurl+'/'+ self.JOB_ID+'/std.err\n'

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

