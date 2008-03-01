########################################################################
# $Id: LHCbCheckLogFile.py,v 1.14 2008/03/01 07:45:41 joel Exp $
########################################################################
""" Base LHCb Gaudi applications log checking utility """

__RCSID__ = "$Id: LHCbCheckLogFile.py,v 1.14 2008/03/01 07:45:41 joel Exp $"

import os, string,sys

from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from WorkflowLib.Module.CheckLogFile import CheckLogFile
from DIRAC import                                        S_OK, S_ERROR, gLogger, gConfig

#from DIRAC.Utility.ProductionUtilities     import *

class LHCbCheckLogFile(CheckLogFile):

   def __init__(self):

      self.info    = 0
      self.result = S_ERROR()
      self.logfile = 'None'
      self.argv0   = 'LHCbCheckLogFile'
      self.log = gLogger.getSubLogger("LHCbCheckLogFile")
      self.iClient = None
      self.jobID = None
      if os.environ.has_key('JOBID'):
        self.jobID = os.environ['JOBID']
      self.timeoffset = 0
      self.jobReport  = RPCClient('WorkloadManagement/JobStateUpdate')
      self.site = gConfig.getValue('/LocalSite/Site','localSite')
      self.error_message = ''
      self.OUTPUT_MAX = 'None'

   def client(self):
      import xmlrpclib

      err = 'None'

      if self.iClient is None:
         self.mode = gConfig.getValue('/LocalSite/Setup','Setup')
         url = gConfig.getValue('/Systems/ProductionManagement/Development/URLs/ProcessingDB')
         self.server = xmlrpclib.Server(url)
         try:
            return self.server
         except Exception, X:
            err = 'Processing Error when calling "'+self.server+'":'+ str(X)
         except:
            err = 'Unknown Processing Error when calling "'+self.server+'"'
         self.log.error( err )


   def update_status(self,status,fileinput='None'):
       ## not yet done by KGG
      self.projectname = self.module.step.job.prod_id
      inputs = []
      if fileinput == 'None':
        if os.environ.has_key('Input_Filename'):
          inputs.append(os.environ['Input_Filename'])
        else:
          inputs = self.module.step.job.inputData()
      else:
        inputs.append(fileinput)
      self.log.info( str( inputs ) )

      for f in inputs:
         if status != 'OK':
            self.log.warn(f+' is problematic')
            self.client().setReplicaStatus(f.replace('LFN:',''),'Problematic',self.site)
         else:
            self.client().setFileStatus(self.projectname,f.replace('LFN:',''),'Processed')

   def execute(self):
      self.site = gConfig.getValue('/LocalSite/Site','Site')

      self.appname = self.getAppName()
      if (int(self.JOB_ID) > 200):
         comm = 'rm -f *monitor*'
         self.log.info("Removing Monitor file(s)")
         res = shellCall(0,comm)


      if self.OUTPUT_MAX:
         self.max_app = self.OUTPUT_MAX
      else:
         self.max_app = 'None'

      if self.max_app != 'None':
         if (int(self.JOB_ID) > int(self.max_app)):
           if self.outputDataSE != None:
             self.outputDataSE = None

#      self.logfile = os.getenv('LOGFILE')
#
#----------------------------------------------------------------------
#
      if self.info:
         self.log.info(self.argv0 +'.CheckLogFile '+self.appLog)

      result = self.getLogFile()
      if not result['OK'] :
         self.log.warn(self.argv0 + '.LogFile - no logfile available - EXIT')
         return S_ERROR(self.argv0 + '.LogFile - no logfile available - EXIT')
#         sys.exit(2)


# check if this is a good job
      result = self.goodJob()
      if result['OK']:
         self.log.info(' CheckLogFile - %s is OK ' % (self.appLog))
         self.__report('%s step OK' % (self.appName))
#         self.update_status('OK')
         return result


      # This is DIRAC problem, no need to proceed further
      if result['Message'].find('DIRAC_EMAIL') != -1 :
        return result

# the job did not finalize properly
# check some other Environment problems (such as POOL)
      error = result['Message']
      result = self.analyzeDIRACEnv(error)
      if result['Message'].find('DIRAC_EMAIL') != -1 :
        return result

      error = result['Message']

# This is probably an application problem
# if the return code = KO then check the logfile for a specific application
      result = self.checkApplicationLog(error)
      self.log.info('Log file checking revealed errors:')
      self.__report('Log file for %s checking revealed errors' % (self.appName))
      self.log.info(result['Message'])
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
#----------------------------------------------------------------------
#

   def goodJob(self):
      self.log.debug(' goodJob: find if the required number of events has been produced in %s'%(self.appLog))

      mailto = 'DIRAC_EMAIL'
      # check if the logfile contain timestamp information
      line,appinit = self.grep(self.appLog,'ApplicationMgr    SUCCESS')
      self.timeoffset = 0
      if line.split(' ')[2] == 'UTC':
          self.timeoffset = 3

      line,poolroot = self.grep(self.appLog,'Error: connectDatabase>','-c')
#      self.module.step.job.addParameter('Number_OF_Files_non_processed',PARAMETER,str(poolroot))
      if poolroot >= 1:
         for file in line.split('\n'):
            if poolroot > 0:
               if(file.count('PFN=')>0):
                  file_input = file.split('PFN=')[1]
                  if(file_input.count('gfal:guid:')>0):
                     result = S_ERROR('Navigation from guid via LFC from input file')
                     return result
                  else:
                     if self.PoolXMLCatalog != None:
                        catalogfile = self.PoolXMLCatalog
                     else:
                        catalogfile = 'pool_xml_catalog.xml'

                     catalog = PoolXMLCatalog(catalogfile)
                     gguid = catalog.getGuidByPfn(file_input)
                     llfn = catalog.getLfnsByGuid(gguid)
                     self.update_status('Bad',llfn['Logical'])

               poolroot = poolroot-1
         return S_ERROR(mailto + ' error to connectDatabase')


      line,castor = self.grep(self.appLog,'Could not connect')
      if castor >= 1:
         return S_ERROR(mailto + ' Could not connect to a file')

      line,tread = self.grep(self.appLog,'SysError in <TDCacheFile::ReadBuffer>: error reading from file')
      if tread >= 1:
         return S_ERROR(mailto + ' TDCacheFile error')

      lEvtMax,n = self.grep(self.appLog,'.EvtMax','-cl')
      if n == 0:
          if self.appName != 'Gauss':
              EvtMax = -1
          else:
              result = S_ERROR(mailto + ' missing job options')
              return result
      else:
         EvtMax = int(string.split(string.replace(lEvtMax,';',' '))[2])


      line,nev = self.grep(self.appLog,'Reading Event record','-c')
      if nev == 0:
         line,nev = self.grep(self.appLog,'Nr. in job =','-c')
         if nev == 0:
            result = S_ERROR(mailto + ' no event')
            return result
         else:
            lastev = string.split(line,'=')[1]
      else:
         lastev = string.split(line)[5]


      mailto = self.appName.upper()+'_EMAIL'
      result = S_OK()

      line,nomore = self.grep(self.appLog,'No more events')
      lprocessed,n =self.grep(self.appLog,'events processed')
      # this line should be present in Gauss and
      # one or the other should be there if not Gauss
      if n == 0:
          if self.appName == 'Gauss' or nomore == 0:
            result = S_ERROR(mailto + ' crash in event ' + lastev)
            self.log.info(' goodJob - result = ',result['Message'])
            return result
          else:
            nprocessed = lastev

      else:
         if len(string.split(lprocessed))-self.timeoffset == 3:
            nprocessed = int(string.split(lprocessed)[0+self.timeoffset])
         else:
            nprocessed = int(string.split(lprocessed)[2+self.timeoffset])

      self.log.info(" %s events processed " % nprocessed)
      self.NUMBER_OF_EVENTS_INPUT = str(nprocessed)

      if nprocessed == EvtMax or nomore == 1:
         line,n = self.grep(self.appLog,'Application Manager Finalized successfully')
         if n == 0 and self.appName != 'Boole':
            result = S_ERROR(mailto + ' not finalized')
            return result
         else:
            loutput,n = self.grep(self.appLog,'Events output:')
            if n == 0:
               if self.appName == 'Gauss ' or self.appName == 'Brunel':
                  result = S_ERROR()
            else:
               noutput = int(string.split(loutput)[4+self.timeoffset])
               self.log.info(" %s events written " % str(noutput))
               self.NUMBER_OF_EVENTS_OUTPUT = str(noutput)

               if noutput != nprocessed:
                  if self.appName == 'Gauss' or self.appName == 'Brunel':
                     result = S_ERROR(mailto + ' too few events on output')
                     if self.appName == 'Gauss' and (nprocessed-noutput) < EvtMax/10:
                       result = S_OK()
      else:
        if EvtMax != -1 and nprocessed != EvtMax:
          self.log.error("Number of events processed "+str(nprocessed)+" is less than requested "+str(EvtMax))
          result = S_ERROR('Too few events processed')
        elif nomore != 1 and EvtMax == -1:
          self.log.error("Number of events processed "+str(nprocessed)+", the end of input not reached")
          loutput,n = self.grep(self.appLog,'Events output:')
          noutput = int(string.split(loutput)[4+self.timeoffset])
          self.NUMBER_OF_EVENTS_OUTPUT = str(noutput)
          file_end = False
          linenextevt,n = self.grep(self.appLog,'Failed to receieve the next event')
          if n == 0:
            file_end = True
          linenextevt,n = self.grep(self.appLog,'Terminating event processing loop due to errors')
          if n == 0:
            file_end = True
          if file_end == False:
            result = S_ERROR('All INPUT events have not been processed')
          else:
            result = S_OK('All INPUT events have been processed')

      self.log.debug(' goodJob - %s events result= %s'%(str(noutput),str(result)))

      return result

 #
#-----------------------------------------------------------------------
#
   def analyzeDIRACEnv(self,error):

      self.log.debug(' DIRACEnv: analyze the DIRAC environment from %s logfile '%(self.appLog))

      result = S_ERROR(error)

      mailto = 'DIRAC_EMAIL'
      writerr = 'Writer failed'
      if self.appName == 'Gauss':
         writerr = 'GaussTape failed'

      line,n = self.grep(self.appLog,writerr)
      if n == 1:
         result = S_ERROR(mailto + ' POOL error')
      else:
         line,n = self.grep(self.appLog,'bus error')
         if n == 1:
            result = S_ERROR(mailto + ' bus error')
         else:
            line,n = self.grep(self.appLog,'User defined signal 1')
            if n == 1:
               result = S_ERROR(mailto + ' User defined signal 1')
            else:
               line,n = self.grep(self.appLog,'Not found DLL')
               if n == 1:
                  result = S_ERROR(mailto + ' Not found DLL')

      return result

#
#-----------------------------------------------------------------------
#
   def getLogFile(self):
       self.log.debug(' OpenLogFile - try to open %s'%(self.appLog))

       result = S_OK()
       if not os.path.exists(self.appLog):
          if os.path.exists(self.appLog+'.gz'):
             fn = self.appLog+'.gz'
             result = shellCall(0,"gunzip "+fn)
             resultTuple = result['Value']
             if resultTuple[0] > 0:
                self.log.info(resultTuple[1])
                result = S_ERROR('no logfile available')
          else:
            result = S_ERROR('no logfile available')

       return result

#
#----------------------------------------------------------------------
#
   def getAppName(self):
       self.log.debug(' getAppName - from %s'%(self.appLog))

       filename = os.path.split(self.appLog)[1]
       appName = string.split(filename,'_')[0]
       if appName == 'Stripping' :appName = 'DaVinci'
       if string.find(appName,'Merg') != -1: appName ='Merging'

       self.log.debug(' getAppName %s %s'%(self.appLog, appName))
       return appName

#
#----------------------------------------------------------------------
#
   def checkApplicationLog(self,error=''):
       self.log.debug(' appLog - from %s'%(self.appLog))


  #############################################################################
   def __report(self,status):
    """Wraps around setJobApplicationStatus of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    self.log.verbose('setJobApplicationStatus(%s,%s,%s)' %(self.jobID,status,'CheckLogFile'))
    jobStatus = self.jobReport.setJobApplicationStatus(int(self.jobID),status,'CheckLogFile')
    if not jobStatus['OK']:
      self.log.warn(jobStatus['Message'])

    return jobStatus

#
#------------------------------------------------------------------------
#
if __name__=='__main__':

   Chk = LHCbCheckLogFile()

   result = Chk.execute()

   print 'main - result= ',result

