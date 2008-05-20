########################################################################
# $Id: AnalyseLogFile.py,v 1.9 2008/05/20 13:13:54 joel Exp $
########################################################################
""" Script Base Class """

__RCSID__ = "$Id: AnalyseLogFile.py,v 1.9 2008/05/20 13:13:54 joel Exp $"

import commands, os, time

from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog
from DIRAC.DataManagementSystem.Client.ReplicaManager    import ReplicaManager
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from WorkflowLib.Utilities.Tools import *
from DIRAC import                                        S_OK, S_ERROR, gLogger, gConfig


class AnalyseLogFile(object):

  def __init__(self):
      self.log = gLogger.getSubLogger("AnalyseLogFile")
      self.site = gConfig.getValue('/LocalSite/Site','localSite')
      self.result = S_ERROR()
      self.mailadress = 'None'
      self.NUMBER_OF_EVENTS_INPUT = None
      self.NUMBER_OF_EVENTS_OUTPUT = None
      self.NUMBER_OF_EVENTS = None
      self.appName = 'None'
      self.inputData = None
      self.JOB_ID = None
      self.jobID = None
      if os.environ.has_key('JOBID'):
        self.jobID = os.environ['JOBID']
      self.timeoffset = 0
      self.jobReport  = RPCClient('WorkloadManagement/JobStateUpdate')
      self.poolXMLCatName = 'pool_xml_catalog.xml'
      self.appVersion = 'None'

  def execute(self):
      self.log.info( "Analyse Log File for %s" %(self.appLog) )
      self.site = gConfig.getValue('/LocalSite/Site','Site')

      if (int(self.JOB_ID) > 200):
         comm = 'rm -f *monitor*'
         self.log.info("Removing Monitor file(s)")
         res = shellCall(0,comm)

      if self.OUTPUT_MAX:
         self.max_app = self.OUTPUT_MAX
      else:
         self.max_app = 'None'

#      if self.max_app != 'None':
#         if (int(self.JOB_ID) > int(self.max_app)):
#           if self.outputDataSE != None:
#             self.outputDataSE = None

# check the is the logfile exist
      result = self.getLogFile()
      if not result['OK'] :
         self.log.warn(self.argv0 + '.LogFile - no logfile available - EXIT')
         return S_ERROR(self.argv0 + '.LogFile - no logfile available - EXIT')

# check if this is a good job
      result = self.goodJob()
      if result['OK']:
         resultnb = self.nbEvent()
         if resultnb['OK']:
           self.log.info(' AnalyseLogFile - %s is OK ' % (self.appLog))
           self.__report('%s step OK' % (self.appName))
           return resultnb
         else:
           self.sendErrorMail(resultnb['Message'])
           self.log.info('Checking number of events returned result:\n%s' %(resultnb))
      else:
         self.sendErrorMail(result['Message'])
         self.__report('%s step Failed' % (self.appName))

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
      mailto = self.appName.upper()+'_EMAIL'
      line,appinit = self.grep(self.appLog,'ApplicationMgr    SUCCESS')
      if line.split(' ')[2] == 'UTC':
          self.timeoffset = 3

      lEvtMax,n = self.grep(self.appLog,'.EvtMax','-cl')
      if n == 0:
          if self.appName != 'Gauss':
              EvtMax = -1
          else:
              result = S_ERROR(mailto + ' missing job options')
              return result
      else:
         EvtMax = int(string.split(string.replace(lEvtMax,';',' '))[2])

      self.log.info('EvtMax = %s' % (str(EvtMax)))

      line,nev = self.grep(self.appLog,'Reading Event record','-cl')
      if nev == 0:
         line,nev = self.grep(self.appLog,'Nr. in job =','-cl')
         if nev == 0:
            result = S_ERROR(mailto + ' no event')
            return result
         else:
            lastev = string.split(line,'=')[1]
      else:
         lastev = string.split(line)[5]

      result = S_OK()

      line,nomore = self.grep(self.appLog,'No more events')
      lprocessed,n =self.grep(self.appLog,'events processed')
      if n == 0:
          if self.appName == 'Gauss' or nomore == 0:
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
      self.NUMBER_OF_EVENTS_INPUT = str(nprocessed)

      #report job parameter with timestamp
      curTime = time.asctime(time.gmtime())
      report = 'Events processed by %s on %s [UTC]' %(self.appName,curTime)
      self.__setJobParam(report,nprocessed)

# find number of events written
      loutput,n = self.grep(self.appLog,'Events output:')
      if n == 0:
         if self.appName == 'Gauss ' or self.appName == 'Brunel':
            result = S_ERROR('no events written')
      else:
         noutput = int(string.split(loutput)[4+self.timeoffset])
         self.log.info(" %s events written " % str(noutput))
         self.NUMBER_OF_EVENTS_OUTPUT = str(noutput)

      if nprocessed == EvtMax or nomore == 1:
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
          file_end = False
          linenextevt,n = self.grep(self.appLog,'Failed to receieve the next event')
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
      line,n = self.grep(self.appLog,'Application Manager Finalized successfully')
      if n == 0:
        if self.appName:
            mailto = self.appName.upper()+'_EMAIL'
        return S_ERROR(mailto+' not finalized')

# trap POOL error to open a file through POOL
      self.log.info('Check POOL connection error')
      line,poolroot = self.grep(self.appLog,'Error: connectDatabase>','-c')
      if poolroot >= 1:
         for file in line.split('\n'):
            if poolroot > 0:
               if (file.count('PFN=')>0):
                  file_input = file.split('PFN=')[1]
                  if (file_input.count('gfal:guid:')>0):
                     result = S_ERROR('Navigation error from guid via LFC for input file')
                     return result
                  else:
                     if self.PoolXMLCatalog != None:
                        catalogfile = self.PoolXMLCatalog
                     else:
                        catalogfile = 'pool_xml_catalog.xml'

                     catalog = PoolXMLCatalog(catalogfile)
                     cat_guid = catalog.getGuidByPfn(file_input)
                     cat_lfn = catalog.getLfnsByGuid(cat_guid)
                     self.update_status('Bad',cat_lfn['Logical'])

               poolroot = poolroot-1
         return S_ERROR(mailto + ' error to connectDatabase')

# trap CASTOR error
      self.log.info('Check CASTOR error connection')
      line,castor = self.grep(self.appLog,'Could not connect')
      if castor >= 1:
         return S_ERROR(mailto + ' Could not connect to a file')

      self.log.info('Check DCACHE connection error')
      line,tread = self.grep(self.appLog,'SysError in <TDCacheFile::ReadBuffer>: error reading from file')
      if tread >= 1:
         return S_ERROR(mailto + ' TDCacheFile error')

      self.log.info('Check IODataManager error')
      line,resolv = self.grep(self.appLog,'Failed to resolve')
      if resolv >= 1:
         self.log.debug(line)
         return S_ERROR(mailto + ' IODataManager error')

      self.log.info('Check connectionIO error')
      line,cdio = self.grep(self.appLog,'Error: connectDataIO')
      if cdio >= 1:
         return S_ERROR(mailto + ' connectDataIO error')

      self.log.info('Check connectionIO error')
      line,cdio = self.grep(self.appLog,'Error:connectDataIO')
      if cdio >= 1:
         return S_ERROR(mailto + ' connectDataIO error')

      self.log.info('Check loop errors')
      linenextevt,n = self.grep(self.appLog,'Terminating event processing loop due to errors')
      if n != 0:
          return S_ERROR(mailto + 'Event loop no terminated')

      self.log.info('GLIBC error')
      linenextevt,n = self.grep(self.appLog,' glibc ')
      if n != 0:
          return S_ERROR(mailto + 'Problem with glibc ')

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

  def sendErrorMail(self,message):
    genmail = message.split(' ')[0]
    subj = message.split()[1]
    try:
        if self.EMAIL:
            mailadress = self.EMAIL
    except:
        self.log.info('No EMAIL adress supplied')
        return

    self.log.info(' Sending Errors by E-mail to %s' %(mailadress))
    subject = self.appName+' '+ self.appVersion + \
              ": "+subj+' '+self.PRODUCTION_ID+'_'+self.JOB_ID+' JobID='+str(self.jobID)
    lfile = open('logmail','w')
    lfile.write('The Application %s %s had a problem \n' %(self.appName,self.appVersion))
    if self.inputData:
      lfile.write('\n\nInput Data:\n')
      for inputname in self.inputData.split(';'):
        lfile.write(inputname+'\n')

    self.mode = gConfig.getValue('/LocalSite/Setup','Setup')
    self.LFN_ROOT= getLFNRoot(self.SourceData)
    logpath = makeProductionPath(self.JOB_ID,self.LFN_ROOT,'LOG',self.mode,self.PRODUCTION_ID,log=True)

    if self.appLog:

      logse = gConfig.getOptions('/Resources/StorageElements/LogSE')
      logurl = 'http://lhcb-logs.cern.ch/storage'+logpath
      lfile.write('\n\nLog Files directory for the job:\n')
      url = logurl+'/'+ self.JOB_ID+'/'
      lfile.write(url+'\n')
      lfile.write('\n\nLog File for the problematic step:\n')
      url = logurl+'/'+ self.JOB_ID+'/'+ self.appLog + '.gz'
      lfile.write(url+'\n')
      lfile.write('\n\nJob StdOut:\n')
      url = logurl+'/'+ self.JOB_ID+'/std.out'
      lfile.write(url+'\n')
      lfile.write('\n\nJob StdErr:\n')
      url = logurl+'/'+ self.JOB_ID+'/std.err'
      lfile.write(url+'\n')

    lfile.close()

    if os.path.exists('corecomm.log'):
      comm = 'cat corecomm.log logmail | mail -s "'+subject+'" '+ mailadress
    else:
      comm = 'cat logmail | mail -s "'+subject+'" '+ mailadress

    self.result = shellCall(0,comm)
    #status = 0
    if self.result['Value'][0]:
      self.log.error( "Sending mail failed %s" % str( self.result['Value'][1] ) )


  def checkApplicationLog(self,error):
    self.log.debug(' appLog - from %s'%(self.appLog))
    self.log.info(error)

  #############################################################################
  def __report(self,status):
    """Wraps around setJobApplicationStatus of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    self.log.verbose('setJobApplicationStatus(%s,%s,%s)' %(self.jobID,status,'AnalyseLogFile'))
    jobStatus = self.jobReport.setJobApplicationStatus(int(self.jobID),status,'AnalyseLogFile')
    if not jobStatus['OK']:
      self.log.warn(jobStatus['Message'])

    return jobStatus

  #############################################################################
  def __setJobParam(self,name,value):
    """Wraps around setJobParameter of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    self.log.verbose('setJobParameter(%s,%s,%s)' %(self.jobID,name,value))
    jobParam = self.jobReport.setJobParameter(int(self.jobID),str(name),str(value))
    if not jobParam['OK']:
      self.log.warn(jobParam['Message'])

    return jobParam
