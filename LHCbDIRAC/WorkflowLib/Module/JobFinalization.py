########################################################################
# $Id: JobFinalization.py,v 1.50 2008/05/06 15:26:15 atsareg Exp $
########################################################################


__RCSID__ = "$Id: JobFinalization.py,v 1.50 2008/05/06 15:26:15 atsareg Exp $"

from DIRAC.DataManagementSystem.Client.Catalog.BookkeepingDBClient import *
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.StorageElement import StorageElement
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog import PoolXMLCatalog
from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from DIRAC                                            import S_OK, S_ERROR, gLogger, gConfig
from WorkflowLib.Utilities.Tools import *

import os, time, re, random, shutil

class JobFinalization(object):

  def __init__(self):
    self.STEP_ID = None
    gLogger.setLevel('debug')
    self.CONFIG_NAME = None
    self.RUN_NUMBER = None
    self.FIRST_EVENT_NUMBER = None
    self.NUMBER_OF_EVENTS = None
    self.NUMBER_OF_EVENTS_OUTPUT = None
    self.PRODUCTION_ID = None
    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']
    self.JOB_ID = None
    self.LFN_ROOT = None
    self.listoutput = []
    self.listoutput_1 = []
    self.listoutput_2 = []
    self.listoutput_3 = []
    self.listoutput_4 = []
    self.listoutput_5 = []
    self.listoutput_6 = []
    self.tmpdir = '.'
    self.logdir = '.'
    self.mode = None
    self.poolXMLCatName = None
    self.TmpCacheSE = 'CERN-Failover'
    self.log = gLogger.getSubLogger("JobFinalization")
    self.nb_events_input = None
    self.rm = ReplicaManager()
    self.bk = BookkeepingClient()
    self.bkDB = BookkeepingDBClient()
    self.jobReport  = RPCClient('WorkloadManagement/JobStateUpdate')
    self.transferID = ''
    self.root = gConfig.getValue('/LocalSite/Root',os.getcwd())
    self.logSE = 'LogSE'
    self.bookkeepingTimeOut = 10 #seconds
    self.SourceData=None
    self.log.setLevel('debug')

  def execute(self):
    self.__report('Starting Job Finalization')
    # first lets collect all outputs in single
    #self.listoutput = self.listoutput_1+self.listoutput_2+self.listoutput_3+self.listoutput_4+self.listoutput_5+self.listoutput_6
    #print "KGG ->>>>>>>>>>>>>", self.listoutput
    #if len(self.listoutput) == 0:
      #self.listoutput = self.listoutput1+self.listoutput2
    res = shellCall(0,'ls -al')
    if res['OK'] == True:
      self.log.info("final listing : %s" % (str(res['Value'][1])))
    else:
      self.log.info("final listing with error: %s" % (str(res['Value'][2])))
    self.log.info('Site root is found to be %s' %(self.root))
    self.log.info('Updating local configuration with available CFG files')
    self.__loadLocalCFGFiles(self.root)
    #self.mode = gConfig.getValue('LocalSite/Setup','test')
    if self.workflow_commons.has_key('DataType'):
      self.mode = self.workflow_commons['DataType'].lower()
    else:
      self.mode = 'test'
    self.log.info('PRODUTION_ID = %s, JOB_ID = %s ' %(self.PRODUCTION_ID,self.JOB_ID))
    self.logdir = self.root+'/job/log/'+self.PRODUCTION_ID+'/'+self.JOB_ID
    self.log.info('Log directory is %s' %self.logdir)
    self.logtar = self.PRODUCTION_ID+'_'+self.JOB_ID+'.tar'
    self.log.info('Log tar is %s' %self.logtar)
    error = 0
    dataTypes = ['SIM','DIGI','DST','RAW','ETC','SETC','FETC','RDST','MDF']

    #self.LFN_ROOT = getLFNRoot(self.SourceData)
    #SourceData seems to be a copy of InputData
    self.LFN_ROOT = getLFNRoot(self.SourceData)

    result = self.finalize(error)

    return result


  def finalize(self,error=0):
    """ finalize method performs final operations after all the job
        steps were executed. Only production jobs are treated.
    """

    result = S_OK()

    self.log.info( "Start finalization %s %s" % ( str( error ), str( self.PRODUCTION_ID ) ) )
    if self.PRODUCTION_ID == None:
        return result

    ########################################################
    # Store log files if even the job failed

    try:
#      self.log.info("Saving logfiles is currently disabled")
      self.uploadLogFiles()
    except Exception,x:
      self.log.error("Exception while log files uploading:")
      self.log.error(str(x))

    all_done = True
    if not error:

      ####################################
      # Get the Pool XML catalog if any
      self.getPoolXMLCatalogs()

      #########################################################################
      # Store to the grid and register the output files
      # The output files will be stored only if this is requested explicitely
      # in the job workflow or this is requested in the local configuration for
      # the given file type
      #
      # Make up to max_attempts to upload each file

      count = 0
      outputs = []
      self.log.info(self.listoutput)
      while (count < len(self.listoutput)):
        if self.listoutput[count].has_key('outputDataName'):
          outputs.append(((self.listoutput[count]['outputDataName']),(self.listoutput[count]['outputDataSE']),(self.listoutput[count]['outputType'])))
        count=count+1
      outputs_done = []
      all_done = False
      count = 0
      max_attempts = 1
      while not all_done and count < max_attempts :
        if count > 0:
          self.log.info("Output data upload retry number "+str(count))
        all_done = True
        for output,outputse,outputtype in outputs:
          if not output in outputs_done:
            resUpload = self.uploadOutputData(output,outputtype.upper(),'1',outputse)
            if resUpload['OK'] == True:
              outputs_done.append(output)
            else:
              all_done = False
        count += 1

      #########################################################################
      # At least some files failed uploading to the destination,
      # still trying to save the work

      if not all_done:
        ok = True
        if self.TmpCacheSE != None:
          cache_se = self.TmpCacheSE
#          for output,otype,oversion,outputse in outputs:
          for outputFile,outputSE,outputType in outputs:
            if not outputFile in outputs_done:
              fname = os.path.basename(outputFile)
              lfn = self.LFN_ROOT+'/'+fname
              resCopy = self.rm.put(lfn,os.getcwd()+'/'+fname,cache_se)
              if resCopy['OK'] == True:
#                pfname = result['PFN']
                self.log.info(resCopy)
                ok = False
                self.log.info("NOT IMPLEMENTED : Setting delayed transfer request for %s %s %s %s " % (fname, lfn, fname, cache_se))
#                self.setTransferRequest(lfn,pfname,size,cache_se,guid)
              else:
                ok = False
          if ok:
            all_done = True
        else:
          self.log.error('Failed to upload all the output data')

      #################################################################
      # Send bookkeeping only if the data was uploaded successfully

      if all_done:
        resBK = self.reportBookkeeping()
        if resBK['OK'] != True:
          self.log.error(resBK['Message'])
          error = 1
          result = S_ERROR()

    #########################################################################
    #  If the Transfer request is not empty, send it for later retry

    if not all_done:
      self.__report('Failed To Save Job Outputs')
      result = S_ERROR('Failed To Save Job Outputs')
    elif not error:
      self.__report('Job Finished Successfully')

    return result


################################################################################
  def reportBookkeeping(self):
    """ Collect and send safely the bookkeeping reports
    """
    self.__report('Sending Bookkeeping Report')
    result = S_OK()
    books = []
    files = os.listdir('.')
    for f in files:
      if re.search('^bookkeeping',f):
        books.append(f)

    #####################################################
    #
    #  TODO: merge bookkeeping requests in just one
    #
    #####################################################


    bad_counter = 0
    for f in books:
      CONTINUE = 0
      self.log.info( "Sending bookkeeping information %s" % str( f ) )
      fm = f.replace('bookkeeping_','')
      reqfile = open(f,'r')
      xmlstring = reqfile.read()
      while (CONTINUE == 0):
          result = self.bk.sendBookkeeping(fm,xmlstring)
          if result['OK'] != True:
            bad_counter += 1
            if result['Message'].find('Connection timed out') != -1:
              time.sleep(self.bookkeepingTimeOut)
            if bad_counter > 3:
              self.log.error( "Failed to send bookkeeping information for %s after %s attempts" % (str( f ) , str(bad_counter)) )
              self.log.error(result)
              self.__report('Failed To Send Bookkeeping Information')
              CONTINUE = 1
          else:
            self.log.info( "Bookkeeping information sent successfully" )
            CONTINUE = 1

    return result

################################################################################
  def getPoolXMLCatalogs(self):
    """ Collect the available Pool XML catalogs in one object
    """

    fcname = []
    if self.poolXMLCatName != None:
      fcn = self.poolXMLCatName
      if os.path.isfile(fcn+'.gz'):
        gunzip(fcn+'.gz')
      if os.path.exists(fcn):
        fcname.append(fcn)
    else:
      if  self.poolXMLCatName != None:
        fcn = self.poolXMLCatName
        if os.path.isfile(fcn+'.gz'):
          gunzip(fcn+'.gz')
        if os.path.exists(fcn):
          fcname.append(fcn)
      else:
        fcn = self.PRODUCTION_ID+'_'+self.JOB_ID+'_catalog.xml'
        if os.path.isfile(fcn+'.gz'):
          gunzip(fcn+'.gz')
        if os.path.exists(fcn):
          fcname.append(fcn)

    flist = os.listdir('.')
    for fcn in flist:
      if re.search('atalog.xml',fcn) and not re.search('BAK',fcn) and not re.search('.temp$',fcn):
        if re.search('.gz',fcn):
          gunzip(fcn)
          fcn = fcn.replace('.gz','')
        fcname.append(fcn)

    self.poolcat = None
    fcnames = uniq(fcname)
    if fcnames:
      self.log.info("The following Pool catalog slices will be used:")
      self.log.info(str(fcnames))
      self.poolcat = PoolXMLCatalog(fcnames)


################################################################################
  def saveLogTar(self,logfile,line,):

    if os.path.exists(logfile):
      self.log.warn( "Saving log file %s : file already exist" % logfile )
      os.system('rm -f '+logfile)
#      return

    status = tar(logfile,line)
    if status > 0 :
      return

################################################################################
  def saveLogFile(self,logfile,gzip_flag=1):

    if not os.path.exists(logfile):
      self.log.error( "Saving log file %s failed: no such file" % logfile )
      return

    if gzip_flag:
      status = gzip(logfile)
      if status > 0 :
        return
      else:
        logfile = logfile+'.gz'

    ##################################################
    #  Copy the log file
    try:
      # print "Copy",logfile+".gz",'to',self.logdir
      shutil.copy(logfile,self.logdir)
      cwd = os.getcwd()
      os.chdir(self.logdir)
      makeIndex()
      os.chdir(cwd)
    except IOError, x:
      self.log.error( "Log file copy failed, trying to copy to the TMP directory" )
      shutil.copy(logfile,self.tmpdir+'/'+self.job_id)

    # Do not leave gzipped files in the working directory.
    # They may be still used later
    gunzip(logfile)

##################################################################################
  def uploadLogFiles(self):
    """ upload log files if any using failover log SEs in case
        of failures
    """

    files = os.listdir('.')
    files.append('../std.out')
    files.append('../std.err')

    # Ugly !!!  - distinguish log files by their extensions
    logexts = ['.txt','.hbook','.log','.root','.out','.output','.xml','.sh', '.info', '.err']

    ##################################################
    #  Create the job log directory

    if not os.path.exists(self.logdir):
      try:
        os.makedirs(self.logdir)
        #os.symlink(self.logdir,self.logdirlink)
      except Exception, x:
        result = re.search("File exists",str(x))
        if result is not None:
          pass
        else:
          self.log.error( "Cannot create the job log directory: %s" % str( x ) )
          self.log.error( "Using the TMP directory for logging" )
          self.logdir = os.path.abspath(self.tmpdir+'/'+self.JOB_ID)
          try:
            os.makedirs(self.logdir)
          except Exception, x:
            result = re.search("File exists",str(x))
            if result is not None:
              pass
            else:
              self.logdir = os.path.abspath('./'+self.JOB_ID)
              os.makedirs(self.logdir)


    cwd = os.getcwd()
    os.chdir(self.logdir)
    jobfile = open('job.info','w')
    jobfile.write(self.PRODUCTION_ID+'_'+self.JOB_ID+'\n')
    log_target_path = makeProductionLfn(self.JOB_ID,self.LFN_ROOT,(self.logtar,'LOG','1'),self.mode,self.PRODUCTION_ID)
    jobfile.write(log_target_path+'\n')
    jobfile.close()
    os.chdir(cwd)

    self.log.info( "\nTarring log files to the log file: \n<-- %s -->" % self.logtar )
    linetotar = ''
    for f in files:
      ext = os.path.splitext(f)[1]
      if ext in logexts:
        self.log.info( 'Saving log file: %s' % str( f ) )
        self.saveLogFile(f)
##TAR        linetotar = linetotar+' '+f

##tar    self.saveLogTar(self.logdir+'/'+self.logtar,linetotar)
##tar    gzip(self.logdir+'/'+self.logtar)


    self.log.info("Transferring log files to LogSE")
    target_path = makeProductionPath(self.JOB_ID,self.LFN_ROOT,'LOG',self.mode,self.PRODUCTION_ID,log=True)
    self.log.info("PutDirectory %s %s %s " % (target_path, os.path.realpath(self.logdir),self.logSE))
    result = self.rm.putDirectory(target_path,os.path.realpath(self.logdir),self.logSE)
##tar      target_path = makeProductionLfn(self.JOB_ID,self.LFN_ROOT,(self.logtar+'.gz','LOG','1'),self.mode,self.PRODUCTION_ID)
##tar      self.log.info("Put %s %s %s " % (target_path, os.path.realpath(self.logdir+'/'+self.logtar+'.gz'), 'LogSE'))
##tar      result = self.rm.put(target_path,os.path.realpath(self.logdir+'/'+self.logtar+'.gz'),'LogSE')
#      result = self.rm.put(target_path,os.path.realpath(self.logdir+'/'+self.logtar+'.gz'),'CERN_DIP_Log')
    self.log.info(result)

    if result['OK']:
      # Construct the http reference to the Log directory
      logref = '<a href="http://lhcb-logs.cern.ch/storage%s/%s/">Log file directory</a>' % (target_path, str(self.JOB_ID))
      self.log.info(logref)
      self.__setJobParam('Log URL',logref)

    if not result['OK']:
      self.log.error("Transferring log files to the main LogSE failed")
      self.log.error( result )
    else:
      self.log.info("Transferring log files to the main LogSE successful")

################################################################################
  def uploadOutputData(self,output,otype,oversion,outputse):
    """ Determine the output file destination SEs and upload file to the grid
    """

    ses = []
    # Add output SE defined in the job description
    self.log.info(outputse)
    if outputse != None:
      outses = outputse.split(',')
      for outse in outses:
        resultSE = gConfig.getValue('/Operations/StorageElement/'+outse,None)
        for se in resultSE.replace(' ','').split(','):
          ses.append(se)

    # Attempt to store first file to the LocalSE if it is in the list of
    # requested SEs

    if len(ses) == 0:
      # Processing for this output type/file is not requested
      return S_OK()

    self.log.info( "Processing output %s %s %s" % ( str( output ), str( otype ), str( oversion ) ) )

    # Check the validity of SEs
    ses_trial = uniq(ses)
    self.log.info(ses_trial)
    ses = []
    ses_local = []
    seValue = None
    try:
      seValue = gConfig.getValue('/LocalSite/LocalSE','')
    except Exception,x:
      self.log.warn('Could not get local SE list with exception')
      self.log.warn(str(x))
    if not seValue:
      self.log.info('Trying again to find the LocalSE list')
      self.__loadLocalCFGFiles(self.root+'/../')
      self.__loadLocalCFGFiles(os.getcwd())
      self.__loadLocalCFGFiles(os.getcwd()+'/../')
      seValue = gConfig.getValue('/LocalSite/LocalSE','')
      self.log.info('Finally resolved LocalSE %s' %(seValue))

    self.log.info('/LocalSite/LocalSE is: %s' %seValue)
    if not seValue:
      self.log.warn('LocalSE list is null from CS')
      return S_ERROR('Site/LocalSE list is null')

    resultSEList = None
    if type(seValue) == type(" "):
      resultSEList = seValue.replace(' ','').split(',')
    elif type(seValue) == type([]):
      resultSEList = seValue
    else:
      self.log.info('CS returned problematic value for /LocalSite/LocalSE')
      return S_ERROR('LocalSite/LocalSE error from CS')

    self.log.info('Site LocalSE list is: %s' %resultSEList)
    for se in resultSEList:
      ses_local.append(se)
    looping = 0
    for se in ses_trial:
      for sel in ses_local:
        if sel == se:
          ses.append(sel)
          looping = 1
          break
      if looping == 1:
        break

    ses = uniq(ses)

    if len(ses) > 0:
      self.log.info("File %s will be stored to the following SEs:\n%s" % (output, str(ses)))

      lfn = makeProductionLfn(self.JOB_ID,self.LFN_ROOT,(output,otype),self.mode,self.PRODUCTION_ID)
      result = self.uploadDataFile(output,lfn,ses)
      return result
    else:
      self.log.error('No valid SEs defined as file destinations')
      return S_ERROR('No valid SEs defined as file destinations')


  #############################################################################
  def __loadLocalCFGFiles(self,localRoot):
    """Loads any extra CFG files residing in the local DIRAC site root.
    """
    files = os.listdir(localRoot)
    self.log.verbose('Checking directory %s' %localRoot)
    for i in files:
      if re.search('.cfg$',i):
        gConfig.loadFile('%s/%s' %(localRoot,i))
        self.log.verbose('Found local .cfg file %s' %i)

###################################################################################################
  def uploadDataFile(self,datafile,lfn,destinationSEList):
    """ Upload a datafile to the grid and set necessary replication requests
    """

    # Get the GUID first
    guid = None
    if self.poolcat:
      guid = self.poolcat.getGuidByPfn(datafile)
      if not guid: guid = None

    if guid is None:
      self.log.warn("GUID is not defined for file %s" % datafile)
      result = shellCall(0,'uuidgen')
      status = result['Value'][0]
      guid = result['Value'][1]
      self.log.info("Generated GUID: %s" % str(guid))
    else:
      self.log.info("File: %s GUID: %s" % (str(datafile), str(guid)))

    size = getfilesize(datafile)
    destination_ses = uniq(destinationSEList)

    one_grid_upload_successful = False
    one_grid_copy_successful = False
    for se in destination_ses:
      result = self.uploadDataFileToSE(datafile,lfn,se,guid)
      if not result['OK']:
        self.log.warn(result)
        self.__setJobParam('Upload failed for file: %s at SE: %s' %(datafile,se),'Size: %s, LFN: %s, GUID: %s' %(size,lfn,guid))
        self.__report('Data Upload Failed')
        return S_ERROR('Data Upload Failed')

    return S_OK()


############################################################################################
  def uploadDataFileToSE(self,datafile,lfn,se,guid):
    """ Upload a datafile to a destination SE, set replica registration request if necessary
    """

    #print "uploadDataFileToSE",datafile,lfn,se,guid

    fname = os.path.basename(datafile)
    lfn_directory = os.path.dirname(lfn)
    size = getfilesize(datafile)

    ################################################
    # Prepare transfer accounting information

    request = {}
    request['TransferID'] = self.transferID
    request['TargetSE'] = se
    self.log.info("Copying %s to %s" % (fname,se))
    LFC_OK = True
    BKK_OK = True
    self.log.info('putAndRegister(%s,%s,%s,%s)' %(lfn,os.getcwd()+'/'+fname,se,guid))
# should put a try/except
    resultPaR = self.rm.putAndRegister(lfn,os.getcwd()+'/'+fname,se,guid)
    if resultPaR['OK']:
      if len(resultPaR['Value']['Failed']) > 0:
        for lfn,mess in resultPaR['Value']['Failed'].items():
          if mess.has_key('register'):
        # Transfer done but registration failed
            LFC_OK = False
            BKK_OK = False
            self.log.info( "Registration failed for %s" % lfn )
#            result['level'] = 'Registration'
      if BKK_OK == True:
        self.bkDB.addReplica((lfn,os.getcwd()+'/'+fname,se))

    self.log.info(resultPaR)
    result = resultPaR

    return result

#############################################################################################
  def isGridSE(self,se):
    """ Test if the se is of a grid type
    """

    section = gConfig.getSections('Resources/StorageElements/'+se)
    if section['OK'] != True:
      return False
    selement = StorageElement(se)
    if not selement.isValid():
      return False
    protocols = selement.getProtocols()
    return 'srm' in protocols or 'gridftp' in protocols or 'gsiftp' in protocols

#############################################################################################
  def getSEName(self,se):
    """ Get non-aliased SE name
    """

    section = gConfig.getSections('Resources/StorageElements/'+se)
    if section['OK'] != True:
      return ''
    selement = StorageElement(se)
    if not selement.isValid():
      return ''
    else:
      return selement.name

  #############################################################################
  def __report(self,status):
    """Wraps around setJobApplicationStatus of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    self.log.verbose('setJobApplicationStatus(%s,%s,%s)' %(self.jobID,status,'JobFinalization'))
    jobStatus = self.jobReport.setJobApplicationStatus(int(self.jobID),status,'JobFinalization')
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