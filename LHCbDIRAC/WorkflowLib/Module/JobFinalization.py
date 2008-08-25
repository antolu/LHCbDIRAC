########################################################################
# $Id: JobFinalization.py,v 1.104 2008/08/25 10:05:59 atsareg Exp $
########################################################################

""" JobFinalization module is used in the LHCb production workflows to
    accomplish all the Job operations. It performs the following operations

    - Stores the output log files in a secure (failover) way;
    - Stores the output data taking into account all the possible
      storage destinations in an efficient and failover safe way;
    - Sends the Bookkeeping information in a failover safe way;
    - Attempts to accomplish accumulated in the entire workflow
      failover requests and prepares a combined request if necessary;

    Variables that JobFinalization module relies on as input:

    self.PRODUCTION_ID
    self.JOB_ID
    self.listoutput
    self.poolXMLCatName
    self.SourceData

"""

__RCSID__ = "$Id: JobFinalization.py,v 1.104 2008/08/25 10:05:59 atsareg Exp $"

from DIRAC.DataManagementSystem.Client.Catalog.BookkeepingDBClient import *
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.StorageElement import StorageElement
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog import PoolXMLCatalog
from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from DIRAC.BookkeepingSystem.Client.BookkeepingClientOld import BookkeepingClientOld
from DIRAC.Core.DISET.RPCClient                       import RPCClient
from DIRAC                                            import S_OK, S_ERROR, gLogger, gConfig
from WorkflowLib.Utilities.Tools import *
from WorkflowLib.Module.ModuleBase import ModuleBase
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.RequestManagementSystem.Client.DISETSubRequest import DISETSubRequest
from DIRAC.Core.Utilities.SiteSEMapping                   import getSEsForSite
from DIRAC.Core.Utilities.List import pop
from DIRAC.Core.Utilities.File import makeGuid, fileAdler
import os, time, re, random, shutil, commands

class JobFinalization(ModuleBase):

  def __init__(self):

    ############### TODO
    # Remove unused variables
    # Make checks of the necessary variables

    #####################################################
    # Variables supposed to be set by the workflow

    self.version = __RCSID__
    self.PRODUCTION_ID = None
    self.JOB_ID = None

    # A list of dictionaries specifying the output data
    self.listoutput = []
    self.outputDataFileMask = ''
    self.poolXMLCatName = ''
    self.sourceData = ''
    self.workflow_commons = None
    self.uploadAllFlag = False


    #####################################################
    # Local variables

    self.jobID = ""
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']

    self.LFN_ROOT = ''
    self.logdir = '.'
    self.mode = ''
    self.InputData = ''
    self.logSE = 'LogSE'
    self.bookkeepingTimeOut = 10 #seconds
    self.root = gConfig.getValue('/LocalSite/Root',os.getcwd())
    self.site = gConfig.getValue('/LocalSite/Site','localSite')

    self.rm = ReplicaManager()
    self.bk = BookkeepingClient()
    self.bkold = BookkeepingClientOld()
    self.bkDB = BookkeepingDBClient()
    self.jobReport  = None
    self.fileReport = None
    self.request = None

    self.log = gLogger.getSubLogger("JobFinalization")
    self.log.setLevel('debug')

######################################################################
  def resolveInputVariables(self):
    if self.workflow_commons.has_key('sourceData'):
        self.sourceData = self.workflow_commons['sourceData']

    if self.workflow_commons.has_key('outputDataFileMask'):
        self.outputDataFileMask = self.workflow_commons['outputDataFileMask']
        if not type(self.outputDataFileMask)==type([]):
          self.outputDataFileMask = self.outputDataFileMask.split(';')

    if self.step_commons.has_key('applicationName'):
       self.applicationName = self.step_commons['applicationName']
       self.applicationVersion = self.step_commons['applicationVersion']
       self.applicationLog = self.step_commons['applicationLog']

    if self.workflow_commons.has_key('configName'):
      self.configVersion = self.workflow_commons['configVersion']
    else:
      self.configVersion = self.applicationVersion

    if self.workflow_commons.has_key('Request'):
      self.request = self.workflow_commons['Request']
    else:
      self.request = RequestContainer()

    if self.workflow_commons.has_key('poolXMLCatName'):
       self.poolXMLCatName = self.workflow_commons['poolXMLCatName']

    if self.workflow_commons.has_key('InputData'):
       self.InputData = self.workflow_commons['InputData']

    if self.workflow_commons.has_key('JobReport'):
      self.jobReport = self.workflow_commons['JobReport']

    if self.workflow_commons.has_key('FileReport'):
      self.fileReport = self.workflow_commons['FileReport']

    if self.workflow_commons.has_key('dataType'):
      self.mode = self.workflow_commons['dataType'].lower()
    else:
      self.mode = 'test'

    if self.workflow_commons.has_key('outputList'):
       self.listoutput = self.workflow_commons['outputList']

    if self.workflow_commons.has_key('uploadAllFlag'):
      if self.workflow_commons['uploadAllFlag'].lower() == "yes":
        self.uploadAllFlag = True

######################################################################
  def execute(self):
    """ Main executon method
    """

    # Add global reporting tool
    self.resolveInputVariables()

    self.request.setRequestName('job_%s_request.xml' % self.jobID)
    self.request.setJobID(self.jobID)
    self.request.setSourceComponent("Job_%s" % self.jobID)

    result = self.setApplicationStatus('Job Finalization')
    self.log.info('Initializing '+self.version)

    res = shellCall(0,'ls -al')
    if res['OK'] == True:
      self.log.info("final listing : %s" % (str(res['Value'][1])))
    else:
      self.log.info("final listing with error: %s" % (str(res['Value'][2])))

    self.log.info('Site root is found to be %s' %(self.root))
    self.log.info('Updating local configuration with available CFG files')
    self.__loadLocalCFGFiles(self.root)
    self.log.info('PRODUTION_ID = %s, JOB_ID = %s ' %(self.PRODUCTION_ID,self.JOB_ID))
    self.logdir = os.path.realpath('./job/log/'+self.PRODUCTION_ID+'/'+self.JOB_ID)
    self.log.info('Log directory is %s' %self.logdir)
    self.logtar = self.PRODUCTION_ID+'_'+self.JOB_ID+'.tar'
    self.log.info('Log tar file name is %s' %self.logtar)

    error = 0
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      error = 1
      self.fileReport.setCommonStatus('unused')
      self.fileReport.commit()
      self.log.info('Job finished with errors. Reduced finalization will be done')

    result = self.fileReport.commit()
    if not result['OK']:
        self.log.error('Can not update the Status of files in the processingDB : %s' %(result))
        error=1
    else:
        self.log.info('Status of files have been properly updated in the ProcessingDB')

    if self.InputData:
      self.LFN_ROOT= getLFNRoot(self.InputData)
    else:
      self.LFN_ROOT=getLFNRoot(self.InputData,self.configVersion)

    result = self.finalize(error)

    return result

##############################################################################
  def finalize(self,error=0):
    """ finalize method performs final operations after all the job
        steps were executed. Only production jobs are treated.
    """

    result = S_OK()
    logs_done = True
    data_done = True
    bk_done = True
    bkold_done = True
    request_done = True
    error_message = ''

    self.log.info( "Start finalization %s %s" % ( str( error ), str( self.PRODUCTION_ID ) ) )
    if not self.PRODUCTION_ID:
      return S_OK()

    ########################################################
    # Store log files if even the job failed


    if not self.workflowStatus['OK']:
       self.log.info('Stop this module before uploading data, failure detected in a previous step :')
       self.log.info('Workflow status : %s' %(self.workflowStatus))
       self.setApplicationStatus('Job Completed With Errors')
       error = 1

    if not error:
      ########################################################
      # Upload the output data

      # Remove explicitly BookkeepingDB catalog for the output data operations
      result = self.rm.fileCatalogue.removeCatalog('BookkeepingDB')
      if not result['OK']:
        self.log.warn('Failed to remove BookkeepingDB to the list of fail catalogs')
        bk_catalog_exists = False
      else:
        bk_catalog_exists = True
        if result['Value'] == "Catalog does not exist":
          bk_catalog_exists = False

      resultUpload = self.uploadOutput()

      # Add the BK catalog back
      if bk_catalog_exists:
        self.rm.fileCatalogue.addCatalog('BookkeepingDB')


      #################################################################
      # Send bookkeeping only if the data was uploaded successfully

      if resultUpload['OK']:
        lfnList = resultUpload['Value']['LFNs']
        failoverLfnList = resultUpload['Value']['FailoverLFNs']
#        resultBK = self.reportBookkeeping()
#        if not resultBK['OK']:
#          self.log.error(resultBK['Message'])
#          bk_done = False
        resultBKOld = self.reportBookkeepingOld()
        if not resultBK['OK']:
          self.log.error(resultBKOld['Message'])
          bkold_done = False
        resultBKReplica = self.setBKReplica(lfnList,failoverLfnList,bkold_done)
      else:
        data_done = False

    try:
      resultLog = self.uploadLogFiles()
      if not resultLog['OK']:
        logs_done = False
    except Exception,x:
      self.log.error("Exception while log files uploading:")
      self.log.error(str(x))

    if not data_done:
      self.setApplicationStatus('Failed to save output data')
      result = S_ERROR('Failed to save output data')

    if logs_done and data_done and bk_done and bkold_done and not error:
      self.setApplicationStatus('Job Finished Successfully')

    if not logs_done:
      error_message = "failed to save logs, "
    if not bk_done and not bkold_done and not request_done:
      error_message += "failed to send bk data, "

    if error_message:
      error_message = error_message[:-2]
      self.setJobParameter('FinalizationError',error_message)

    ##################################################################
    # Create failover request if necessary
    resultRequest = self.createRequest()
    if not resultRequest['OK']:
      self.log.error('Failed to create overall job request for job %s' % self.jobID)
      result = S_ERROR('Failed to create overall job request')


    return result

################################################################################
  def reportBookkeeping(self):
    """ Collect and send safely the bookkeeping reports
    """

    result = S_OK()
    books = []
    files = os.listdir('.')
    for f in files:
      if re.search('^newbookkeeping',f):
        books.append(f)

    for f in books:
      counter = 0
      send_flag = True
      self.log.info( "Sending bookkeeping file %s" % str( f ) )
      fm = f.replace('newbookkeeping_','')
      reqfile = open(f,'r')
      xmlstring = reqfile.read()
      while (send_flag):
        result = self.bk.sendBookkeeping(fm,xmlstring)
        if not result['OK']:
          counter += 1
          if result['Message'].find('Connection timed out') != -1:
            time.sleep(self.bookkeepingTimeOut)
          if counter > 3:
            self.log.error( "Failed to send bookkeeping information for %s: %s" % (str(f),result['Message']) )
            self.request.setDISETRequest(result['rpcStub'],executionOrder=2)
            send_flag = False
            result = S_ERROR('Failed to send bookkeeping information for %s' % str(f))
        else:
          self.log.info( "Bookkeeping %s sent successfully" % f )
          send_flag = False

    return result

################################################################################
  def reportBookkeepingOld(self):
    """ Collect and send safely the bookkeeping reports
    """

    result = S_OK()
    books = []
    files = os.listdir('.')
    for f in files:
      if re.search('^bookkeeping',f):
        books.append(f)

    for f in books:
      counter = 0
      send_flag = True
      self.log.info( "Sending bookkeeping file %s" % str( f ) )
      fm = f.replace('bookkeeping_','')
      reqfile = open(f,'r')
      xmlstring = reqfile.read()
      while (send_flag):
        result = self.bkold.sendBookkeeping(fm,xmlstring)
        if not result['OK']:
          counter += 1
          if result['Message'].find('Connection timed out') != -1:
            time.sleep(self.bookkeepingTimeOut)
          if counter > 3:
            self.log.error( "Failed to send bookkeeping information for %s: %s" % (str(f),result['Message']) )
            self.request.setDISETRequest(result['rpcStub'],executionOrder=2)
            send_flag = False
            result = S_ERROR('Failed to send bookkeeping information for %s' % str(f))
        else:
          self.log.info( "Bookkeeping %s sent successfully" % f )
          send_flag = False

    return result

################################################################################
  def setBKReplica(self,lfns,failover_lfns,bk_flag):
    """ Set the BK replica flag for the given lfns. Set deferred request in case of
        files stored in the failover storage or if the Bookkeeping update failed
    """
    fileDict={}
    fileDict['PFN'] = 'pfn'
    fileDict['Size'] = 999
    fileDict['Addler'] = ''
    fileDict['GUID'] = ''

    for lfn in lfns:
      fileDict['LFN'] = lfn
      if not lfn in failover_lfns and bk_flag:
        resultBKReplica = self.bkDB.addFile((lfn,'',999,'SE','',''))
        if not resultBKReplica['OK']:
          self.request.setDISETRequest(resultBKReplica['rpcStub'],executionOrder=3)
      else:
        resultBKRequest = self.setRegistrationRequest('SE','BookkeepingDB',fileDict,executionOrder=3)

    return S_OK()

################################################################################
  def __getPoolXMLCatalogs(self):
    """ Collect the available Pool XML catalogs in one object
    """

    fcname = []
    if not self.poolXMLCatName:
      fcn = self.poolXMLCatName
      if os.path.isfile(fcn+'.gz'):
        gunzip(fcn+'.gz')
      if os.path.exists(fcn):
        fcname.append(fcn)
    else:
      if not self.poolXMLCatName:
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
  def saveLogFile(self,logfile,gzip_flag=0):

    if not os.path.exists(logfile):
      self.log.error( "Saving log file %s failed: no such file" % logfile )
      return S_ERROR('No such file')


    ##################################################
    #  Copy the log file
    try:
      # print "Copy",logfile+".gz",'to',self.logdir
      shutil.copy(logfile,self.logdir)
      cwd = os.getcwd()
      os.chdir(self.logdir)
      if gzip_flag:
        status = gzip(logfile)
        if status > 0 :
          return
        else:
          logfile = logfile+'.gz'
      makeIndex()
      os.chdir(cwd)
    except IOError, x:
      return S_ERROR('File copy failed: '+str(x))

    return S_OK()

##################################################################################
  def uploadLogFiles(self):
    """ upload log files if any using failover log SEs in case
        of failures
    """

    files = os.listdir('.')
    files.append('std.out')
    files.append('std.err')

    # Ugly !!!  - distinguish log files by their extensions
    logexts = ['.txt','.hbook','.log','.root','.out','.output','.xml','.sh', '.info', '.err']

    ##################################################
    #  Create the job log directory

    if not os.path.exists(self.logdir):
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

    for f in files:
      ext = os.path.splitext(f)[1]
      if ext in logexts:
        self.log.info( 'Saving log file: %s' % str( f ) )
        self.saveLogFile(f)

    self.log.info("Transferring log files to LogSE")
    target_path = makeProductionPath(self.JOB_ID,self.LFN_ROOT,'LOG',self.mode,self.PRODUCTION_ID,log=True)
    self.log.info("PutDirectory %s %s %s " % (target_path, os.path.realpath(self.logdir),self.logSE))
    result = self.rm.putDirectory(target_path,os.path.realpath(self.logdir),self.logSE)
    self.log.info(result)

    # Construct the http reference to the Log directory
    #  Check to HAVE HTTP protocol in the CS for LOGSE to avoid hardcoding path...
    if result['OK']:
      logref = '<a href="http://lhcb-logs.cern.ch/storage%s/%s/">Log file directory</a>' % (target_path, str(self.JOB_ID))
    else:
      logref = '<a href="http://lhcb-logs.cern.ch/storage%s/%s/log.tar.gz">Log file directory</a>' % (target_path, str(self.JOB_ID))
    self.log.info(logref)
    self.setJobParameter('Log URL',logref)

    if not result['OK']:
      self.log.error("Transferring log files to the main LogSE failed")
      self.log.error( result['Message'] )
    else:
      self.log.info("Transferring log files to the main LogSE successful")
      return S_OK()

    if not result['OK']:
      # Make failover copy and set the corresponding request

      ldir = os.path.abspath(self.logdir)
      ldirname = os.path.dirname(ldir)
      lbasename = os.path.basename(self.logdir)
      ltarname = lbasename+'.tar.gz'
      comm = 'tar czvf '+ltarname+' -C '+ldirname+' '+lbasename
      result = shellCall(0,comm)
      if result['OK']:
        lfn = target_path+'/'+ltarname
        result = self.uploadFileFailover(ltarname,lfn)
        if result['OK']:
          if result.has_key('Register'):
            self.setPfnReplicationRequest(lfn,'LogSE',result,removeOrigin=True)
          else:
            self.setReplicationRequest(lfn,'LogSE')
          return S_OK()
        else:
          self.log.error('Failed to store log files to LogSE and failover SEs')
      else:
        self.log.error('Failed to create log files tar archive for job %s' % self.jobID)


    return S_ERROR('Failed to store log files to LogSE and failover SEs')

################################################################################
  def uploadOutput(self):
    """ Perform all the necessary operations for the output data upload
    """

    all_done = True
    # Get list of existing catalogs
    self.existingCatalogs = []
    result = gConfig.getSections('/Resources/FileCatalogs')
    if result['OK']:
      self.existingCatalogs = result['Value']
    self.masterCatalog = ''
    for cat in self.existingCatalogs:
      masterFlag = gConfig.getValue('/Resources/FileCatalogs/%s/Master' % cat,False)
      if masterFlag:
        self.masterCatalog = cat
    if self.masterCatalog:
      self.log.verbose('Master Catalog found %s' % self.masterCatalog)
    else:
      self.log.warn('Could not determine Master Catalog')

    ####################################
    # Get the Pool XML catalog if any
    self.__getPoolXMLCatalogs()

    #########################################################################
    # Store to the grid and register the output files
    # The output files will be stored only if this is requested explicitely
    # in the job workflow or this is requested in the local configuration for
    # the given file type
    #
    # Make up to max_attempts to upload each file

    outputs = []
    self.log.info(self.listoutput)
    for item in self.listoutput:
      outputName = ''
      outputSE = ''
      outputType = ''
      if item.has_key('outputDataName'):
        outputName = item['outputDataName']
      if item.has_key('outputDataSE'):
        outputSE = item['outputDataSE']
      if item.has_key('outputDataType'):
        outputType = item['outputDataType']

      if self.outputDataFileMask:
        if outputType in self.outputDataFileMask:
          self.log.info('File name: %s will be uploaded to outputSE %s' %(outputName,outputSE))
          outputs.append((outputName,outputSE,outputType))
        else:
          self.log.info('File name %s will be ignored since output data file mask is %s' %(outputName,string.join(self.outputDataFileMask,', ')))
      else:
        self.log.info('No output data file mask is specified, all outputs will be uploaded')
        outputs.append((outputName,outputSE,outputType))


    #print "AT >>>>", outputs

    outputs_done = []
    lfns_done = []
    failover_lfns_done = []
    all_done = False
    count = 0
    max_attempts = 1
    while not all_done and count < max_attempts :
      if count > 0:
        self.log.info("Output data upload retry number "+str(count))
      all_done = True
      for outputName,outputSE,outputType in outputs:
        if not outputName in outputs_done:
          resUpload = self.uploadOutputData(outputName,outputType.upper(),outputSE)
          if resUpload['OK']:
            outputs_done.append(outputName)
            lfns_done.append(result['Value'])
            if resUpload['Failover']:
              failover_lfns_done.append(result['Value'])
          else:
            all_done = False

      count += 1

    outputs_failed = []
    for outputName,outputSE,outputType in outputs:
      if not outputName in outputs_done:
        outputs_failed.append(outputName)
    if all_done:
      # We are lucky today !
      result = S_OK()
      result['Value'] = {}
      result['Value']['LFNs'] = lfns_done
      result['Value']['Successful'] = outputs_done
      result['Value']['FailoverLFNs'] = failover_lfns_done
      result['Value']['Failed'] = outputs_failed
    else:
      # No chance today ;(
      if lfns_done:
        resRemoval = self.setRemovalRequest(lfns_done)
      result = S_ERROR('Failed to upload output data')
      result['Value'] = {}
      result['Value']['Failed'] = outputs_failed
      result['Value']['Successful'] = outputs_done
      result['Value']['FailoverLFNs'] = failover_lfns_done
      result['Value']['LFNs'] = lfns_done
    return result

################################################################################
  def uploadOutputData(self,output,otype,outputse):
    """ Determine the output file destination SEs and upload file to the grid
    """

    # Prepare the output file metadata
    # Get the GUID first
    guid = None
    if self.poolcat:
      guid = self.poolcat.getGuidByPfn(output)
      if not guid: guid = None

    if guid is None:
      self.log.warn("GUID is not defined for file %s" % output)
      guid = makeGuid()
      self.log.info("Generated GUID: %s" % str(guid))
    else:
      self.log.info("File: %s GUID: %s" % (str(output), str(guid)))

    size = getfilesize(output)
    checkSum = fileAdler(output)
    # AT >>>> debug
    #lfn = makeProductionLfn(self.JOB_ID,self.LFN_ROOT,(output,otype),self.mode,self.PRODUCTION_ID)
    lfn = "/lhcb/test/test.file"
    # AT >>>> debug

    fileDict = {}
    fileDict['LFN'] = lfn
    fileDict['Size'] = size
    fileDict['Addler'] = checkSum
    fileDict['GUID'] = guid
    fileDict['Status'] = "Waiting"

    se_groups = [ x.strip() for x in outputse.split(',')]
    done_ses = []

    #print "AT >>>> se_groups", se_groups

    # Process each SE or SE group in turn. Make the first pass without failover
    succeeded_ses = {}

    DATA_MODES = ['Upload','Replication','Failover','Replication']
    all_done = False
    all_failed = False
    full_upload = False
    pfn_upload = None
    failover_done = False

    for mode in DATA_MODES:

      if all_done or all_failed:
        break

      for se_index in range(len(se_groups)):

        #print "AT >>>> index", se_index,mode,range(len(se_groups))

        se_group = se_groups[se_index]
        outputmode,se_group,jobLimit = self.__parseOutputGroup(se_group)

        #print "AT >>>> ",outputmode,se_group,jobLimit

        # Check if the maximum number of jobs is set
        if jobLimit > 0 and self.JOB_ID:
          if int(self.JOB_ID) >= jobLimit:
            self.log.info('Job number %d exceeds the limit %d, skipping file' (int(self.JOB_ID),jobLimit))
            continue

        result = self.__getDestinationSEList(se_group, outputmode)
        if not result['OK']:
          self.log.error('No valid SEs defined as file destinations',se_group)
          self.log.error('Could not resolve local SE',result['Message'])
          return result
        out_ses = uniq(result['Value'])

        # Check if the output is already uploaded to some overlapping SE
        for se in done_ses:
          if se in out_ses:
            pop(out_ses,se)

        # First pass upload mode
        if mode == "Upload":
          result = self.uploadDataFile(output,lfn,out_ses,outputmode,guid,fileDict)
          if result['OK']:
            res_ses = result['Value']['Successful'] + result['Value']['Register']
            if res_ses:
              if not succeeded_ses.has_key(se_index):
                succeeded_ses[se_index] = []
              for se in res_ses:
                succeeded_ses[se_index].append(se)
              done_ses += res_ses

              if result['Value']['Successful']:
                full_upload = True
              elif result['Value']['Register']:
                pfn_upload = result['Value']['FileDictionary']['PFN']

              if not self.uploadAllFlag:
                break

        # If we have got some successful upload, set replication requests
        # for other destinations
        elif mode == "Replication":

          #print "AT >>>> replication",succeeded_ses,full_upload,pfn_upload

          if succeeded_ses:
            if full_upload:
              fDict = {}
            elif pfn_upload:
              fDict = fileDict
              fDict['PFN'] = pfn_upload
            else:
              self.log.warn("Inconsistent success reported !")
              break

          #print "AT >>> replication 2",failover_done

          # Set replication requests now
          result = S_ERROR('Nothing to replicate')
          if failover_done:
            result = self.replicateFile(lfn,fDict,out_ses,outputmode,[],True)
          elif  succeeded_ses.has_key(se_index):
            done_group_ses = succeeded_ses[se_index]
            result = self.replicateFile(lfn,fDict,out_ses,outputmode,done_group_ses)
          elif succeeded_ses:
            result = self.replicateFile(lfn,fDict,out_ses,outputmode,[])

          if result['OK']:
            res_ses = result['Value']
            if not succeeded_ses.has_key(se_index):
              succeeded_ses[se_index] = []
            for se in res_ses:
              succeeded_ses[se_index].append(se)
            done_ses += res_ses

          if succeeded_ses:
            all_done = True

        elif mode == "Failover":
          # if no upload was successful so far, try failover upload
          if not succeeded_ses:
            result = self.uploadDataFileFailover(output,lfn,guid)

            #print "AT >>>> uploadDataFileFailover ", result

            if result['OK']:
              succeeded_ses[999] = []
              done_se = result['FailoverSE']
              succeeded_ses[999].append(done_se)
              if not result.has_key('PFN'):
                full_upload = True
              else:
                pfn_upload = result['PFN']
              failover_done = True
            else:
              all_failed = True
          # We do not need to do that twice
          break

        #print "AT >>>> end of loop", mode,  succeeded_ses

    #print "AT >>>> done status",all_done, all_failed,succeeded_ses

    if all_done:
      result = S_OK(lfn)
      result['Failover'] = False
      if failover_done:
        result['Failover'] = True
      return result
    else:
      return S_ERROR('Failed to upload file ' + output)

#############################################################################
  def __parseOutputGroup(self,se_group):
    """ Parse the output destination specification
    """

    outputmode = "Local"
    outputgroup = "Unknown"
    jobNumberLimit = 0
    items = se_group.split(":")
    if len(items) == 1:
      outputgroup = items[0]
    elif len(items) == 2:
      if items[1].find('<') != -1:
        jobNumberLimit = int(items[1].split('<')[1])
        outputgroup = items[0]
      else:
        outputmode = items[0]
        outputgroup = items[1]
    elif len(items) == 3:
      if items[2].find('<') == -1:
        self.log.error('Illegal output destination %s' % se_group)
      else:
        outputmode = items[0]
        outputgroup = items[1]
        jobNumberLimit = int(items[2].split('<')[1])
    else:
      self.log.error('Illegal output destination %s' % se_group)

    return outputmode,outputgroup,jobNumberLimit

#############################################################################
  def __getDestinationSEList(self,outputSE, outputmode):
    """ Evaluate the output SE list
    """

    SEs = []
    # Add output SE defined in the job description
    self.log.info('Resolving '+outputSE+' SE')

    # Check if the SE is defined explicitly for the site
    prefix = self.site.split('.')[0]
    country = self.site.split('.')[-1]
    # Concrete SE name
    result = gConfig.getOptions('/Resources/StorageElements/'+outputSE)
    if result['OK']:
      self.log.info('Found concrete SE %s' %outputSE)
      return S_OK([outputSE])
    # There is an alias defined for this Site
    alias_se = gConfig.getValue('/Resources/Sites/%s/%s/AssociatedSEs/%s' % (prefix,self.site,outputSE),[])
    if alias_se:
      self.log.info('Found associated SE for site %s' %(alias_se))
      return S_OK(alias_se)

    localSEs = self.__getLocalSEList()
    self.log.info('Local SE list is: %s' %(localSEs))
    groupSEs = gConfig.getValue('/Resources/StorageElementGroups/'+outputSE,[])
    self.log.info('Group SE list is: %s' %(groupSEs))
    if not groupSEs:
      return S_ERROR('Failed to resolve SE '+outputSE)

    if outputmode == "Local":
      for se in localSEs:
        if se in groupSEs:
          self.log.info('Found SE for Local mode: %s' %(se))
          return S_OK([se])

      #check if country is already one with associated SEs
      associatedSE = gConfig.getValue('/Resources/Countries/%s/AssociatedSEs/%s' %(country,outputSE),'')
      if associatedSE:
        self.log.info('Found associated SE %s in /Resources/Countries/%s/AssociatedSEs/%s' %(associatedSEs,country,outputSE))
        return S_OK([associatedSE])

      # Final check for country associated SE
      count = 0
      assignedCountry = country
      while count<10:
        self.log.verbose('Loop count = %s' %(count))
        self.log.verbose("/Resources/Countries/%s/AssignedTo" %assignedCountry)
        opt = gConfig.getOption("/Resources/Countries/%s/AssignedTo" %assignedCountry)
        if opt['OK'] and opt['Value']:
          assignedCountry = opt['Value']
          self.log.verbose('/Resources/Countries/%s/AssociatedSEs' %assignedCountry)
          assocCheck = gConfig.getOption('/Resources/Countries/%s/AssociatedSEs' %assignedCountry)
          count += 1
          if assocCheck['OK'] and assocCheck['Value']:
            break
#        else:
#          break

      if not assignedCountry:
        self.log.info('Could not establish associated country for site')
        return S_ERROR('Could not determine associated SE list for %s' %country)

      alias_se = gConfig.getValue('/Resources/Countries/%s/AssociatedSEs/%s' %(assignedCountry,outputSE),[])
      if alias_se:
        self.log.info('Found alias SE for site: %s' %alias_se)
        return S_OK(alias_se)
      else:
        self.log.info('Could not establish alias SE for country %s from section: /Resources/Countries/%s/AssociatedSEs/%s' %(country,assignedCountry,outputSE))
        return S_ERROR('Failed to resolve SE '+outputSE)

    # For collective Any and All modes return the whole group

    # Make sure that local SEs are passing first
    newSEList = []
    for se in groupSEs:
      if se in localSEs:
        newSEList.append(se)
    SEs = uniq(newSEList+groupSEs)
    self.log.info('Found unique SEs: %s' %(SEs))
    return S_OK(SEs)

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
  def __getLocalSEList(self):
    """ Ge the list of local Storage Element names
    """
    result = getSEsForSite(self.site)
    if not result['OK']:
      self.log.warn('Could not get SEs for site with result\n%s' %result)
      return []
    return result['Value']

#############################################################################
  def replicateFile(self,lfn,fileDict,out_ses,outputmode,succeeded_ses,failoverFlag=False):
    """ Set replication requests for the given LFN according to the given
        output mode
    """

    #print "AT >>>> replicateFile",lfn,out_ses,succeeded_ses,fileDict

    done_ses = []

    if (outputmode == "Local" or outputmode == "Any"):
      if succeeded_ses:
        return S_OK()
      else:
        if fileDict:
          result = self.setPfnReplicationRequest(lfn,out_ses[0],fileDict,removeOrigin=failoverFlag)
        else:
          result = self.setReplicationRequest(lfn,out_ses[0],removeOrigin=failoverFlag)
        if result['OK']:
          done_ses.append(out_ses[0])
    elif outputmode == "All":
      for se in out_ses:
        if se not in succeeded_ses:
          if fileDict:
            result = self.setPfnReplicationRequest(lfn,se,fileDict,removeOrigin=failoverFlag)
          else:
            result = self.setReplicationRequest(lfn,se,removeOrigin=failoverFlag)
          if result['OK']:
            done_ses.append(se)

    return S_OK(done_ses)

###################################################################################################
  def uploadDataFile(self,datafile,lfn,destinationSEList,outputmode,guid,fileDict,failover=False):
    """ Upload a datafile to the grid and set necessary replication requests
    """

    if outputmode == "All":
      self.log.info("Attempting to store file %s to the following SEs:\n%s" % (datafile, str(destinationSEList)))
    elif len(destinationSEList) == 1:
      self.log.info("Attempting to store file %s to SE: %s" % (datafile, str(destinationSEList[0])))
    else:
      self.log.info("Attempting to store file %s to any of the following SEs:\n%s" % (datafile, str(destinationSEList)))

    destination_ses = uniq(destinationSEList)

    done_ses = []
    failed_ses = []
    register_ses = []
    fDict = {}

    for se in destination_ses:
      result = self.uploadDataFileToSE(datafile,lfn,se,guid)

      if result['OK']:
        if result.has_key('Register'):
          fDict.update(fileDict)
          fDict['PFN'] = result['PFN']
          for cat in result['Catalogs']:
            result = self.setRegistrationRequest(se,cat,fDict)
          register_ses.append(se)
        else:
          done_ses.append(se)

        if outputmode != "All":
          break

      else:
        failed_ses.append(se)

    resultDict = {}
    resultDict['Successful'] = done_ses
    resultDict['Failed'] = failed_ses
    resultDict['Register'] = register_ses
    resultDict['FileDictionary'] = fDict

    result = S_OK(resultDict)

    #print "AT >>>>  uploadDataFile result", result

    return result

############################################################################################
  def uploadDataFileToSE(self,datafile,lfn,se,guid):
    """ Upload a datafile to a destination SE, set replica registration request if necessary
    """

    #print "uploadDataFileToSE",datafile,lfn,se,guid

    fname = os.path.basename(datafile)
    lfn_directory = os.path.dirname(lfn)
    size = getfilesize(datafile)

    self.log.info("Copying %s to %s" % (datafile,se))
    result = self.rm.putAndRegister(lfn,os.getcwd()+'/'+fname,se,guid)

    # AT >>>> debug
    #if se == "IN2P3-disk":
    #  result = S_OK()
    #  result['Value'] = {}
    #  result['Value']['Failed'] = {}
    #  result['Value']['Failed'][lfn] = {'register':{'PFN':'/some/pfn'}}
    # AT >>>> debug

    if result['OK']:
      self.log.info('Upload of %s to %s successful' % (datafile,se))
      # Transfer is OK, let's check the registration part
      if result['Value']['Failed']:
        resDict = result['Value']['Failed'][lfn]
        if resDict.has_key('register'):
          registerDict = resDict['register']
          res = S_OK()
          res['Register'] = registerDict
          res['Catalogs'] = []
          for key,value in resDict.items():
            if key in self.existingCatalogs:
              res['Catalogs'].append(key)
          res['PFN'] = registerDict['PFN']
          return res
        else:
          self.log.warn('Error while %s file upload:' % fname)
          self.log.warn(str(resDict))
          return S_ERROR('Unknown error')
      else:
        return S_OK()
    else:
      self.log.error('Failed to upload %s to %s:\n' % (datafile,se),result['Message'])
      return result

#############################################################################################
  def uploadDataFileFailover(self,datafile,lfn,guid):
    """ Upload file to a failover SE
    """

    fname = os.path.basename(datafile)
    failover_ses = gConfig.getValue('/Resources/StorageElementGroups/Tier1-Failover',[])
    random.shuffle(failover_ses)
    print failover_ses

    count = 0
    max_count = 3
    while (count < max_count):
      count += 1
      for se in failover_ses:
        result = self.uploadDataFileToSE(datafile,lfn,se,guid)
        if result['OK']:
          result['FailoverSE'] = se
          return result

    return S_ERROR('Failed to store %s file to any failover SE' % fname)

#############################################################################################
  def setReplicationRequest(self,lfn,se,removeOrigin=False):
    """ Set replication request for lfn with se Storage Element destination
    """

    self.log.info('Setting replication request for %s to %s' % (lfn,se))
    if removeOrigin:
      result = self.request.addSubRequest({'Attributes':{'Operation':'replicateAndRegisterAndRemove',
                                                         'TargetSE':se,'ExecutionOrder':1}},
                                           'transfer')
    else:
      result = self.request.addSubRequest({'Attributes':{'Operation':'replicateAndRegister',
                                                         'TargetSE':se,'ExecutionOrder':1}},
                                           'transfer')
    if not result['OK']:
      return result

    index = result['Value']
    fileDict = {'LFN':lfn,'Status':'Waiting'}
    result = self.request.setSubRequestFiles(index,'transfer',[fileDict])

    return S_OK()

#############################################################################################
  def setRegistrationRequest(self,se,catalog,fileDict,executionOrder=1):
    """ Set replication request for lfn with se Storage Element destination
    """

    l_se = se
    l_catalog = catalog
    lfn = fileDict['LFN']
    self.log.info('Setting registration request for %s at %s for catalog %s' % (lfn,l_se,str(l_catalog)))
    result = self.request.addSubRequest({'Attributes':{'Operation':'registerFile','ExecutionOrder':executionOrder,
                                                       'TargetSE':l_se,'Catalogue':l_catalog}},'register')
    if not result['OK']:
      return result

    index = result['Value']
    result = self.request.setSubRequestFiles(index,'register',[fileDict])

    return S_OK()

#############################################################################################
  def setPfnReplicationRequest(self,lfn,se,fileDict,removeOrigin=False):
    """ Set replication request for lfn with se Storage Element destination
    """

    self.log.info('Setting PFN replication request for %s to %s' % (lfn,se))
    if removeOrigin:
      result = self.request.addSubRequest({'Attributes':{'Operation':'putAndRegisterAndRemove',
                                                         'TargetSE':se,'ExecutionOrder':1}},
                                           'transfer')
    else:
      result = self.request.addSubRequest({'Attributes':{'Operation':'putAndRegister',
                                                         'TargetSE':se,'ExecutionOrder':1}},
                                           'transfer')
    if not result['OK']:
      return result

    index = result['Value']
    fileDict['Status'] = 'Waiting'
    result = self.request.setSubRequestFiles(index,'transfer',[fileDict])

    return S_OK()

#############################################################################################
  def setRemovalRequest(self,lfnList):
    """ Clean up uploaded data for the LFNs in the list
    """

    # Clean up the current request
    for req_type in ['transfer','register']:
      for lfn in lfnList:
        result = self.request.getNumSubRequests(req_type)
        if result['OK']:
          nreq = result['Value']
          if nreq:
            # Go through subrequests in reverse order in order not to spoil the numbering
            ind_range = [0]
            if nreq > 1:
              ind_range = range(nreq-1,-1,-1)
            for i in ind_range:
              result = self.request.getSubRequestFiles(i,req_type)
              if result['OK']:
                fileList = result['Value']
                if fileList[0]['LFN'] == lfn:
                  result = self.request.removeSubRequest(i,transfer)

      # Set removal requests just in case
      for lfn in lfnList:
        result = self.request.addSubRequest({'Attributes':{'Operation':'removeFile'}},'removal')
        index = result['Value']
        fileDict = {'LFN':lfn,'Status':'Waiting'}
        result = self.request.setSubRequestFiles(index,'removal',[fileDict])

    return S_OK()

#############################################################################
  def createRequest(self,fileReportFlag=False):
    """ Create and send a combined request for all the pending operations
    """

    # get the accumulated reporting request
    reportRequest = None
    if self.jobReport:
      result = self.jobReport.generateRequest()
      if not result['OK']:
        self.log.warn('Could not generate request for job report with result:\n%s' %(result))
      else:
        reportRequest = result['Value']
    if reportRequest:
      self.request.update(reportRequest)


    if self.fileReport and fileReportFlag:
      result = self.fileReport.generateRequest()
      if not result['OK']:
        self.log.warn('Could not generate request for file report with result:\n%s' %(result))
      else:
        reportRequest = result['Value']
    if reportRequest:
      result = self.request.update(reportRequest)

    accountingReport = None
    if self.workflow_commons.has_key('AccountingReport'):
      accountingReport  = self.workflow_commons['AccountingReport']
    if accountingReport:
      result = accountingReport.commit()
      if not result['OK']:
        self.request.addSubRequest(DISETSubRequest(result['rpcStub']).getDictionary(),'diset')

    if self.request.isEmpty()['Value']:
      return S_OK()

    request_string = self.request.toXML()['Value']
    self.log.debug(request_string)
    # Write out the request string
    fname = self.PRODUCTION_ID+"_"+self.JOB_ID+"_request.xml"
    xmlfile = open(fname,'w')
    xmlfile.write(request_string)
    xmlfile.close()
    return S_OK()

