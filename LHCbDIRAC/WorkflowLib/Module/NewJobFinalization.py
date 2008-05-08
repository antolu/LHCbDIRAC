########################################################################
# $Id: NewJobFinalization.py,v 1.6 2008/05/08 21:13:11 atsareg Exp $
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

__RCSID__ = "$Id: NewJobFinalization.py,v 1.6 2008/05/08 21:13:11 atsareg Exp $"

############### TODO
# Cleanup import of unnecessary modules

from DIRAC.DataManagementSystem.Client.Catalog.BookkeepingDBClient import *
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.StorageElement import StorageElement
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog import PoolXMLCatalog
from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from DIRAC.Core.DISET.RPCClient                       import RPCClient
from DIRAC                                            import S_OK, S_ERROR, gLogger, gConfig
from WorkflowLib.Utilities.Tools import *
from DIRAC.RequestManagementSystem.Client.DataManagementRequest import DataManagementRequest
from DIRAC.RequestManagementSystem.Client.DISETSubRequest import DISETSubRequest
import os, time, re, random, shutil, commands

class JobFinalization(object):

  def __init__(self):

    ############### TODO
    # Remove unused variables
    # Make checks of the necessary variables

    #####################################################
    # Variables supposed to be set by the workflow

    self.PRODUCTION_ID = None
    self.JOB_ID = None

    # A list of dictionaries specifying the output data
    self.listoutput = []

    self.poolXMLCatName = None
    self.SourceData = None
    self.workflow_commons = None

    #####################################################
    # Local variables

    self.jobID = ""
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']

    self.LFN_ROOT = None
    self.logdir = '.'
    self.mode = None
    self.logSE = 'LogSE'
    self.bookkeepingTimeOut = 10 #seconds
    self.root = gConfig.getValue('/LocalSite/Root',os.getcwd())
    self.site = gConfig.getValue('/LocalSite/Site','localSite')

    self.rm = ReplicaManager()
    self.bk = BookkeepingClient()
    self.bkDB = BookkeepingDBClient()
    self.jobReport  = None
    self.request = DataManagementRequest()
    self.request.setRequestName('job_%s_request.xml' % self.jobID)
    self.request.setJobID(self.jobID)
    self.request.setSourceComponent("Job_%s" % self.jobID)

    self.log = gLogger.getSubLogger("JobFinalization")
    self.log.setLevel('debug')

######################################################################
  def execute(self):
    """ Main executon method
    """

    # Add global reporting tool
    if self.workflow_commons.has_key('JobReport'):
      self.jobReport  = self.workflow_commons['JobReport']

    result = self.__report('Job Finalization')

    print "AT ->>>>>>>>>>>>>", self.listoutput
    res = shellCall(0,'ls -al')
    if res['OK'] == True:
      self.log.info("final listing : %s" % (str(res['Value'][1])))
    else:
      self.log.info("final listing with error: %s" % (str(res['Value'][2])))

    self.log.info('Site root is found to be %s' %(self.root))
    self.log.info('Updating local configuration with available CFG files')
    self.__loadLocalCFGFiles(self.root)
    self.mode = gConfig.getValue('LocalSite/Setup','test')
    self.log.info('PRODUTION_ID = %s, JOB_ID = %s ' %(self.PRODUCTION_ID,self.JOB_ID))
    self.logdir = os.path.realpath('./job/log/'+self.PRODUCTION_ID+'/'+self.JOB_ID)
    self.log.info('Log directory is %s' %self.logdir)
    self.logtar = self.PRODUCTION_ID+'_'+self.JOB_ID+'.tar'
    self.log.info('Log tar file name is %s' %self.logtar)

    error = 0
    if self.step_commons.has_key('Status'):
      if step_commons['Status'] == "Error":
        error = 1

    self.LFN_ROOT = getLFNRoot(self.SourceData)

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
    request_done = True
    error_message = ''

    self.log.info( "Start finalization %s %s" % ( str( error ), str( self.PRODUCTION_ID ) ) )
    if not self.PRODUCTION_ID:
      return S_OK()

    ########################################################
    # Store log files if even the job failed

    try:
      resultLog = self.uploadLogFiles()
      if not resultLog['OK']:
        logs_done = False
    except Exception,x:
      self.log.error("Exception while log files uploading:")
      self.log.error(str(x))

    if not error:
      ########################################################
      # Upload the output data

      resultUpload = self.uploadOutput()

      #################################################################
      # Send bookkeeping only if the data was uploaded successfully

      if resultUpload['OK']:
        resultBK = self.reportBookkeeping()
        if not resultBK['OK']:
          self.log.error(resultBK['Message'])
          bk_done = False
      else:
        data_done = False

    if not data_done:
      self.__report('Failed to save output data')
      result = S_ERROR('Failed to save output data')

    if logs_done and data_done and bk_done and not error:
      self.__report('Job Finished Successfully')

    if not logs_done:
      error_message = "failed to save logs, "
    if not bk_done and not request_done:
      error_message += "failed to send bk data, "

    if error_message:
      error_message = error_message[:-2]
      self.__setJobParameter('FinalizationError',error_message)

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
        result = self.bk.sendBookkeeping(fm,xmlstring)
        if not result['OK']:
          counter += 1
          if result['Message'].find('Connection timed out') != -1:
            time.sleep(self.bookkeepingTimeOut)
          if counter > 3:
            self.log.error( "Failed to send bookkeeping information for %s: %s" % (str(f),result['Message']) )
            self.request.addSubRequest('bookkeeping',DISETSubRequest(result['rpcStub']))
            send_flag = False
            result = S_ERROR('Failed to send bookkeeping information for %s' % str(f))
        else:
          self.log.info( "Bookkeeping %s sent successfully" % f )
          send_flag = False

    return result

################################################################################
  def __getPoolXMLCatalogs(self):
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
  def saveLogFile(self,logfile,gzip_flag=1):

    if not os.path.exists(logfile):
      self.log.error( "Saving log file %s failed: no such file" % logfile )
      return S_ERROR('No such file')

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
      return S_ERROR('File copy failed: '+str(x))

    # Do not leave gzipped files in the working directory.
    # They may be still used later
    gunzip(logfile)
    return S_OK()

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
    if result['OK']:
      logref = '<a href="http://lhcb-logs.cern.ch/storage%s/%s/">Log file directory</a>' % (target_path, str(self.JOB_ID))
    else:
      logref = '<a href="http://lhcb-logs.cern.ch/storage%s/%s/log.tar.gz">Log file directory</a>' % (target_path, str(self.JOB_ID))
      self.log.info(logref)
      self.__setJobParam('Log URL',logref)

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
      outputMode = 'Local'
      outputType = ''
      if item.has_key('outputDataName'):
        outputName = item['outputDataName']
      if item.has_key('outputDataSE'):
        outputSE = item['outputDataSE']
      if item.has_key('outputDataMode'):
        outputMode = item['outputDataMode']
      if item.has_key('outputDataType'):
        outputType = item['outputDataType']
      outputs.append((outputName,outputSE,outputType,outputMode))
    outputs_done = []
    all_done = False
    count = 0
    max_attempts = 1
    while not all_done and count < max_attempts :
      if count > 0:
        self.log.info("Output data upload retry number "+str(count))
      all_done = True
      for outputName,outputSE,outputType,outputMode in outputs:
        if not outputName in outputs_done:
          resUpload = self.uploadOutputData(outputName,outputType.upper(),outputSE,outputMode)
          if resUpload['OK']:
            outputs_done.append(outputName)
          else:
            
            all_done = False
      count += 1

    outputs_failed = []
    for outputName,outputSE,outputType,outputMode in outputs:
      if not outputName in outputs_done:
        outputs_failed.append(outputName) 
    if all_done:
      return S_OK()
    else:
      result = S_ERROR('Failed to upload output data')  
      result['Value'] = {}
      result['Value']['Failed'] = outputs_failed
      result['Value']['Successful'] = outputs_done

################################################################################
  def uploadOutputData(self,output,otype,outputse,outputmode):
    """ Determine the output file destination SEs and upload file to the grid
    """

    result = self.__getDestinationSEList(outputse)
    if not result['OK']:
      self.log.error('No valid SEs defined as file destinations')
      return result
    ses = result['Value']

    lfn = makeProductionLfn(self.JOB_ID,self.LFN_ROOT,(output,otype),self.mode,self.PRODUCTION_ID)
    if outputmode == "Any":
      result = self.uploadDataFile(output,lfn,ses,allSEs=False)
    elif outputmode == "All":
      result = self.uploadDataFile(output,lfn,ses,allSEs=True)
    elif outputmode == "Local":
      ses = ses[:1]
      result = self.uploadDataFile(output,lfn,ses,allSEs=True)
    return result

#############################################################################
  def __getDestinationSEList(self,outputSE):
    """ Evaluate the output SE list
    """

    SEs = []
    # Add output SE defined in the job description
    self.log.info(outputSE)
    if outputSE != None:
      outSEs = outputSE.split(',')
      for outSE in outSEs:
        print "AT>>>>>>>>>>>>>>>>> ", outSE, '/Operations/StorageElementGroups/'+outSE
        csSEs = gConfig.getValue('/Operations/StorageElementGroups/'+outSE,[])
        if csSEs:
          SEs += csSEs
        else:
          # Check if this is a concrete SE name
          se =  gConfig.getValue('/Resources/StorageElement/'+outSE,'')
          if se:
            SEs.append(se)

    if not SEs:
      return S_ERROR('No valid SE names specified')

    localSEs = self.__getLocalSEList()

    # Make sure that local SEs are passing first
    newSEList = []
    for se in SEs:
      if se in localSEs:
        newSEList.append(se)
    SEs = uniq(newSEList+SEs)

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

    # This should be done in a better defined way elsewhere
    self.__loadLocalCFGFiles(self.root+'/../')
    self.__loadLocalCFGFiles(os.getcwd())
    self.__loadLocalCFGFiles(os.getcwd()+'/../')

    # SEs defined locally
    localses = gConfig.getValue('/LocalSite/LocalSE',[])
    prefix = self.site.split('.')[0]
    sname = self.site.replace(prefix+'.','')
    # SEs defined in the Configuration Service
    csses = gConfig.getValue('/Resources/Sites/%s/%s/SE' % (prefix,sname),[])
    ses = uniq(localses+csses)
    return ses

###################################################################################################
  def uploadDataFile(self,datafile,lfn,destinationSEList,allSEs=True):
    """ Upload a datafile to the grid and set necessary replication requests
    """

    if allSEs:
      self.log.info("File %s will be stored to the following SEs:\n%s" % (datafile, str(destinationSEList)))
    else:
      self.log.info("File %s will be stored to any of the following SEs:\n%s" % (datafile, str(destinationSEList)))


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

    done_ses = []
    failed_ses = []
    request_ses = []
    failover_ses = []
    for se in destination_ses:
      result = self.uploadDataFileToSE(datafile,lfn,se,guid)
      if result['OK']:
        done_ses.append(se)
        if result.has_key('Register'):
          self.setRegistrationRequest(result)
          request_ses.append(se)
        if not allSEs:
          # We can stop here by setting appropriate requests
          for sse in destination_ses:
            if sse != se and sse not in done_ses:
              self.setReplicationRequest(lfn,se,removeOrigin=False)
              request_ses.append(sse)
          resultDict = {}
          resultDict['Successful'] = done_ses
          resultDict['Failed'] = failed_ses
          resultDict['RequestSet'] = request_ses
          result = S_OK(resultDict)

      else:
        self.log.warn(result)
        self.__setJobParam('Upload failed for file: %s to SE: %s' %(datafile,se),'Size: %s, LFN: %s, GUID: %s' %(size,lfn,guid))
        # Try failover destination now
        result = self.uploadFileFailover(datafile,lfn,guid)
        if result['OK']:
          request_ses.append(se)
          if result.has_key('Register'):
            self.setPfnReplicationRequest(lfn,se,result,removeOrigin=True)
          else:
            self.setReplicationRequest(lfn,se,removeOrigin=True)
          failover_ses.append(se)
        else:
          failed_ses.append(se)

    resultDict = {}
    resultDict['Successful'] = done_ses
    resultDict['Failed'] = failed_ses
    resultDict['RequestSet'] = request_ses
    resultDict['Failed'] = failover_ses
    
    
    print "AT >>>>>>>>>>>>>>>>>>>>>>> __________", resultDict
    
    if failed_ses:
      result = S_ERROR('Failed to save file %s' % lfn)
      result['Value']
    else:
      return S_OK(resultDict)

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
    
    
    print "AT >>>>>>>>>>>>>>>>>>", result
    
    if result['OK']:
      # Transfer is OK, let's check the registration part
      if result['Value']['Failed']:
        resDict = result['Value']['Failed'][lfn]
        if resDict.has_key('register'):
          registerDict = resDict['register']
          res = S_OK()
          res['Register'] = resDictregisterDict
          res['Catalogs'] = []
          res['PFN'] = resDictregisterDict['PFN']
          return res
        else:
          self.log.warn('Error while %s file upload:' % fname)
          self.log.warn(str(resDict))
          return S_ERROR('Unknown error')
      else:
        return S_OK()
    else:
      return result


#############################################################################################
  def uploadFileFailover(self,datafile,lfn,guid):
    """ Upload file to a failover SE
    """

    fname = os.path.basename(datafile)
    failover_ses = gConfig.getValue('/Operations/StorageElementGroups/Tier1-Failover',[])
    random.shuffle(failover_ses)
    print failover_ses

    count = 0
    max_count = 3
    while (count < max_count):
      count =+ 1
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

    result = self.request.initiateSubRequest('transfer')
    if not result['OK']:
      return result

    index = result['Value']
    fileDict = {'LFN':lfn,'TargetSE':se}
    result = self.request.setSubRequestFiles(index,'transfer',[fileDict])
    if removeOrigin:
      result = self.addSubRequestAttributeValue(index,'transfer','Operation','moveAndRegister')
    else:
      result = self.addSubRequestAttributeValue(index,'transfer','Operation','replicateAndRegister')

    return S_OK()

#############################################################################################
  def setRegistrationRequest(self,fileDict):
    """ Set replication request for lfn with se Storage Element destination
    """

    result = self.request.initiateSubRequest('register')
    if not result['OK']:
      return result

    index = result['Value']
    result = self.request.setSubRequestFiles(index,'register',[fileDict])
    result = self.addSubRequestAttributeValue(index,'register','Operation','registerFile')

    return S_OK()

#############################################################################################
  def setPfnReplicationRequest(self,lfn,se,fileDict,removeOrigin=False):
    """ Set replication request for lfn with se Storage Element destination
    """

    result = self.request.initiateSubRequest('transfer')
    if not result['OK']:
      return result

    index = result['Value']
    result = self.request.setSubRequestFiles(index,'transfer',[fileDict])
    if removeOrigin:
      result = self.addSubRequestAttributeValue(index,'transfer','Operation','moveAndRegister')
    else:
      result = self.addSubRequestAttributeValue(index,'transfer','Operation','copyAndRegister')

    return S_OK()


  #############################################################################
  def createRequest(self):
    """ Create and send a combined request for all the pending operations
    """

    # get the accumulated reporting request
    reportRequest = None
    if self.jobReport:
      result = self.jobReport.generateRequest()
      reportRequest = result['Value']

    if reportRequest:
      self.request.update(reportRequest)
      
    if self.request.isEmpty():
      return S_OK()
        
    request_string = self.request.toXML()['Value']

    print "AT >>>>>>>>>>>>>>>@@@@@@@@@@@@@@@@@@@",request_string

    # Write out the request string
    fname = self.PRODUCTION_ID+"_"+self.JOB_ID+"_request.xml"
    xmlfile = open(fname,'w')
    xmlfile.write(request_string)
    xmlfile.close()

    return S_OK()

  #############################################################################
  def __report(self,status):
    """Wraps around setJobApplicationStatus of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    self.log.verbose('setJobApplicationStatus(%s,%s,%s)' %(self.jobID,status,'JobFinalization'))
    if not self.jobReport:
      return S_OK('No reporting tool given')
    jobStatus = self.jobReport.setApplicationStatus(status)
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
    if not self.jobReport:
      return S_OK('No reporting tool given')
    jobParam = self.jobReport.setJobParameter(str(name),str(value))
    if not jobParam['OK']:
      self.log.warn(jobParam['Message'])

    return jobParam
