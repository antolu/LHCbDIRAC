########################################################################
# $Id: JobFinalization.py,v 1.3 2008/02/15 07:52:36 joel Exp $
########################################################################


__RCSID__ = "$Id: JobFinalization.py,v 1.3 2008/02/15 07:52:36 joel Exp $"

from DIRAC.DataManagementSystem.Client.ReplicaManager    import ReplicaManager
from DIRAC.DataManagementSystem.Client.StorageElement import StorageElement
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog
from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from WorkflowLib.Utilities.Tools import *
from DIRAC import  *

import os, time, re, random

class JobFinalization(object):

  def __init__(self):
    self.STEP_ID = None
    self.CONFIG_NAME = None
    self.RUN_NUMBER = None
    self.FIRST_EVENT_NUMBER = None
    self.NUMBER_OF_EVENTS = None
    self.NUMBER_OF_EVENTS_OUTPUT = None
    self.PRODUCTION_ID = None
    self_JOB_ID = None
    self.tmpdir = '.'
    self.logdir = '.'
    self.mode = None
    self.poolXMLCatName = None
    self.outputDataSE = None
    self.log = gLogger.getSubLogger("JobFinalization")
    self.nb_events_input = None
    self.rm = ReplicaManager()
    self.bk = BookkeepingClient()
    self.transferID = ''
    pass

  def execute(self):

    res = shellCall(0,'ls -al')
    self.log.info("final listing : %s" % (str(res)))
    self.root = gConfig.getValue('/LocalSite/Root',os.getcwd())
    self.mode = gConfig.getValue('LocalSite/Setup','test')
    self.logdir = self.root+'/job/log/'+self.PRODUCTION_ID+'/'+self.JOB_ID
    error = 0
    result = self.finalize(error)

    return S_OK()


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
      self.uploadLogFiles()
    except Exception,x:
      self.log.error("Exception while log files uploading:")
      self.log.error(str(x))

    all_done = True
    self.log.info("debug error %s" % (str(error)))
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

      outputs = self.outputData
      outputs_done = []
      all_done = False
      count = 0
      max_attempts = 10
      while not all_done and count < max_attempts :
        if count > 0:
          self.log.info("Output data upload retry number "+str(count))
        all_done = True
        if not outputs in outputs_done:
          resUpload = self.uploadOutputData(outputs,self.appType,'1',self.outputDataSE)
          if resUpload['OK'] == True:
            outputs_done.append(outputs)
          else:
            all_done = False
        count += 1

      #########################################################################
      # At least some files failed uploading to the destination,
      # still trying to save the work

      if not all_done:
        ok = True
        if self.infodict.has_key('TmpCacheSE'):
          cache_se = self.infodict['TmpCacheSE']
          for output,otype,oversion,outputse in outputs:
            if not output in outputs_done:
              fname = os.path.basename(output)
              resCopy = self.rm.copy(fname,cache_se)
              if resCopy['Status'] == 'OK':
                pfname = result['PFN']
                self.log.info("Setting delayed transfer request for %s %s %s %s " % (fname, lfn, pfname, cache_se))
                self.setTransferRequest(lfn,pfname,size,cache_se,guid)
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
          self.log.error(resBK['Value'])

    #########################################################################
    #  If the Transfer request is not empty, send it for later retry

    if not self.transferRequest.isEmpty():
      self.transferRequest.dump()
      ownerDN = getCurrentDN()
      self.transferRequest.setOwnerDN(ownerDN)
      reqfilename = 'transfer_'+self.prod_id+'_'+self.job_id+'.xml'
      self.transferRequest.toFile(reqfilename)

      self.log.info("Setting transfer request for later execution")
      resReq = self.rdbClient.setRequest('transfer',reqfilename)
      if resReq['Status'] == "OK":
        vobox = resReq['Server']
        self.module.step.job.setJobParameter('Transfer VO-box',vobox,0)
        self.log.info("Transfer request is set on VO-box: "+vobox)
      else:
        self.log.error('Failed to set Transfer Request')

      if not error:
        self.module.step.job.report('Waiting for data transfer')
      self.module.step.job.setJobParameter('DataStatus',
                                           self.transferRequest.dumpShortToString(),0)
      if not all_done:
        self.module.step.job.report('Job failed to safe its output')
    else:
      if not all_done:
        self.module.step.job.report('Job failed to safe its output')
      elif not error:
        self.module.step.job.report('Job finished successfully')

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

    #####################################################
    #
    #  TODO: merge bookkeeping requests in just one
    #
    #####################################################


    bad_counter = 0
    for f in books:
      self.log.info( "Sending bookkeeping information %s" % str( f ) )
      fm = f.replace('bookkeeping_','')
      reqfile = open(f,'r')
      xmlstring = reqfile.read()
      result = self.bk.sendBookkeeping(fm,xmlstring)
      if result['OK'] != True:
        self.log.error( "Failed to send bookkeeping information for %s" % str( f ) )
        self.log.error( "Setting bookkeeping request for later retry" )
#        result = self.rdbClient.setRequest('bookkeeping',f)
#        self.info.log(result)
#        if result['OK'] != True:
#          self.log.error( "Setting bookkeeping request failed !" )
#          bad_counter += 1
#        else:
#          self.log.info("Bookkeeping request set on "+result["Server"]+" server")
      else:
        self.log.info( "Bookkeeping information sent successfully" )

      if bad_counter:
        result = S_ERROR("Failed to send %d bookkeeping reports" % bad_counter)

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
    if fcname:
      self.log.info("The following Pool catalog slices will be used:")
      self.log.info(str(fcname))
      self.poolcat = PoolXMLCatalog(fcname)

################################################################################
  def setTransferRequest(self,lfn,pfname,size,se,guid,operation='CopyAndRegister',move=False):

    lfn_directory = os.path.dirname(lfn)
    rmsource = 'no'
    if move:
      rmsource = 'yes'

    self.transferRequest.addTransfer({"LFN":          lfn,
                                      'PFN':          pfname,
                                      'Size':         size,
                                      'TargetSE':     se,
                                      'TargetPath':   lfn_directory,
                                      'Type':         'File',
                                      'RemoveSource': rmsource,
                                      'GUID':         guid})

################################################################################
  def setReplicationRequest(self,lfn,se,source_se,guid,move=False):

    lfn_directory = os.path.dirname(lfn)
    rmsource = 'no'
    if move:
      rmsource = 'yes'

    self.transferRequest.addTransfer({"LFN":          lfn,
                                      'TargetSE':     se,
                                      'TargetPath':   lfn_directory,
                                      'SourceSE':     source_se,
                                      'GUID':         guid,
                                      'Type':         'File',
                                      'RemoveSource': rmsource,
                                      'Operation':    'ReplicateAndRegister'})

################################################################################
  def setTransferLogRequest(self,se):

    target_path = makeProductionPath(self.job_mode,self.jobparameters,self.prod_id,log=True)

    self.transferRequest.addTransfer({'PFN':          self.logdir,
                                      'Type':         'Directory',
                                      'TargetPath':   target_path,
                                      'RegisterFlag': 'no',
                                      'TargetSE':     se})

################################################################################
  def setRegisterRequest(self,lfn,pfname,size,se,guid,catalog):

    self.transferRequest.addRegister({"LFN":         lfn,
                                      'PFN':         pfname,
                                      'Size':        size,
                                      'TargetSE':    se,
                                      'GUID':        guid,
                                      'Catalog':     catalog})

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

    # Ugly !!!  - distinguish log files by their extensions
    logexts = ['.txt','.hbook','.log','.root','.output','.xml','.sh']

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

    dataTypes = ['SIM','DIGI','DST','RAW','ETC','SETC','FETC','RDST','MDF']
#    self.inputData = "LFN:/lhcb/data/CCRC08/RAW/LHCb/CCRC/402154/402154_0000047096.raw;LFN:/lhcb/data/CCRC08/RAW/LHCb/CCRC/402154/402154_0000047097.raw"

    for inputname in self.inputData.split(';'):
      self.LFN_ROOT = ''
      lfnroot = inputname.split('/')
      if len(lfnroot) > 1:
          CONTINUE = 1
          j = 1
          while CONTINUE == 1:
            if not lfnroot[j] in dataTypes:
              self.LFN_ROOT = self.LFN_ROOT+'/'+lfnroot[j]
            else:
              CONTINUE = 0
              break
            j = j + 1
            if j > len(lfnroot):
              CONTINUE = 0
              break

    cwd = os.getcwd()
    os.chdir(self.logdir)
    jobfile = open('job.info','w')
    jobfile.write(self.PRODUCTION_ID+'_'+self.JOB_ID+'\n')
    log_target_path = makeProductionLfn(self,(inputname,'LOG','1'),self.mode,self.PRODUCTION_ID)
    jobfile.write(log_target_path+'\n')
    jobfile.close()
    os.chdir(cwd)

    self.log.info( "\nMoving log files to the log directory: \n<-- %s -->" % self.logdir )
    for f in files:
      ext = os.path.splitext(f)[1]
      if ext in logexts:
        self.log.info( 'Saving log file: %s' % str( f ) )
        self.saveLogFile(f)

    try:
      logse = gConfig.getOptions('/Resources/StorageElements/LogSE')
    except:
      logse = None

    if logse:
      self.log.info("Transfering log files to LogSE")
      target_path = makeProductionLfn(self,('jojo','LOG','1'),self.mode,self.PRODUCTION_ID)
#      target_path = '/joel/'
      self.log.info(self.logdir)
      self.log.info(logse)
      self.log.info(target_path)
      result = self.rm.putDirectory(target_path,self.logdir,'LogSE')
      self.log.info(result)

      if result['OK'] == True:
        # Construct the http reference to the Log directory
        logref = '<a href="http://lhcb-logs.cern.ch/storage%s/%s">Log file directory</a>' % \
                 (target_path,str(self.JOB_ID))
        self.LogFileDirectory = logref
      self.LogFileLFNPath = target_path

      if result['OK'] != True:
        self.log.error("Transfering log files to the main LogSE failed")
        self.log.error( result['Message'] )

        ################################################################
        #
        #  Commented out as nobody will be looking for the log files there
        #
        # Copying log files to the main LogSE failed
        # Try one of the failover SE's
        #logses = cfgSvc.get(self.mode,'FailoverLogSE',[])
        #OK = False
        #for se in logses:
        #  self.log.info("Transfering log files to "+se)
        #  result = self.rm.copyDir(self.logdir,se,target_path)
        #  if result['Status'] != 'OK':
        #    self.log.error("Transfering log files to "+se+" failed")
        #  else:
        #    self.log.info("Transfering log files to "+se+" successful")
        #    OK = True
        #    break
        #
        #if not OK:

        ##################################################################
        # Put log files in a tar archive and store in one of the data
        # failover SE's

      else:
        self.log.info("Transfering log files to the main LogSE successful")

################################################################################
  def uploadOutputData(self,output,otype,oversion,outputse):
    """ Determine the output file destination SEs and upload file to the grid
    """

    ########################################################
    #  Get all the output SE's for this file

#    if self.infodict.has_key(otype):
#      ses = self.infodict[otype]
#      if (ses == ""):
#         ses = []
#      else:
#         ses = ses.split(',')
#    else:
#      ses = []

    ses = []
    # Add output SE defined in the job description
    if outputse:
      outses = outputse.split(',')
      for outse in outses:
        if not outse in ses and outse != "None" :
          ses.append(outse)

    # Attempt to store first file to the LocalSE if it is in the list of
    # requested SEs

    if "LocalSE" in ses:
      ses.insert(0,"LocalSE")

    if len(ses) == 0:
      # Processing for this output type/file is not requested
      return S_OK()

    self.log.info( "Processing output %s %s %s" % ( str( output ), str( otype ), str( oversion ) ) )

    # Check the validity of SEs
    ses_trial = uniq(ses)
    self.log.info(ses_trial)
    ses = []
    for se in ses_trial:
      sname = self.getSEName(se)
      if not sname:
        self.log.error("Storage Element "+se+" is not defined")
      else:
        ses.append(sname)

    ses = uniq(ses)

    if len(ses) > 0:
      self.log.info("File %s will be stored to the following SE's:\n%s" % (output, str(ses)))

      lfn = makeProductionLfn(self,(output,otype),self.mode,self.PRODUCTION_ID)
      result = self.uploadDataFile(output,lfn,ses)
      return result
    else:
      self.log.error('No valid SEs defined as file destinations')
      return S_ERROR('No valid SEs defined as file destinations')


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
      if result['OK'] == True:
        if self.isGridSE(se):
          one_grid_upload_successful = True
          source_se = result['SEName']
          break
      elif result['Level'] == 'Registration':
        if self.isGridSE(se):
          one_grid_copy_successful = True
          source_pfn = result['PFN']
          source_se = result['SEName']
          break

    if one_grid_upload_successful:
      # File is uploaded in a fully grid compliant manner,
      # set replication requests for other SEs
      for se in destination_ses:
        if se != source_se:
          self.setReplicationRequest(lfn,se,source_se,guid)
      return S_OK('Full')

    elif one_grid_copy_successful:
      # File is uploaded to a grid storage but not properly registered,
      # set transfer requests for other SEs
      for se in destination_ses:
        if se != source_se:
          self.setTransferRequest(lfn,source_pfn,size,se,guid)
      return S_OK('Registration')

    ###########################################################################
    #
    #  File upload to the main destinations failed, try failover SE's

#    failoverSEList = cfgSvc.get(self.mode,'FailoverDataSE',[])
    failoverSEList = 'CERN-Failover'

    # Reshuffle the list of the failover SEs
#    random.shuffle(failoverSEList)
    tier1SEName = self.getSEName('Tier1SE')
    localSEName = self.getSEName('LocalSE')

    failover_ses = []
    if localSEName:
      if self.isGridSE(localSEName):
        failover_ses.append(localSEName)
      else:
        # In case the transfer/replication request will be defined locally
        # non-grid local SE is a possible storage
        if 'local' == self.rdbClient.rdb_urls[0]:
          failover_ses.append(localSEName)
    if tier1SEName in failoverSEList:
      failover_ses.append(tier1SEName)

    for se in failoverSEList:
      sename = self.getSEName(se)
      if sename:
        if sename != localSEName and sename != tier1SEName \
           and not sename in [ self.getSEName(x) for x in destinationSEList ]:
          failover_ses.append(sename)

    self.log.info("Trying failover destinations "+str(failover_ses))

    for se in failover_ses:
      result = self.uploadDataFileToSE(datafile,lfn,se,guid)
      if result['OK'] == True:
        one_grid_upload_successful = True
        source_se = result['SEName']
        break
      elif result['Level'] == 'Registration':
        one_grid_copy_successful = True
        source_se = result['SEName']
        source_pfn = result['PFN']
        break

    if one_grid_upload_successful:
      # File is uploaded in a fully grid compliant manner,
      # set data move requests for destination SEs
      for se in destination_ses:
        if source_se != se:
          self.setReplicationRequest(lfn,se,source_se,guid,move=True)
      return S_OK('Full')

    elif one_grid_copy_successful:
      # File is uploaded to a grid storage but not properly registered,
      # set transfer requests for other SEs
      for se in destination_ses:
        if source_se != se:
          self.setTransferRequest(lfn,source_pfn,size,se,guid,move=True)
      return S_OK('Registration')

    ################################################################################
    # All attempts to upload file to the grid failed, alas
    return S_ERROR('Fatal errors in uploading file to the Grid')


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
    resultCaR = self.rm.putAndRegister(lfn,fname,se,guid,
                                        lfn_directory)

    if resultCaR['OK'] != True:
      self.log.error("rm.putAndRegister failed for %s to %s" % (fname,se))
      self.log.error(resultCaR['Message'])
      if resultCaR.has_key('RegisterLog'):
        # Transfer done but registration failed
        LFC_OK = False
        for catalog,status in resultCaR['RegisterLog']:
          if status != 'OK':
            self.setRegisterRequest(lfn,resultCaR['PFN'],size,se,guid,catalog)
          else:
            if catalog == "LFC":
              LFC_OK = True

        result = resultCaR
        if LFC_OK:
          result['Status'] = "OK"
        else:
          self.log.error( "Registration failed for %s" % fname )

        if not re.search("Failed to copy file",result['Message']):
          if result.has_key('PFN'):
            pfname = result['PFN']
        result['Level'] = "Registration"
      else:
        result = S_ERROR('Fatal error while uploading %s to %s' % (fname,se))
        result['Level'] = 'Fatal'
    else:
      result = resultCaR

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
