########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/WorkflowLib/Module/NewGaudiApplication.py,v 1.1 2008/04/25 08:59:12 rgracian Exp $
# File :   NewGaudiApplication
# Author : Ricardo Graciani
########################################################################
__RCSID__   = "$Id: NewGaudiApplication.py,v 1.1 2008/04/25 08:59:12 rgracian Exp $"
__VERSION__ = "$Revision: 1.1 $"
""" Gaudi Application Class """

from DIRAC.Core.Utilities                                import systemCall
from DIRAC.Core.Utilities                                import shellCall
from DIRAC.Core.Utilities                                import ldLibraryPath
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig, platformTuple

import shutil, re, string, os, sys

class GaudiApplication(object):

  #############################################################################
  def __init__(self):
    self.enable = True
    self.version = __RCSID__
    self.debug = True
    self.log = gLogger.getSubLogger( "GaudiApplication" )
    self.appLog = None
    self.appOutputData = 'NoOutputName'
    self.appInputData = 'NoInputName'
    self.inputDataType = 'MDF'
    self.result = S_ERROR()
    self.logfile = None
    self.NUMBER_OF_EVENTS = None
    self.inputData = '' # to be resolved
    self.InputData = '' # from the (JDL WMS approach)
    self.outputData = None
    self.poolXMLCatName = 'pool_xml_catalog.xml'
    self.generator_name=''
    self.optfile_extra = ''
    self.optfile = ''
    self.jobReport  = RPCClient('WorkloadManagement/JobStateUpdate')
    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']

  #############################################################################
  def resolveInputDataOpts(self,options):
    """ Input data resolution has two cases. Either there is explicitly defined
        input data for the application step (a subset of total workflow input data reqt)
        *or* this is defined at the job level and the job wrapper has created a
        pool_xml_catalog.xml slice for all requested files.

        Could be overloaded for python / standard options in the future.
    """
    if self.InputData:
      self.log.info('Input data defined taken from JDL parameter')
      if type(self.inputData) != type([]):
        self.inputData = self.InputData.split(';')
    elif self.inputData:
      self.log.info('Input data defined in workflow for this Gaudi Application step')
      if type(self.inputData) != type([]):
        self.inputData = self.inputData.split(';')
    elif os.path.exists(self.poolXMLCatName):
      self.log.info('Determining input data from Pool XML slice')
      inputDataCatalog = PoolXMLCatalog(self.poolXMLCatName)
      inputDataList = inputDataCatalog.getLfnsList()
      self.inputData = inputDataList
    else:
      self.log.verbose('Job has no input data requirement')

    if self.inputData:
      #write opts
      inputDataFiles = []
      for lfn in self.inputData:
        if self.inputDataType == "MDF":
          inputDataFiles.append(""" "DATAFILE='%s' SVC='LHCb::MDFSelector'", """ %(lfn))
        elif self.inputDataType == "ETC":
          inputDataFiles.append(""" "COLLECTION='TagCreator/1' DATAFILE='%s' TYPE='POOL_ROOTTREE' SEL='(GlobalOr>=1)' OPT='READ'", """%(lfn))
        else:
          inputDataFiles.append(""" "DATAFILE='%s' TYP='POOL_ROOTTREE' OPT='READ'", """ %(lfn))
      inputDataOpt = string.join(inputDataFiles,'\n')[:-2]
      evtSelOpt = """EventSelector.Input={%s};\n""" %(inputDataOpt)
      options.write(evtSelOpt)

    poolOpt = """\nPoolDbCacheSvc.Catalog= {"xmlcatalog_file:%s"};\n""" %(self.poolXMLCatName)
    options.write(poolOpt)
#    poolOpt = """\nFileCatalog.Catalogs= {"xmlcatalog_file:%s"};\n""" %(self.poolXMLCatName)
#    options.write(poolOpt)

  #############################################################################
  def resolveInputDataPy(self,options):
    """ Input data resolution has two cases. Either there is explicitly defined
        input data for the application step (a subset of total workflow input data reqt)
        *or* this is defined at the job level and the job wrapper has created a
        pool_xml_catalog.xml slice for all requested files.

        Could be overloaded for python / standard options in the future.
    """
    if self.InputData:
      self.log.info('Input data defined taken from JDL parameter')
      if type(self.inputData) != type([]):
        self.inputData = self.InputData.split(';')
    elif self.inputData:
      self.log.info('Input data defined in workflow for this Gaudi Application step')
      if type(self.inputData) != type([]):
        self.inputData = self.inputData.split(';')
    elif os.path.exists(self.poolXMLCatName):
      inputDataCatalog = PoolXMLCatalog(self.poolXMLCatName)
      inputDataList = inputDataCatalog.getLfnsList()
      self.inputData = inputDataList
    else:
      self.log.verbose('Job has no input data requirement')

    if self.inputData:
      #write opts
      inputDataFiles = []
      for lfn in self.inputData:
        if self.inputDataType == "MDF":
          inputDataFiles.append(""" "DATAFILE='%s' SVC='LHCb::MDFSelector'", """ %(lfn))
        elif self.inputDataType == "ETC":
          inputDataFiles.append(""" "COLLECTION='TagCreator/1' DATAFILE='%s' TYPE='POOL_ROOTTREE' SEL='(GlobalOr>=1)' OPT='READ'", """%(lfn))
        else:
          inputDataFiles.append(""" "DATAFILE='%s' TYP='POOL_ROOTTREE' OPT='READ'", """ %(lfn))
      inputDataOpt = string.join(inputDataFiles,'\n')[:-2]
      evtSelOpt = """EventSelector().Input=[%s];\n""" %(inputDataOpt)
      options.write(evtSelOpt)
#    if self.outputData != None:
#      for opt in self.outputData.split(';'):
#        options.write("""OutputStream("DstWriter").Output = "DATAFILE='PFN:'+opt+' TYP='POOL_ROOTTREE' OPT='RECREATE'""")

    poolOpt = """\nPoolDbCacheSvc().Catalog= ["xmlcatalog_file:%s"]\n""" %(self.poolXMLCatName)
    options.write(poolOpt)
#    poolOpt = """\nFileCatalog().Catalogs= ["xmlcatalog_file:%s"]\n""" %(self.poolXMLCatName)
#    options.write(poolOpt)

  #############################################################################
  def manageOpts(self):
    print "manage options OPTS",self.optfile
    options = open('gaudi.opts','w')
    options.write('\n\n//////////////////////////////////////////////////////\n')
    options.write('// Dynamically generated options in a production or analysis job\n\n')
    if os.path.exists('gaudirun.opts'): os.remove('gaudirun.opts')
    if os.path.exists('gaudiruntmp.opts'): os.remove('gaudiruntmp.opts')
    if re.search('\$',self.optfile) is None:
      comm = 'cat '+self.optfile+' > gaudiruntmp.opts'
      output = shellCall(0,comm)
    else:
      comm = 'echo "#include '+self.optfile+'" > gaudiruntmp.opts'
      commtmp = open('gaudiruntmp.opts','w')
      newline = '#include "'+self.optfile+'"'
      commtmp.write(newline)
      commtmp.close()

    self.optfile = 'gaudiruntmp.opts'
    for opt in self.optionsLine.split(';'):
      if not re.search('tring',opt):
        if re.search('#include',opt):
          options.write(opt+'\n')
        else:
          options.write(opt+';\n')
      else:
        self.log.warn('Options line not in correct format ignoring string')

    self.resolveInputDataOpts(options)
    if self.NUMBER_OF_EVENTS != None:
        options.write("""ApplicationMgr.EvtMax ="""+self.NUMBER_OF_EVENTS+""" ;\n""")
#    for opt in self.outputData.split(';'):
#      options.write("""DigiWriter.Output = "DATAFILE='PFN:"""+opt+"""' TYP='POOL_ROOTTREE' OPT='RECREATE'";\n""")
    options.write('\n//EOF\n')
    options.close()

    comm = 'cat '+self.optfile+' gaudi.opts > gaudirun.opts'
    output = shellCall(0,comm)
    self.optfile = 'gaudirun.opts'
    os.environ['JOBOPTPATH'] = 'gaudirun.opts'

  #############################################################################
  def managePy(self):
    if os.path.exists(self.optfile_extra): os.remove(self.optfile_extra)

    try:
        self.log.info("Adding extra options : %s" % (self.optionsLine))
        options = open(self.optfile_extra,'a')
        options.write('\n\n#//////////////////////////////////////////////////////\n')
        options.write('# Dynamically generated options in a production or analysis job\n\n')
        options.write('from Gaudi.Configuration import *\n')
        for opt in self.optionsLine.split(';'):
            options.write(opt+'\n')
            self.resolveInputDataPy(options)
            if self.NUMBER_OF_EVENTS != None:
               options.write("""ApplicationMgr().EvtMax ="""+self.NUMBER_OF_EVENTS+""" ;\n""")
        options.close()
    except Exception, x:
        print "No additional options"

  #############################################################################
  def execute(self):
    self.__report('Initializing GaudiApplication')
    self.root = gConfig.getValue('/LocalSite/Root',os.getcwd())

    cwd = os.getcwd()
    self.log.debug(self.version)
    self.log.info( "Executing application %s %s" % ( self.appName, self.appVersion ) )
    self.log.info("Platform for job is %s" % ( self.systemConfig ) )
    self.log.info("Root directory for job is %s" % ( self.root ) )
    localDir = 'lib' #default

    # Check if the specified options file exists in the current directory,
    # for example, it is supplied in the job input sandbox
    try:
        exec 'from LHCb_config import *'
    except Exception, x:
        self.log.error("failed to import LHCb_config.py : %s" % (x))
        self.__report('failed to import LHCb_config.py')
        self.result = S_ERROR('failed to import LHCb_config.py')
        return self.result

    if applications.has_key(string.upper(self.appName)) == 1:
        prefix = applications[string.upper(self.appName)]
    else:
        self.log.error("Application unknown :%s" % (self.appName))


    #will think about this later
    #############################################################

    app_dir_path = self.root+'/lib/lhcb/'+string.upper(self.appName)+'/'+ \
                   string.upper(self.appName)+'_'+self.appVersion+'/'+prefix+'/' \
                   +self.appVersion
    app_dir_path_install = self.root+'/lib/lhcb/'+string.upper(self.appName)+'/'+ \
                   string.upper(self.appName)+'_'+self.appVersion+'/InstallArea'

    mysiteroot = self.root
    if os.path.exists('%s/%s' %(cwd,self.optionsFile)):
      self.optfile = self.optionsFile
    # Otherwise take the one from the application options directory
    else:
      optpath = app_dir_path+'/options'
      if os.path.exists(optpath+'/'+self.optionsFile):
        self.optfile = optpath+'/'+self.optionsFile
      else:
        self.optfile = self.optionsFile

    if self.optionsFile.find('.opts') > 0:
      self.optfile_extra = './gaudi_extra_options.opts'
      optionsType = 'opts'
      self.manageOpts()
    else:
      optionsType = 'py'
      self.optfile_extra = './gaudi_extra_options.py'
      self.managePy()

#    comm = open(optfile,'a')
#    newline = """OutputStream("DstWriter").Output = "DATAFILE='PFN:joel.dst' TYPE='POOL_ROOTTREE' OPT='REC'"\n """
#    comm.write(newline)
#    comm.close()


#    os.environ['JOBOPTPATH'] = optfile

    cmtEnv = dict(os.environ)
    gaudiEnv = {}

    mySiteRoot = os.path.join( self.root, localDir )
    cmtEnv['MYSITEROOT'] = mySiteRoot
    cmtEnv['CMTCONFIG']  = self.systemConfig

    extCMT = os.path.join( mySiteRoot, 'scripts', 'ExtCMT' )
    if platformTuple[0] == 'Windows':
      extCMT += '.bat'
    else:
      extCMT += '.sh'

    if not os.path.exists( extCMT ):
      self.log.warn( 'Missing ExtCMT script: %s' % extCMT )
      self.result = S_ERROR( 'Missing ExtCMT script: %s' % extCMT )
      return self.result

    # gaudiEnv['JOBOPTPATH'] = 'gaudirun.opts'

    setupProject = os.path.join( mySiteRoot, 'scripts', 'SetupProject' )
    if platformTuple[0] == 'Windows':
      setupProject += '.bat'
    else:
      setupProject += '.sh'
      extCMT = '. %s' % extCMT

    setupProject = [setupProject]
    setupProject.append( '--ignore-missing' )
    if self.generator_name:
      setupProject.append( '--tag_add=%s' % self.generator_name )
    setupProject.append( self.appName )
    setupProject.append( self.appVersion )
    setupProject.append( 'gfal CASTOR dcache_client lfc oracle' )

    setupProject = '. '+' '.join(setupProject)

    envAsDict = '; python -c "import os,sys ; print >> sys.stderr, os.environ"'

    # Run ExtCMT
    timeout = 300
    if platformTuple[0] == 'Windows':
      # this needs to be tested
      ret = shellCall( timeout, [ extCMT + envAsDict ], env=cmtEnv ) 
    else:
      ret = systemCall( timeout, [ '/bin/bash', '-c', extCMT + envAsDict ], env = cmtEnv )

    if ret['OK'] and ret['Value'][0] == 0:
      self.log.info( ret['Value'][1] )
      try:
        setupProjectEnv = eval( ret['Value'][2] )
      except:
        self.result = S_ERROR()
        return self.result

    # Run SetupProject
    if platformTuple[0] == 'Windows':
      # this needs to be tested
      ret = shellCall( timeout, [ setupProject + envAsDict ], env=setupProjectEnv ) 
    else:
      ret = systemCall( timeout, [ '/bin/bash', '-c', setupProject + envAsDict ], env = setupProjectEnv )

    if ret['OK'] and ret['Value'][0] == 0:
      self.log.info( ret['Value'][1] )
      try:
        gaudiEnv = eval( ret['Value'][2] )
      except:
        self.result = S_ERROR()
        return self.result

    for k in gaudiEnv:
      print k, '=', gaudiEnv[k]

    appDir = os.path.join(os.getcwd(),'%s_%s' % ( self.appName, self.appVersion ))
    if os.path.exists( appDir ):
      import shutil
      shutil.rmtree( appDir )
    # add shipped libraries if available
    extraLibs = os.path.join( mySiteRoot, self.systemConfig )
    if os.path.exists( extraLibs ):
      gaudiEnv['LD_LIBRARY_PATH'] += ':%s' % extraLibs
    gaudiEnv['LD_LIBRARY_PATH'] = ldLibraryPath.unify( gaudiEnv['LD_LIBRARY_PATH'], appDir )

    # 
    gaudiRoot = gaudiEnv['GAUDIALGROOT']
    gaudiVer = os.path.basename( gaudiRoot )
    while gaudiVer.find('GAUDI_v') != 0:
      gaudiRoot = os.path.dirname(gaudiRoot)
      gaudiVer  = os.path.basename( gaudiRoot )
      if gaudiVer == '':
        self.result = S_ERROR( 'Can not determine Gaudi Version' )
        return self.result
    gaudiMajor = int(gaudiVer.split('_v')[1].split('r')[0])
    gaudiMinor = int(gaudiVer.split('_v')[1].split('r')[1])

    if gaudiMajor > 19 or ( gaudiMajor == 19 and gaudiMinor > 6 ) :
      self.log.INFO( 'Gaudi Version %s' % gaudiVer )
      self.log.INFO( 'Replace  PoolDbCacheSvc.Catalog by FileCatalog.Catalogs in options file.' )
      f1 = open( self.optionsFile, 'r' )
      f2 = open( self.optionsFile+'.new', 'w')
      lines = f1.readlines()
      for line in lines:
        f2.write( line.replace('PoolDbCacheSvc.Catalog','FileCatalog.Catalogs'))
      f1.close()
      f2.close()
      os.file.rename( self.optionsFile, self.optionsFile+'.old' )
      os.file.rename( self.optionsFile+'.new', self.optionsFile )

    f = open( 'localEnv.log', 'w' )
    for k in gaudiEnv:
      v = gaudiEnv[k]
      f.write( '%s=%s\n' % ( k,v ) )
    f.close()

    if optionsType == 'py':
      gaudiCmd = ['gaudirun.py']
      gaudiCmd.append(self.optfile)
      gaudiCmd.append(self.optfile_extra)
    else:
      # Default
      gaudiCmd = [ os.path.join( app_dir_path_install, self.systemConfig, 'bin', self.appName+'.exe' )]
      if os.path.exists( os.path.join('lib',self.appName+'.exe')):
        gaudiCmd = [ os.path.join('lib',self.appName+'.exe' )]
        self.log.INFO( 'Found User shipped executable %s' % self.appName+'.exe' )
      gaudiCmd.append( self.optfile )

    if self.appLog == None:
      self.appLog = self.logfile
    if os.path.exists(self.appLog): os.remove(self.appLog)
    self.__report('%s %s' %(self.appName,self.appVersion))
    ret = systemCall(0,gaudiCmd,env=gaudiEnv,callbackFunction=self.redirectLogOutput)

    if not ret['OK']:
      self.result = ret
      return self.result

    resultTuple = ret['Value']

    status = resultTuple[0]
    stdOutput = resultTuple[1]
    stdError = resultTuple[2]

    self.log.info( "Status after the application execution is %s" % str( status ) )

    # logfile = open(self.appLog,'w')
    # logfile.write(stdOutput)

    # if len(stdError) > 0:
    #   logfile.write('\n\n\nError log:\n=============\n')
    #   logfile.write(stdError)
    # logfile.close()

    failed = False
    if status > 0:
      self.log.error( "%s execution completed with errors:" % self.appName )
      failed = True
    elif len(stdError) > 0:
      self.log.error( "%s execution completed with application Warning:" % self.appName )
    else:
      self.log.info( "%s execution completed succesfully:" % self.appName )

    if failed==True:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( stdError )
      self.__report('%s Exited With Status %s' %(self.appName,status))
      self.result = S_ERROR(self.appName+" execution completed with errors")
      return self.result

    # Return OK assuming that subsequent CheckLogFile will spot problems
    self.__report('%s %s Successful' %(self.appName,self.appVersion))
    self.result = S_OK()
    return self.result

  #############################################################################
  def redirectLogOutput(self, fd, message):
    print message
    sys.stdout.flush()
    if message:
      if self.appLog:
        log = open(self.appLog,'a')
        log.write(message+'\n')
        log.close()
      else:
        self.log.error("Application Log file not defined")

  #############################################################################
  def __report(self,status):
    """Wraps around setJobApplicationStatus of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    self.log.verbose('setJobApplicationStatus(%s,%s,%s)' %(self.jobID,status,'GaudiApplication'))
    jobStatus = self.jobReport.setJobApplicationStatus(int(self.jobID),status,'GaudiApplication')
    if not jobStatus['OK']:
      self.log.warn(jobStatus['Message'])

    return jobStatus

