########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/WorkflowLib/Module/NewGaudiApplication.py,v 1.23 2008/08/05 13:57:09 rgracian Exp $
# File :   NewGaudiApplication.py
# Author : Ricardo Graciani
########################################################################
__RCSID__   = "$Id: NewGaudiApplication.py,v 1.23 2008/08/05 13:57:09 rgracian Exp $"
__VERSION__ = "$Revision: 1.23 $"
""" Gaudi Application Class """

from DIRAC.Core.Utilities                                import systemCall
from DIRAC.Core.Utilities                                import shellCall
from DIRAC.Core.Utilities                                import ldLibraryPath
from DIRAC.Core.Utilities                                import Source
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig, platformTuple

try:
  from DIRAC.LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea, LocalArea, CheckApplication
except Exception,x:
  from LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea, LocalArea, CheckApplication

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
    self.jobID = None
    # FIXME: the JobID can be taken from the Job Dictionary by the LHCbJob module
    # like Application Name/Version, Options file, InputData,...
    # we do not need an evieonmental variable
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']
    # FIXME: Other parameters fixed by the calling module
    self.appName = ''
    self.appVersion = ''
    self.optionsFile = ''
    self.optionsLine = ''
    # FIXME: We should find a better name 
    self.systemConfig = ''
    self.logfile = ''

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

    options = open('gaudirun.opts','w')
    options.write('#include "%s"  ' % self.optfile)
    options.write('\n\n//////////////////////////////////////////////////////\n')
    options.write('// Dynamically generated options in a production or analysis job\n\n')

    self.log.info("Adding extra options : %s" % (self.optionsLine))
    for opt in self.optionsLine.split(';'):
      # FIXME: I do not understand what this magic does, why "tring" is searched?
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

    self.optfile = 'gaudirun.opts'

  #############################################################################
  def managePy(self):

    self.log.info("Adding extra options : %s" % (self.optionsLine))
    options = open(self.optfile_extra,'w')
    options.write('\n\n#//////////////////////////////////////////////////////\n')
    options.write('# Dynamically generated options in a production or analysis job\n\n')
    options.write('from Gaudi.Configuration import *\n')
    for opt in self.optionsLine.split(';'):
        options.write(opt+'\n')
    if self.NUMBER_OF_EVENTS != None:
       options.write("""ApplicationMgr().EvtMax ="""+self.NUMBER_OF_EVENTS+""" ;\n""")
    options.write('\n# EOF\n')
    options.close()

  #############################################################################
  def execute(self):
    
    self.result = S_OK()
    if not self.appName or not self.appVersion:
      self.resul = S_ERROR( 'No Gaudi Application defined' )
    elif not self.systemConfig:
      self.result = S_ERROR( 'No LHCb platform selected' )
    # FIXME: clarify if appLog or logfile is to be used
    elif not self.logfile and not self.appLog:
      self.result = S_ERROR( 'No Log file provided' )
    
    if not self.result['OK']:
      return self.result
    
    if not self.optionsFile and not self.optionsLine:
      self.log.warn( 'No options File nor options Line provided' )
    
    self.__report( 'Initializing GaudiApplication' )

    self.cwd  = os.getcwd()
    self.root = gConfig.getValue( '/LocalSite/Root', self.cwd )

    self.log.debug( self.version )
    self.log.info( "Executing application %s %s" % ( self.appName, self.appVersion ) )
    self.log.info( "Platform for job is %s" % ( self.systemConfig ) )
    self.log.info( "Root directory for job is %s" % ( self.root ) )

    sharedArea = SharedArea()
    localArea  = LocalArea()

    # 1. Check if Application is available in Shared Area
    appRoot = CheckApplication( ( self.appName, self.appVersion ), self.systemConfig, sharedArea )
    if appRoot:
      mySiteRoot = sharedArea
    else:
      # 2. If not, check if available in Local Area
      appRoot = CheckApplication( ( self.appName, self.appVersion ), self.systemConfig, localArea )
      if appRoot:
        mySiteRoot = localArea
      else:
        self.log.warn( 'Application not Found' )
        self.__report( 'Appliaction not Found' )
        self.result = S_ERROR( 'Application not Found' )

    if not self.result['OK']:
      return self.result
      
    self.__report( 'Application Found' )
    self.log.info( 'Application Root Found:', appRoot )

    # 3. Check Named options file in options directory of the Application 
    self.optfile = os.path.join( appRoot, 'options', self.optionsFile )
    self.log.info( 'Default options file:', self.optfile )
    # if there is a $ character in self.optionsFile the lower will never match
    # looking for a user supplied one
    if os.path.exists( self.optionsFile ):
      self.optfile = self.optionsFile
      self.log.info( 'Using user supplied options file:', self.optfile )

    # 4. Check user provided options file and Manage Options
    # it is done here so that it can be used of the environment
    if self.optionsFile.find('.opts') > 0:
      optionsType = 'opts'
      self.manageOpts()
    elif self.optionsFile.find('.py') > 0:
      optionsType = 'py'
      self.optfile_extra = './gaudi_extra_options.py'
      self.managePy()
    else:
      pass

# This is not necessary since it is passed as argument
#    os.environ['JOBOPTPATH'] = 'gaudirun.opts'

    cmtEnv = dict(os.environ)
    gaudiEnv = {}

    cmtEnv['MYSITEROOT'] = mySiteRoot
    cmtEnv['CMTCONFIG']  = self.systemConfig

    extCMT       = os.path.join( mySiteRoot, 'scripts', 'ExtCMT' )
    setupProject = os.path.join( mySiteRoot, 'scripts', 'SetupProject' )

    setupProject = [setupProject]
    setupProject.append( '--ignore-missing' )
    if self.generator_name:
      setupProject.append( '--tag_add=%s' % self.generator_name )
    setupProject.append( self.appName )
    setupProject.append( self.appVersion )
    setupProject.append( 'gfal CASTOR dcache_client lfc oracle' )

    timeout = 300

    # Run ExtCMT
    ret = Source( timeout, [extCMT], cmtEnv )
    if ret['OK']:
      if ret['stdout']:
        self.log.info( ret['stdout'] )
      if ret['stderr']:
        self.log.warn( ret['stderr'] )
      setupProjectEnv = ret['outputEnv']
    else:
      self.log.error( ret['Message'])
      if ret['stdout']:
        self.log.info( ret['stdout'] )
      if ret['stderr']:
        self.log.warn( ret['stderr'] )
      self.result = ret
      return self.result

    # Run SetupProject
    ret = Source( timeout, setupProject, setupProjectEnv )
    if ret['OK']:
      if ret['stdout']:
        self.log.info( ret['stdout'] )
      if ret['stderr']:
        self.log.warn( ret['stderr'] )
      gaudiEnv = ret['outputEnv']
    else:
      self.log.error( ret['Message'])
      if ret['stdout']:
        self.log.info( ret['stdout'] )
      if ret['stderr']:
        self.log.warn( ret['stderr'] )
      self.result = ret
      return self.result

    # Now link all libraries in a single directory
    appDir = os.path.join( self.cwd, '%s_%s' % ( self.appName, self.appVersion ))
    if os.path.exists( appDir ):
      import shutil
      shutil.rmtree( appDir )
    # add shipped libraries if available
    # extraLibs = os.path.join( mySiteRoot, self.systemConfig )
    #if os.path.exists( extraLibs ):
    #  gaudiEnv['LD_LIBRARY_PATH'] += ':%s' % extraLibs
    #  self.log.info( 'Adding %s to LD_LIBRARY_PATH' % extraLibs )
    # Add compat libs
    compatLib = os.path.join( self.root, self.systemConfig, 'compat' )
    if os.path.exists(compatLib):
      gaudiEnv['LD_LIBRARY_PATH'] += ':%s' % compatLib
    gaudiEnv['LD_LIBRARY_PATH'] = ldLibraryPath.unify( gaudiEnv['LD_LIBRARY_PATH'], appDir )


    # 
    if not gaudiEnv.has_key('GAUDIALGROOT'):
      self.log.warn( 'GAUDIALGROOT not defined' )
      self.log.warn( 'Can not determine Gaudi Root' )
      self.result = S_ERROR( 'Can not determine Gaudi Root' )
      return self.result
    gaudiRoot = gaudiEnv['GAUDIALGROOT']
    gaudiVer = os.path.basename( gaudiRoot )
    while gaudiVer.find('GAUDI_v') != 0:
      gaudiRoot = os.path.dirname(gaudiRoot)
      gaudiVer  = os.path.basename( gaudiRoot )
      if gaudiVer == '':
        self.log.error( 'Can not determine Gaudi Version %s' % gaudiEnv['GAUDIALGROOT'] )
        self.result = S_ERROR( 'Can not determine Gaudi Version' )
        return self.result
    gaudiMajor = int(gaudiVer.split('_v')[1].split('r')[0])
    gaudiMinor = int(gaudiVer.split('_v')[1].split('r')[1])

    if gaudiMajor > 19 or ( gaudiMajor == 19 and gaudiMinor > 6 ) :
      self.log.info( 'Gaudi Version %s' % gaudiVer )
      self.log.info( 'Replace  PoolDbCacheSvc.Catalog by FileCatalog.Catalogs in options file.' )
      # only apply if user optionFile supplied otherwise a proper version should be used.
      # This should be removed in any case. Maybe do a check and report an error
      for optfile in [self.optionsFile, self.optfile]:

        if os.path.exists( optfile ):
          f1 = open( optfile, 'r' )
          f2 = open( optfile+'.new', 'w')
          lines = f1.readlines()
          for line in lines:
            f2.write( line.replace('PoolDbCacheSvc.Catalog','FileCatalog.Catalogs'))
          f1.close()
          f2.close()
          os.rename( optfile, optfile+'.old' )
          os.rename( optfile+'.new', optfile )

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
      gaudiCmd = [self.appName+'.exe']
      if os.path.exists( os.path.join('lib',self.appName+'.exe')):
        gaudiCmd = [ os.path.join('lib',self.appName+'.exe' )]
        self.log.info( 'Found User shipped executable %s' % self.appName+'.exe' )
      gaudiCmd.append( self.optfile )

    if self.appLog == None:
      self.appLog = self.logfile
    if os.path.exists(self.appLog): os.remove(self.appLog)
    self.__report('%s %s' %(self.appName,self.appVersion))
    self.writeGaudiRun(gaudiCmd, gaudiEnv)
    self.log.info( 'Running', ' '.join(gaudiCmd)  )
    ret = systemCall(0,gaudiCmd,env=gaudiEnv,callbackFunction=self.redirectLogOutput)

    if not ret['OK']:
      self.log.error(gaudiCmd)
      self.log.error(ret)
      self.result = S_ERROR()
      return self.result

      self.result = ret
      return self.result

    resultTuple = ret['Value']

    status = resultTuple[0]
    stdOutput = resultTuple[1]
    stdError = resultTuple[2]

    self.log.info( "Status after the application execution is %s" % str( status ) )

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

  def writeGaudiRun( self, gaudiCmd, gaudiEnv, shell='/bin/bash'):
    """
     Write a shell script that can be used to run the application
     with the same environment as in GaudiApplication.
     This script is not used internally, but can be used "a posteriori" for 
     debugging purposes 
    """
    if shell.find('csh'):
      ext = 'csh'
      environ = []
      for var in gaudiEnv:
        environ.append('setenv %s "%s"' % (var, gaudiEnv[var]) )
    else:
      ext = 'sh'
      for var in gaudiEnv:
        environ.append('export %s="%s"' % (var, gaudiEnv[var]) )

    scriptName = '%sRun.%s' % ( self.appName, ext )
    script = open( scriptName, 'w' )
    script.write( """#! %s
#
# This is a debug script to run %s %s interactively
#
%s
#
%s | tee %s
#
exit $?
#
""" % (shell, self.appName, self.appVersion, 
        '\n'.join(environ), 
        ' '.join(gaudiCmd),
        self.appLog ) )

    script.close()

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
    jobReport = RPCClient('WorkloadManagement/JobStateUpdate')
    jobStatus = jobReport.setJobApplicationStatus(int(self.jobID),status,'GaudiApplication')
    if not jobStatus['OK']:
      self.log.warn(jobStatus['Message'])

    return jobStatus

