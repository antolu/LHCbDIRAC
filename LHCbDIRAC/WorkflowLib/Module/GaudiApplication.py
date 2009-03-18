########################################################################
# $Id: GaudiApplication.py,v 1.108 2009/03/18 11:08:10 paterson Exp $
########################################################################
""" Gaudi Application Class """

__RCSID__ = "$Id: GaudiApplication.py,v 1.108 2009/03/18 11:08:10 paterson Exp $"

from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog
from DIRAC.Core.DISET.RPCClient                          import RPCClient
try:
  from DIRAC.LHCbSystem.Utilities.CombinedSoftwareInstallation  import MySiteRoot
except Exception,x:
  from LHCbSystem.Utilities.CombinedSoftwareInstallation  import MySiteRoot
from WorkflowLib.Module.ModuleBase                       import *
from WorkflowLib.Utilities.Tools import *
from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig

import shutil, re, string, os, sys

class GaudiApplication(ModuleBase):

  #############################################################################
  def __init__(self):
    ModuleBase.__init__(self)
    self.enable = True
    self.STEP_NUMBER = ''
    self.version = __RCSID__
    self.debug = True
    self.log = gLogger.getSubLogger( "GaudiApplication" )
    self.result = S_ERROR()
    self.optfile = ''
    self.run_number = 0
    self.firstEventNumber = 1
    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']

    self.systemConfig = ''
    self.applicationLog = ''
    self.applicationName = ''
    self.inputDataType = 'MDF'
    self.numberOfEvents = 0
    self.inputData = '' # to be resolved
    self.InputData = '' # from the (JDL WMS approach)
    self.outputData = ''
    self.poolXMLCatName = 'pool_xml_catalog.xml'
    self.generator_name=''
    self.optfile_extra = ''
    self.optionsLinePrev = ''
    self.optionsLine = ''
    self.extraPackages = ''


  #############################################################################
  def resolveInputDataPy(self,options):
    """ Input data resolution has two cases. Either there is explicitly defined
        input data for the application step (a subset of total workflow input data reqt)
        *or* this is defined at the job level and the job wrapper has created a
        pool_xml_catalog.xml slice for all requested files.

        Could be overloaded for python / standard options in the future.
    """
    if self.inputData:
      self.log.info('Input data defined in workflow for this Gaudi Application step')
      if type(self.inputData) != type([]):
        self.inputData = self.inputData.split(';')
    elif self.InputData:
      self.log.info('Input data defined taken from JDL parameter')
      if type(self.inputData) != type([]):
        self.inputData = self.InputData.split(';')
    else:
      self.log.verbose('Job has no input data requirement')

    if self.inputData:
      #write opts
      inputDataFiles = []
      for lfn in self.inputData:
        if self.inputDataType == "MDF":
          inputDataFiles.append(""" "DATAFILE='%s' SVC='LHCb::MDFSelector'", """ %(lfn))
        elif self.inputDataType in ("ETC","SETC","FETC"):
          inputDataFiles.append(""" "COLLECTION='TagCreator/1' DATAFILE='%s' TYPE='POOL_ROOTTREE' SEL='(GlobalOr>=1)' OPT='READ'", """%(lfn))
        else:
          inputDataFiles.append(""" "DATAFILE='%s' TYP='POOL_ROOTTREE' OPT='READ'", """ %(lfn))
      inputDataOpt = string.join(inputDataFiles,'\n')[:-2]
      evtSelOpt = """EventSelector().Input=[%s];\n""" %(inputDataOpt)
      options.write(evtSelOpt)
    poolOpt = """\nFileCatalog().Catalogs= ["xmlcatalog_file:%s"]\n""" %(self.poolXMLCatName)
    options.write(poolOpt)

  #############################################################################
  def managePy(self):
    if os.path.exists(self.optfile_extra): os.remove(self.optfile_extra)

    try:
        self.log.info("Adding extra options : %s" % (self.optionsLine))
        options = open(self.optfile_extra,'w')
        options.write('\n\n#//////////////////////////////////////////////////////\n')
        options.write('# Dynamically generated options in a production or analysis job\n\n')
        #TEMPORARY HACK because DaVinci doesn't yet have a configuration package.
        if self.applicationName.lower()=='davinci':
          options.write('from Gaudi.Configuration import *\n')
        else:
          options.write('from '+self.applicationName+'.Configuration import *\n')

        if self.optionsLine:
          for opt in self.optionsLine.split(';'):
              if len(opt) > 0:
                 options.write(opt+'\n')
        self.resolveInputDataPy(options)
        if self.run_number != 0 and self.applicationName == 'Gauss':
          options.write("""GaussGen = GenInit(\"GaussGen")\n""")
          options.write("""GaussGen.RunNumber = """+str(self.run_number)+"""\n""")

        if self.step_commons.has_key('firstEventNumber') and self.applicationName == 'Gauss':
          self.firstEventNumber = int(self.numberOfEvents) * (int(self.JOB_ID) - 1) + 1
          options.write("""GaussGen.FirstEventNumber = """+str(self.firstEventNumber)+"""\n""")

        if self.numberOfEvents != 0:
            options.write("""ApplicationMgr().EvtMax ="""+self.numberOfEvents+""" \n""")
        options.close()
    except Exception, x:
        print "No additional options"

  def resolveInputVariables(self):
    if self.workflow_commons.has_key('SystemConfig'):
       self.systemConfig = self.workflow_commons['SystemConfig']

    if self.step_commons.has_key('applicationName'):
       self.applicationName = self.step_commons['applicationName']
       self.applicationVersion = self.step_commons['applicationVersion']
       self.applicationLog = self.step_commons['applicationLog']

    if self.step_commons.has_key('numberOfEvents'):
       self.numberOfEvents = self.step_commons['numberOfEvents']

    if self.step_commons.has_key('optionsFile'):
       self.optionsFile = self.step_commons['optionsFile']

    if self.step_commons.has_key('optionsLine'):
       self.optionsLine = self.step_commons['optionsLine']

    if self.step_commons.has_key('optionsLinePrev'):
       self.optionsLinePrev = self.step_commons['optionsLinePrev']

    if self.step_commons.has_key('generatorName'):
      self.generator_name = self.step_commons['generatorName']

    if self.step_commons.has_key('extraPackages'):
       self.extraPackages = self.step_commons['extraPackages']

    if self.workflow_commons.has_key('poolXMLCatName'):
       self.poolXMLCatName = self.workflow_commons['poolXMLCatName']

    if self.step_commons.has_key('inputDataType'):
       self.inputDataType = self.step_commons['inputDataType']

    if self.workflow_commons.has_key('InputData'):
       self.InputData = self.workflow_commons['InputData']

    if self.step_commons.has_key('inputData'):
       self.inputData = self.step_commons['inputData']


  #############################################################################
  def execute(self):

    self.resolveInputVariables()
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
       self.log.info('Skip this module, failure detected in a previous step :')
       self.log.info('Workflow status : %s' %(self.workflowStatus))
       self.log.info('Step Status %s' %(self.stepStatus))
       return S_OK()

    self.result = S_OK()

    #self.setApplicationStatus( 'Initializing GaudiApplication' )

    if not self.applicationName or not self.applicationName:
      self.resul = S_ERROR( 'No Gaudi Application defined' )
    elif not self.systemConfig:
      self.result = S_ERROR( 'No LHCb platform selected' )
    elif not self.applicationLog:
      self.result = S_ERROR( 'No Log file provided' )

    if not self.result['OK']:
      return self.result

    if not self.optionsFile and not self.optionsLine:
      self.log.warn( 'No options File nor options Line provided' )

    self.log.info('Initializing '+self.version)

    self.result = S_OK()

    cwd = os.getcwd()
    self.root = gConfig.getValue('/LocalSite/Root',cwd)
    self.log.debug(self.version)
    self.log.info( "Executing application %s %s" % ( self.applicationName, self.applicationVersion ) )
    self.log.info("Platform for job is %s" % ( self.systemConfig ) )
    self.log.info("Root directory for job is %s" % ( self.root ) )

    sharedArea = MySiteRoot()
#    app_dir_path = CheckApplication( ( self.applicationName, self.applicationVersion ), self.systemConfig, sharedArea )
#    if app_dir_path:
#      mySiteRoot = sharedArea
#    else:
#      self.log.error( 'Application Not Found' )
      #self.setApplicationStatus( 'Application Not Found' )
#      self.result = S_ERROR( 'Application Not Found' )

    mySiteRoot=sharedArea
    self.log.info('MYSITEROOT is %s' %mySiteRoot)
    localArea = sharedArea
    if re.search(':',sharedArea):
      localArea = string.split(sharedArea,':')[0]
      self.log.info('Setting local software area to %s' %localArea)

    if not self.result['OK']:
      return self.result

    if self.applicationName == "Gauss" and self.PRODUCTION_ID and self.JOB_ID:
      self.run_number = runNumber(self.PRODUCTION_ID,self.JOB_ID)

    if self.optionsFile and not self.optionsFile == "None":
      print self.optionsFile
      print self.optionsFile.split(';')
      for fileopt in self.optionsFile.split(';'):
        if os.path.exists('%s/%s' %(cwd,os.path.basename(fileopt))):
          self.optfile += ' '+os.path.basename(fileopt)
        # Otherwise take the one from the application options directory
        elif re.search('\$',fileopt):
          self.log.info('Found options file containing environment variable: %s' %fileopt)
          self.optfile += '  %s' %(fileopt)
        else:
          self.log.info('Assume SetupProject environment knows where to find %s' %fileopt)
          self.optfile+=' '+fileopt
#          optpath = app_dir_path+'/options'
#          if os.path.exists(optpath+'/'+fileopt):
#            self.optfile += ' '+optpath+'/'+fileopt
#          else:
#            self.optfile += ' '+fileopt

    print 'final ',self.optfile
    self.optfile_extra = 'gaudi_extra_options.py'
    self.managePy()


    if os.path.exists(self.applicationName+'Run.sh'): os.remove(self.applicationName+'Run.sh')
    script = open(self.applicationName+'Run.sh','w')
    script.write('#!/bin/sh \n')
    script.write('#####################################################################\n')
    script.write('# Dynamically generated script to run a production or analysis job. #\n')
    script.write('#####################################################################\n')
    script.write('#'+self.version+'\n')
    script.write('#####################################################################\n')

    orig_ld_path = self.root
    if os.environ.has_key("LD_LIBRARY_PATH"):
      orig_ld_path = os.environ['LD_LIBRARY_PATH']
      self.log.info('original ld lib path is: '+orig_ld_path)

    script.write('declare -x MYSITEROOT='+mySiteRoot+'\n')
    script.write('declare -x CMTCONFIG='+self.systemConfig+'\n')
    script.write('declare -x CSEC_TRACE=1\n')
    script.write('declare -x CSEC_TRACEFI8LE=csec.log\n')
    script.write('. %s/LbLogin.sh\n' %localArea)
#    script.write('. '+mySiteRoot+'/scripts/ExtCMT.sh\n')

    # DLL fix which creates fake CMT package
    cmtFlag = ' '
    ld_base_path = os.path.abspath('.')
    if os.path.exists(ld_base_path+'/lib/requirements'):
      self.log.debug('User requirements file found, creating fake CMT package...')
      script.write('echo Creating Fake CMT package for user requirements file...\n')
      cmtStr = ld_base_path+'/'+self.applicationName+'_'+self.applicationVersion
      cmtProjStr = self.applicationName+'_'+self.applicationVersion
      cmtUpperStr = string.upper(self.applicationName)+' '+string.upper(self.applicationName)+'_'+self.applicationVersion
      script.write('mkdir -p '+cmtStr+'/cmttemp/v1/cmt\n')
      script.write('mkdir -p '+cmtStr+'/cmt\n')
      script.write('echo use '+cmtUpperStr+' >  '+cmtStr+'/cmt/project.cmt\n')
      script.write('export User_release_area='+ld_base_path+'\n')
      script.write('cp '+ld_base_path+'/lib/requirements '+cmtStr+'/cmttemp/v1/cmt\n')
      script.write('export CMTPATH='+cmtStr+'\n')
      cmtFlag = '--use="cmttemp v1" '
    script.write('echo $LHCBPYTHON\n')

    if not self.extraPackages == '':
      if type(self.extraPackages) != type([]):
        self.extraPackages = self.extraPackages.split(';')

      self.log.info('Found extra package versions: %s' %(string.join(self.extraPackages,', ')))
      for package in self.extraPackages:
        cmtFlag += '--use="%s %s" ' %(package.split('.')[0],package.split('.')[1])

    externals = ''
    site = gConfig.getValue('/LocalSite/Site','')
    if not site:
      externals = 'gfal CASTOR dcache_client lfc oracle' #should never happen, site is always defined
      self.log.info('/LocalSite/Site undefined so setting externals to: %s' %externals)
    else:
      if gConfig.getOption('/Operations/ExternalsPolicy/%s' %site)['OK']:
        externals = gConfig.getValue('/Operations/ExternalsPolicy/%s' %site,[])
        externals = string.join(externals,' ')
        self.log.info('Found externals policy for %s = %s' %(site,externals))
      else:
        externals = gConfig.getValue('/Operations/ExternalsPolicy/Default',[])
        externals = string.join(externals,' ')
        self.log.info('Using default externals policy for %s = %s' %(site,externals))

    setupProjectPath = os.path.dirname(os.path.realpath('%s/LbLogin.sh' %localArea))

    if self.generator_name == '':
#      script.write('. '+mySiteRoot+'/scripts/SetupProject.sh --debug --ignore-missing '+cmtFlag \
#                 +self.applicationName+' '+self.applicationVersion+' '+externals+'\n')
      script.write('. '+setupProjectPath+'/SetupProject.sh --debug --ignore-missing '+cmtFlag \
                 +self.applicationName+' '+self.applicationVersion+' '+externals+'\n')
    else:
#      script.write('. '+mySiteRoot+'/scripts/SetupProject.sh --debug --ignore-missing '+cmtFlag+' --tag_add='+self.generator_name+' ' \
#                 +self.applicationName+' '+self.applicationVersion+' '+externals+'\n')
      script.write('. '+setupProjectPath+'/SetupProject.sh --debug --ignore-missing '+cmtFlag+' --tag_add='+self.generator_name+' ' \
                 +self.applicationName+' '+self.applicationVersion+' '+externals+'\n')

    script.write('if [ $SetupProjectStatus != 0 ] ; then \n')
    script.write('   exit 1\nfi\n')

   #To fix Shr variable problem with component libraries
    if os.path.exists(ld_base_path+'/lib'):
      script.write("""
varlist=`env | cut -d = -f1`
liblist=`dir %s/lib`
for var in $varlist; do
  flag=`echo $var | awk '{print index($0,"Shr")}'`
  if [ $flag -gt 0 ]; then
     for lib in $liblist; do
       length=`echo $lib | awk '{print index($0,".so")}'`
       libbase=`echo $lib | awk '{print substr($0,4,(length-6))}'`
       libfinal=${libbase}Shr
       if [ $libfinal == $var ]; then
          echo Found user substitution for a standard library $lib pointed by $var
          newShrPath=lib${libbase}
          declare -x $var=%s/lib/$newShrPath
       fi
     done
  fi
done
""" % (ld_base_path,ld_base_path))

    #To ensure correct LD LIBRARY PATH now reconstruct
    script.write('declare -x LD_LIBRARY_PATH\n')
    #System  inis directory, to solve problems with e.g. dcap libraries etc. not being found
    localinis = os.path.abspath('localinis')
    script.write('declare -x LD_LIBRARY_PATH='+localinis+':${LD_LIBRARY_PATH}\n')
    #Application software inis directory
    absinis = os.path.abspath('inis')
    script.write('declare -x LD_LIBRARY_PATH='+absinis+':${LD_LIBRARY_PATH}\n')
    #Prepend DIRAC/lib
    script.write('declare -x LD_LIBRARY_PATH='+self.root+'/DIRAC/lib'+':${LD_LIBRARY_PATH}\n')
    #Finally prepend directory for user libraries
    script.write('declare -x LD_LIBRARY_PATH='+ld_base_path+'/lib:${LD_LIBRARY_PATH}\n') #DLLs always in lib dir

    compatLib = os.path.join( self.root, self.systemConfig, 'compat' )
    if os.path.exists(compatLib):
      script.write('declare -x LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:'+compatLib+'\n')

    shippedPythonComponents = '%s/python' %ld_base_path
    if os.path.exists(shippedPythonComponents):
      self.log.info('Found shipped python directory, prepending to PYTHONPATH')
      script.write('declare -x PYTHONPATH=%s:${PYTHONPATH}\n' %shippedPythonComponents)

    script.write('echo =============================\n')
    script.write('echo LD_LIBRARY_PATH is\n')
    script.write('echo $LD_LIBRARY_PATH | tr ":" "\n"\n')
    script.write('echo =============================\n')
    script.write('echo PATH is\n')
    script.write('echo $PATH | tr ":" "\n"\n')
    script.write('echo =============================\n')
    script.write('echo PYTHONPATH is\n')
    script.write('echo $PYTHONPATH | tr ":" "\n"\n')
    script.write('env | sort >> localEnv.log\n')
    script.write('export MALLOC_CHECK_=2\n')
    #To Deal with compiler libraries if shipped
#    comp_path = mySiteRoot+'/'+self.systemConfig
    comp_path = localArea+'/'+self.systemConfig #TODO: why is this not used elsewhere?
    if os.path.exists(comp_path):
      self.log.info('Compiler libraries found...')
      # Use the application loader shipped with the application if any (ALWAYS will be here)
      if self.generator_name == '':
        comm = 'gaudirun.py  '+self.optfile+' ./'+self.optfile_extra+'\n'
      else:
        comm = 'gaudirun.py  '+self.optfile+' $LB'+self.generator_name.upper()+'ROOT/options/'+self.generator_name+'.opts ./'+self.optfile_extra+'\n'

      print 'Command = ',comm
      script.write(comm)
      script.write('declare -x appstatus=$?\n')

    script.write('exit $appstatus\n')
    script.close()

    if os.path.exists(self.applicationLog): os.remove(self.applicationLog)

    os.chmod(self.applicationName+'Run.sh',0755)
    comm = 'sh -c "./'+self.applicationName+'Run.sh"'
    self.setApplicationStatus('%s %s step %s' %(self.applicationName,self.applicationVersion,self.STEP_NUMBER))
    self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    resultTuple = self.result['Value']

    status = resultTuple[0]
    stdOutput = resultTuple[1]
    stdError = resultTuple[2]

    self.log.info( "Status after the application execution is %s" % str( status ) )

    failed = False
    if status != 0:
      self.log.error( "%s execution completed with errors:" % self.applicationName )
      failed = True
    else:
      self.log.info( "%s execution completed succesfully:" % self.applicationName )


    if failed==True:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( stdError )
      #self.setApplicationStatus('%s Exited With Status %s' %(self.applicationName,status))
      self.log.error('%s Exited With Status %s' %(self.applicationName,status))
      return S_ERROR('%s Exited With Status %s' %(self.applicationName,status))

    # Still have to set the application status e.g. user job case.
    self.setApplicationStatus('%s %s Successful' %(self.applicationName,self.applicationVersion))
    return S_OK('%s %s Successful' %(self.applicationName,self.applicationVersion))

  #############################################################################
  def redirectLogOutput(self, fd, message):
    sys.stdout.flush()
    if message:
      if self.applicationLog:
        log = open(self.applicationLog,'a')
        log.write(message+'\n')
        log.close()
      else:
        self.log.error("Application Log file not defined")

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#