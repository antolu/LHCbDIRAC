########################################################################
# $Id$
########################################################################
""" Gaudi Application Class """

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.Resources.Catalog.PoolXMLCatalog              import PoolXMLCatalog
from DIRAC.Core.DISET.RPCClient                          import RPCClient

from LHCbDIRAC.Core.Utilities.ProductionData                import constructProductionLFNs,_makeProductionLfn,_getLFNRoot
from LHCbDIRAC.Core.Utilities.CombinedSoftwareInstallation  import MySiteRoot
from LHCbDIRAC.Core.Utilities.CondDBAccess                  import getCondDBFiles
from LHCbDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase

from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig, List
import DIRAC

import re, string, os, sys, time, glob

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
    self.applicationType = ''
    self.jobType = ''


  #############################################################################
  def resolveInputDataPy(self):
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

    options = []
    if self.inputData:
      
      #write opts
      inputDataFiles = []
      for lfn in self.inputData:
        lfn = lfn.replace('LFN:','').replace('lfn:','')
        if self.inputDataType == "MDF":
          inputDataFiles.append(""" "DATAFILE='LFN:%s' SVC='LHCb::MDFSelector'", """ %(lfn))
        elif self.inputDataType in ("ETC","SETC","FETC"):
          inputDataFiles.append(""" "COLLECTION='TagCreator/EventTuple' DATAFILE='LFN:%s' TYP='POOL_ROOT' SEL='(StrippingGlobal==1)' OPT='READ'", """%(lfn))
        elif self.inputDataType == 'RDST':
          if re.search('rdst$',lfn):
            inputDataFiles.append(""" "DATAFILE='LFN:%s' TYP='POOL_ROOTTREE' OPT='READ'", """ %(lfn))
          else:
            self.log.info('Ignoring file %s for %s step with input data type %s' %(lfn,self.applicationName,self.inputDataType))
        else:
          inputDataFiles.append(""" "DATAFILE='LFN:%s' TYP='POOL_ROOTTREE' OPT='READ'", """ %(lfn))
      inputDataOpt = string.join(inputDataFiles,'\n')[:-2]
      evtSelOpt = """EventSelector().Input=[%s];\n""" %(inputDataOpt)
      options.append(evtSelOpt)

      if self.applicationName.lower()=='moore':
        options = []
        options.append('from Configurables import Moore')
        mooreInput = ['LFN:%s' %i.replace('lfn:','').replace('LFN:','') for i in self.inputData]        
        options.append("Moore().inputFiles = %s" %(mooreInput))

    poolOpt = """\nFileCatalog().Catalogs= ["xmlcatalog_file:%s"]\n""" %(self.poolXMLCatName)
    options.append(poolOpt)
    return S_OK(options)

  #############################################################################
  def managePy(self):
    if os.path.exists(self.optfile_extra): os.remove(self.optfile_extra)

    try:
        optionsLines=[]
        optionsLines.append('\n\n#//////////////////////////////////////////////////////')
        optionsLines.append('# Dynamically generated options in a production or analysis job\n')
        #TEMPORARY HACK because DaVinci doesn't yet have a configuration package.
        if self.applicationName.lower()=='davinci' or self.applicationName.lower()=='lhcb':
          optionsLines.append('from Gaudi.Configuration import *')
        else:
          optionsLines.append('from %s.Configuration import *' %self.applicationName)

        # Always download the SQLite DDDB file locally to the job
        #optionsLines.append("""from Configurables import CondDB\n""")
        #optionsLines.append("""CondDB(SQLiteLocalCopiesDir = \".\")\n""")

        if self.optionsLine:
          for opt in self.optionsLine.split(';'):
              if opt:
                 optionsLines.append(opt)
        inputDataOpts=self.resolveInputDataPy()
        if inputDataOpts['OK']:
          optionsLines+=inputDataOpts['Value']
        if self.run_number != 0 and self.applicationName == 'Gauss':
          optionsLines.append("GaussGen = GenInit(\"GaussGen\")")
          optionsLines.append("GaussGen.RunNumber = %s" %(self.run_number))

        if self.step_commons.has_key('firstEventNumber') and self.applicationName == 'Gauss':
          self.firstEventNumber = int(self.numberOfEvents) * (int(self.JOB_ID) - 1) + 1
          optionsLines.append("GaussGen.FirstEventNumber = %s" %(self.firstEventNumber))

        if self.numberOfEvents != 0:
            optionsLines.append("ApplicationMgr().EvtMax = %s" %(self.numberOfEvents))
        self.log.info('Extra options generated by GaudiApplication for %s %s step:' %(self.applicationName,self.applicationVersion))
        finalLines = string.join(optionsLines,'\n')+'\n'
        print finalLines
        options = open(self.optfile_extra,'w')
        options.write(finalLines)
        options.close()
    except Exception, x:
        print x
        print "No additional options"

  #############################################################################
  def resolveInputVariables(self):
    """ Resolve all input variables for the module here.
    """

    if self.workflow_commons.has_key('SystemConfig'):
      self.systemConfig = self.workflow_commons['SystemConfig']

    if self.step_commons.has_key('applicationName'):
      self.applicationName = self.step_commons['applicationName']
      self.applicationVersion = self.step_commons['applicationVersion']
      self.applicationLog = self.step_commons['applicationLog']

    if self.step_commons.has_key('applicationType'):
      self.applicationType = self.step_commons['applicationType']

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

    if self.workflow_commons.has_key('JobType'):
      self.jobType = self.workflow_commons['JobType']

    #only required until the stripping is the same for MC / data
    if self.workflow_commons.has_key('configName'):
      self.bkConfigName = self.workflow_commons['configName']

    if self.step_commons.has_key('listoutput'):
      self.stepOutputs = self.step_commons['listoutput']


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
      self.result = S_ERROR( 'No Gaudi Application defined' )
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

    ## FIXME: need to agree what the name of the Online Farm is
    if DIRAC.siteName() == 'DIRAC.ONLINE-FARM.ch':
      return self.onlineExecute()

    sharedArea = MySiteRoot()
    if sharedArea == '':
      self.log.error( 'MySiteRoot Not found' )
      return S_ERROR(' MySiteRoot Not Found')

    mySiteRoot=sharedArea
    self.log.info('MYSITEROOT is %s' %mySiteRoot)
    localArea = sharedArea
    if re.search(':',sharedArea):
      localArea = string.split(sharedArea,':')[0]
    self.log.info('Setting local software area to %s' %localArea)

    if self.applicationName == "Gauss" and self.PRODUCTION_ID and self.JOB_ID:
      self.run_number =  int(self.PRODUCTION_ID)*100+int(self.JOB_ID)

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
          self.log.info('Assume options file %s is in $%sOPTS' %(fileopt,self.applicationName.upper()))
          self.optfile+=' $%sOPTS/%s' %(self.applicationName.upper(),fileopt)
#          optpath = app_dir_path+'/options'
#          if os.path.exists(optpath+'/'+fileopt):
#            self.optfile += ' '+optpath+'/'+fileopt
#          else:
#            self.optfile += ' '+fileopt

    print 'Final options files:',self.optfile
    toClean = []
    if re.search('disablelfc',self.optfile.lower()):
      self.log.info('CORAL LFC Access is disabled, obtaining XML files...')
      result = getCondDBFiles()
      if not result['OK']:
        self.log.error('Could not obtain CondDB XML files with message:\n%s' %(result['Message']))
        return result
      else:
        self.log.info('Successfully obtained Oracle CondDB XML access files')
        self.log.verbose(result)
        for f in result['Value']: toClean.append(f)

    self.optfile_extra = 'gaudi_extra_options.py'
    self.managePy()

    scriptName = '%s_%s_Run_%s.sh' %(self.applicationName,self.applicationVersion,self.STEP_NUMBER)

    if os.path.exists(scriptName): os.remove(scriptName)
    script = open(scriptName,'w')
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
    script.write('declare -x CSEC_TRACEFILE=csec.log\n')
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
    if gConfig.getOption( '/Operations/ExternalsPolicy/%s' % DIRAC.siteName() )['OK']:
      externals = gConfig.getValue( '/Operations/ExternalsPolicy/%s' % DIRAC.siteName(), [] )
      externals = string.join(externals,' ')
      self.log.info('Found externals policy for %s = %s' %( DIRAC.siteName(), externals ) )
    else:
      externals = gConfig.getValue('/Operations/ExternalsPolicy/Default',[])
      externals = string.join(externals,' ')
      self.log.info('Using default externals policy for %s = %s' %( DIRAC.siteName(), externals ) )


    setupProjectPath = os.path.dirname(os.path.realpath('%s/LbLogin.sh' %localArea))

    if self.generator_name == '':
#      script.write('. '+mySiteRoot+'/scripts/SetupProject.sh --debug --ignore-missing '+cmtFlag \
#                 +self.applicationName+' '+self.applicationVersion+' '+externals+'\n')
      script.write('. '+setupProjectPath+'/SetupProject.sh --debug --ignore-missing '+cmtFlag \
                 +self.applicationName+' '+self.applicationVersion+' '+externals+' \n')
    else:
#      script.write('. '+mySiteRoot+'/scripts/SetupProject.sh --debug --ignore-missing '+cmtFlag+' --tag_add='+self.generator_name+' ' \
#                 +self.applicationName+' '+self.applicationVersion+' '+externals+'\n')
      script.write('. '+setupProjectPath+'/SetupProject.sh --debug --ignore-missing '+cmtFlag+' --tag_add='+self.generator_name+' ' \
                 +self.applicationName+' '+self.applicationVersion+' '+externals+' \n')

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
    #To Deal with compiler libraries if shipped
#    comp_path = mySiteRoot+'/'+self.systemConfig
    comp_path = localArea+'/'+self.systemConfig #TODO: why is this not used elsewhere?
    if os.path.exists(comp_path):
      self.log.info('Compiler libraries found...')

    # Use the application loader shipped with the application if any (ALWAYS will be here)
    comm = 'gaudirun.py  '+self.optfile+' '+self.optfile_extra+'\n'
#      if self.generator_name == '':
#        comm = 'gaudirun.py  '+self.optfile+' ./'+self.optfile_extra+'\n'
#      else:
#        comm = 'gaudirun.py  '+self.optfile+' $LB'+self.generator_name.upper()+'ROOT/options/'+self.generator_name+'.opts ./'+self.optfile_extra+'\n'

    print 'Command = ',comm
    script.write(comm)
    script.write('declare -x appstatus=$?\n')
    script.write('# check for core dumps and analyze it if present\n')
    script.write('if [ -e core.* ] ; then\n')
    script.write('  gdb python core.* >> %s_coredump.log << EOF\n' % self.applicationName )
    script.write('where\n')
    script.write('quit\n')
    script.write('EOF\n')
    script.write('fi\n')

    script.write('exit $appstatus\n')
    script.close()

    if os.path.exists(self.applicationLog): os.remove(self.applicationLog)

    os.chmod(scriptName,0755)
    comm = 'sh -c "./%s"' %scriptName
    self.setApplicationStatus('%s %s step %s' %(self.applicationName,self.applicationVersion,self.STEP_NUMBER))
    self.stdError = ''
    self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput,bufferLimit=20971520)
    #self.result = {'OK':True,'Value':(0,'Disabled Execution','')}
    resultTuple = self.result['Value']

    status = resultTuple[0]
    # stdOutput = resultTuple[1]
    # stdError = resultTuple[2]

    for f in toClean:
      self.log.verbose('Removing temporary file: %s' %f)
      if os.path.exists(f):
        os.remove(f)

    self.log.info( "Status after the application execution is %s" % str( status ) )

    failed = False
    if status != 0:
      self.log.error( "%s execution completed with errors:" % self.applicationName )
      failed = True
    else:
      self.log.info( "%s execution completed succesfully:" % self.applicationName )

    if failed==True:
      self.log.error( "==================================\n StdError:\n" )
      self.log.error( self.stdError )
      #self.setApplicationStatus('%s Exited With Status %s' %(self.applicationName,status))
      self.log.error('%s Exited With Status %s' %(self.applicationName,status))
      return S_ERROR('%s Exited With Status %s' %(self.applicationName,status))

    #For the MC stripping case, must add the streams generated by DaVinci
    #initially only the MC case separates the streams... if the same applies
    #for real data the last condition below can be removed then e.g. configName = MC or data
    if self.jobType.lower()=='datastripping' and self.applicationName.lower()=='davinci' and self.applicationType.lower()=='dst' and self.inputDataType.lower()=='dst':
      self.log.info('DataStripping DaVinci DST step, will attempt to add output data files to the global list of output data')
      finalOutputs=[]
      toMatch = ''
      outputDataSE = ''
      for output in self.stepOutputs:
        if output['outputDataType']=='dst':
          outputDataSE = output['outputDataSE']
          toMatch = output['outputDataName'].split('.')[0]
          globList = glob.glob('*%s*dst' %toMatch)
          self.log.info('Pattern to match is "*%s*dst"' %toMatch)
          strippingFiles = []
          for check in globList:
            if os.path.isfile(check):
              self.log.info('Found output file matching "*%s*dst": %s' %(toMatch,check))
              strippingFiles.append(check)
          for f in strippingFiles:
            bkType = string.upper(string.join(string.split(f,'.')[1:],'.'))
            finalOutputs.append({'outputDataName':f,'outputDataType':'DST','outputDataSE':outputDataSE,'outputBKType':bkType})
        else:
          finalOutputs.append(output)
      self.log.info('Final step outputs are: %s' %(finalOutputs))
      self.workflow_commons['outputList'] = finalOutputs + self.workflow_commons['outputList']
      self.log.info('Attempting to recreate the production output LFNs...')
      result = constructProductionLFNs(self.workflow_commons)
      if not result['OK']:
        self.log.error('Could not create production LFNs',result['Message'])
        return result
      self.workflow_commons['BookkeepingLFNs']=result['Value']['BookkeepingLFNs']
      self.workflow_commons['LogFilePath']=result['Value']['LogFilePath']
      self.workflow_commons['ProductionOutputData']=result['Value']['ProductionOutputData']
      self.step_commons['listoutput']=finalOutputs

    # Still have to set the application status e.g. user job case.
    self.setApplicationStatus('%s %s Successful' %(self.applicationName,self.applicationVersion))
    return S_OK('%s %s Successful' %(self.applicationName,self.applicationVersion))

  #############################################################################
  def redirectLogOutput(self, fd, message):
    sys.stdout.flush()
    if message:
      if re.search('INFO Evt',message): print message
      if self.applicationLog:
        log = open(self.applicationLog,'a')
        log.write(message+'\n')
        log.close()
      else:
        self.log.error("Application Log file not defined")
      if fd == 1:
        self.stdError += message
  #############################################################################

  def onlineExecute( self ):
    """Use for the Online Farm."""
    import xmlrpclib
    from operator import itemgetter
    xmlrpcerror = "Cannot connect to RecoManager"
    matcherror = "Cannot find slice in RecoManager"
    inputoutputerror = "Input/Output data error"
    # 0: checks
    if not self.workflow_commons.has_key( 'dataType' ):
      return S_ERROR( inputoutputerror )
    dataType = self.workflow_commons[ 'dataType' ].lower()
    if not self.workflow_commons.has_key('configName'):
      return S_ERROR( inputoutputerror )
    configName = self.workflow_commons['configName']
    if self.workflow_commons.has_key('configVersion'):
      configVersion = self.workflow_commons['configVersion']
    else:
      configVersion = self.applicationVersion
    if not self.step_commons.has_key( 'outputData' ):
      return S_ERROR( inputoutputerror )
    if not self.step_commons.has_key( 'listoutput' ):
      return S_ERROR( inputoutputerror )
    outputDataName = None
    outputDataType = None
    for output in self.step_commons[ 'listoutput' ]:
      if output[ 'outputDataName' ] == self.step_commons[ 'outputData' ]:
        outputDataName = output[ 'outputDataName' ]
        outputDataType = output[ 'outputDataType' ]
        break
    if not ( outputDataType and outputDataName ):
      return S_ERROR( inputoutputerror )
    if not 'applicationLog' in self.step_commons:
      return S_ERROR( "No log file specified" )
    # First: Get the full requirements for the job.
    bkProcessingPass = self.workflow_commons[ 'BKProcessingPass' ]
    step = 'Step%d' %( int(self.STEP_NUMBER) - 1 )
    bkProcessingPass[ step ][ 'ExtraPackages' ] = List.fromChar( bkProcessingPass[ step ][ 'ExtraPackages' ] , ';' )
    bkProcessingPass[ step ][ 'OptionFiles' ] = List.fromChar( bkProcessingPass[ step ][ 'OptionFiles' ] , ';' )
    # Second: get the application configuration from the RecoManager XMLRPC
    recoManager = xmlrpclib.ServerProxy( 'http://storeio01.lbdaq.cern.ch:8889' )
    # recoManager = DummyRPC()
    try:
      result = recoManager.sliceStatus()
    except:
      self.log.exception()
      return S_ERROR( xmlrpcerror )
    if not result[ 'OK' ]:
      self.log.error( result[ 'Message' ] )
      return S_ERROR( matcherror )
    # Third: find slices which match the given configuration options
    validSlices = {}
    for sliceName in result[ 'Value' ]:
      sliceConfig = result[ 'Value' ][ sliceName ][ 'config' ]
      self.log.debug( 'Comparing:%s and %s' %( bkProcessingPass[ step ] , sliceConfig ) )
      if self.compareConfigs( bkProcessingPass[ step ] , sliceConfig ):
        validSlices[ sliceName ] = result[ 'Value' ][ sliceName ][ 'availability' ]
    if len( validSlices ) == 0:
      self.log.error( "No slice found matching configuration" )
      return S_ERROR( matcherror )
    # Fourth: find which of the matching slices is better for job sending (more availability)
    sliceName = sorted( validSlices.iteritems(), key = itemgetter(1), reverse = True )[0][0]
    # Fifth: submit the file and wait.
    inputData = self.inputData.lstrip( 'LFN:' ).lstrip( 'lfn:' )
    lfnRoot = _getLFNRoot( inputData, configName, configVersion )
    outputFile = _makeProductionLfn( self.JOB_ID, lfnRoot, (outputDataName, outputDataType), dataType, self.PRODUCTION_ID )
    outputFile = outputFile.lstrip( 'LFN:' ).lstrip( 'lfn:' )
    poolXMLCatalog = PoolXMLCatalog( self.poolXMLCatName )
    try:
      guid = poolXMLCatalog.getGuidByLfn( inputData )
    except:
      self.log.exception()
      return S_ERROR( "Error getting GUID for inputfile" )
    logFile = os.path.abspath( self.step_commons[ 'applicationLog' ] )
    try:
      result = recoManager.submitJob( sliceName, inputData , outputFile , logFile, guid )
    except:  
      self.log.exception()
      return S_ERROR( xmlrpcerror )      
    if not result[ 'OK' ]:
      # if 'fileID' in result['Value']:
      #   fileID = result[ 'Value' ]
      #   res = recoManager.getJobOutput(fileID)
        # log = res['Value']['log']
        # writeLogFromList( loglines )
      self.log.error( "Error running job" , result[ 'Message' ] )
      return S_ERROR( "Error submiting job" )
    # The submission went well
    if os.path.exists( self.applicationLog ):
      os.remove( self.applicationLog )
    self.setApplicationStatus( '%s %s step %s' %( self.applicationName, self.applicationVersion, self.STEP_NUMBER ) )
    jobID = result[ 'Value' ]
    retrycount = 0
    while True:
      time.sleep(20)
      try:
        ret = recoManager.jobStatus( jobID )
      except:
        self.log.exception()        
        return S_ERROR( xmlrpcerror )
      if not ret[ 'OK' ]:
        retrycount = retrycount + 1
        if retrycount > 5:
          return S_ERROR( ret[ 'Message' ] )
        continue
      retrycount = 0
      jobstatus = ret[ 'Value' ][ 'status' ]
      if jobstatus in [ 'DONE' , 'ERROR' ]:
        ret = recoManager.getJobOutput( jobID )
        if not ret[ 'OK' ]:
          outputError = "Error retrieving output of jobID: %s" %jobID
          self.log.error( outputError , ret[ 'Message' ] )
          return S_ERROR( outputError )
        jobInfo = ret[ 'Value' ] # ( status , inputevents , outputevents , logfile , path )
        # Hack: create symlink to output data
        for path in jobInfo[ 'path' ].values():
          os.symlink( path , os.path.basename( path ) )
        self.step_commons[ 'numberOfEventsInput' ] = str( jobInfo[ 'eventsRead' ] )
        self.step_commons[ 'numberOfEventsOutput' ] = str( jobInfo[ 'eventsWritten' ] )
        self.step_commons[ 'md5' ] = jobInfo[ 'md5' ]
        self.step_commons[ 'guid' ] = jobInfo[ 'guid' ]
        # loglines = jobInfo[ 'log' ]
        # writeLogFromList( loglines )
        self.log.info( "Status after the application execution is %s" %jobstatus )
        failed = False
        if jobstatus == 'ERROR':
          self.log.error( "%s execution completed with errors:" % self.applicationName )
          failed = True
        else:
          self.log.info( "%s execution completed succesfully:" % self.applicationName )
        if failed:
          self.log.error('%s Exited With Status %s' %(self.applicationName,jobstatus))
          return S_ERROR('%s Exited With Status %s' %(self.applicationName,jobstatus))
        self.setApplicationStatus('%s %s Successful' %(self.applicationName,self.applicationVersion))
        print self.step_commons
        return S_OK('%s %s Successful' %(self.applicationName,self.applicationVersion))

  def compareConfigs( self , config1 , config2 ): # FIXME
    if len(config1.keys()) != len(config2.keys()):
      return False
    for key in config1:
      if not key in config2:
        return False
      else:
        if key == 'ExtraPackages':
          if not sorted( config2[ 'ExtraPackages' ] ) == sorted( config1[ 'ExtraPackages' ] ):
            return False
        elif config1[ key ] != config2[ key ]:
          return False
    return True
    
  # def writeLogFromList( self , loglines ):
  #   log = open( self.step_commons[ 'applicationLog' ] , 'w' )
  #   for line in loglines:
  #     log.write( "%s\n" %line )
  #   log.close()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#