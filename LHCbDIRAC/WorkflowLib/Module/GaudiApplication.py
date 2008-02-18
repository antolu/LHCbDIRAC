########################################################################
# $Id: GaudiApplication.py,v 1.20 2008/02/18 16:35:27 paterson Exp $
########################################################################
""" Gaudi Application Class """

__RCSID__ = "$Id: GaudiApplication.py,v 1.20 2008/02/18 16:35:27 paterson Exp $"

from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig

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
      lfns = [string.replace(fname,'LFN:','') for fname in self.inputData]
      inputDataFiles = []
      for lfn in lfns:
        if self.inputDataType == "MDF":
          inputDataFiles.append(""" "DATAFILE='LFN:%s' SVC='LHCb::MDFSelector'", """ %(lfn))
        else:
          inputDataFiles.append(""" "DATAFILE='LFN:%s' TYP='POOL_ROOTTREE' OPT='READ'", """ %(lfn))
      inputDataOpt = string.join(inputDataFiles,'\n')[:-2]
      evtSelOpt = """EventSelector.Input={%s};\n""" %(inputDataOpt)
      options.write(evtSelOpt)
    if self.outputData != None:
      for opt in self.outputData.split(';'):
        options.write("""DstWriter.Output = "DATAFILE='PFN:%s' TYP='POOL_ROOTTREE' OPT='RECREATE'";\n""" %(opt))

#    poolOpt = """\nPoolDbCacheSvc.Catalog= {"xmlcatalog_file:%s"};\n""" %(self.poolXMLCatName)
#    options.write(poolOpt)
    poolOpt = """\nFileCatalog.Catalogs= {"xmlcatalog_file:%s"};\n""" %(self.poolXMLCatName)
    options.write(poolOpt)

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
      lfns = [string.replace(fname,'LFN:','') for fname in self.inputData]
      inputDataFiles = []
      for lfn in lfns:
        if self.inputDataType == "MDF":
          inputDataFiles.append(""" "DATAFILE='LFN:%s' SVC='LHCb::MDFSelector'", """ %(lfn))
        else:
          inputDataFiles.append(""" "DATAFILE='LFN:%s' TYP='POOL_ROOTTREE' OPT='READ'", """ %(lfn))
      inputDataOpt = string.join(inputDataFiles,'\n')[:-2]
      evtSelOpt = """EventSelector().Input=[%s];\n""" %(inputDataOpt)
      options.write(evtSelOpt)
    if self.outputData != None:
      for opt in self.outputData.split(';'):
        options.write("""OutputStream("DstWriter").Output = "DATAFILE='PFN:'+opt+' TYP='POOL_ROOTTREE' OPT='RECREATE'""")

#    poolOpt = """\nPoolDbCacheSvc().Catalog= ["xmlcatalog_file:%s"]\n""" %(self.poolXMLCatName)
#    options.write(poolOpt)
    poolOpt = """\nFileCatalog().Catalogs= ["xmlcatalog_file:%s"]\n""" %(self.poolXMLCatName)
    options.write(poolOpt)

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
        options.close()
    except Exception, x:
        print "No additional options"

  #############################################################################
  def execute(self):
    self.__report('Initializing GaudiApplication')
    cwd = os.getcwd()
    self.root = gConfig.getValue('/LocalSite/Root',cwd)
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
      self.optfile_extra = 'gaudi_extra_options.opts'
      optionsType = 'opts'
      self.manageOpts()
    else:
      optionsType = 'py'
      self.optfile_extra = 'gaudi_extra_options.py'
      self.managePy()

#    comm = open(optfile,'a')
#    newline = """OutputStream("DstWriter").Output = "DATAFILE='PFN:joel.dst' TYPE='POOL_ROOTTREE' OPT='REC'"\n """
#    comm.write(newline)
#    comm.close()


#    os.environ['JOBOPTPATH'] = optfile

    if os.path.exists(self.appName+'Run.sh'): os.remove(self.appName+'Run.sh')
    script = open(self.appName+'Run.sh','w')
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

    #os.system('python '+self.root+'/scripts/fixLDpath.py '+orig_ld_path+' None localinis')
##JC    os.system('%s %s/scripts/dirac-fix-ld-library-path %s None localinis\n' %(sys.executable,self.root,orig_ld_path))
    script.write('declare -x MYSITEROOT='+self.root+'/'+localDir+'\n')
    script.write('declare -x CMTCONFIG='+self.systemConfig+'\n')
    script.write('. '+self.root+'/'+localDir+'/scripts/ExtCMT.sh\n')

    # DLL fix which creates fake CMT package
    cmtFlag = ' '
    ld_base_path = os.path.abspath('.')
    if os.path.exists(ld_base_path+'/lib/requirements'):
      self.log.debug('User requirements file found, creating fake CMT package...')
      script.write('echo Creating Fake CMT package for user requirements file...\n')
      cmtStr = ld_base_path+'/'+self.appName+'_'+self.appVersion
      cmtProjStr = self.appName+'_'+self.appVersion
      cmtUpperStr = string.upper(self.appName)+' '+string.upper(self.appName)+'_'+self.appVersion
      script.write('mkdir -p '+cmtStr+'/cmttemp/v1/cmt\n')
      script.write('mkdir -p '+cmtStr+'/cmt\n')
      script.write('echo use '+cmtUpperStr+' >  '+cmtStr+'/cmt/project.cmt\n')
      script.write('export User_release_area='+ld_base_path+'\n')
      script.write('cp '+ld_base_path+'/lib/requirements '+cmtStr+'/cmttemp/v1/cmt\n')
      script.write('export CMTPATH='+cmtStr+'\n')
      cmtFlag = '--use="cmttemp v1" '
    script.write('echo $LHCBPYTHON\n')
    if self.generator_name == '':
      script.write('. '+self.root+'/'+localDir+'/scripts/SetupProject.sh --ignore-missing '+cmtFlag \
                 +self.appName+' '+self.appVersion+' gfal lfc CASTOR dcache_client oracle\n')
    else:
      script.write('. '+self.root+'/'+localDir+'/scripts/SetupProject.sh --ignore-missing '+cmtFlag+' --tag_add='+self.generator_name+ ' '+\
                 self.appName+' '+self.appVersion+' gfal lfc CASTOR dcache_client oracle\n')

    script.write('echo $LD_LIBRARY_PATH | tr ":" "\n"\n')
    #To handle oversized LD_LIBARARY_PATHs
##JC    script.write('%s %s/scripts/dirac-fix-ld-library-path $LD_LIBRARY_PATH %s inis\n' %(sys.executable,self.root,orig_ld_path))
#    script.write('python '+self.root+'/DIRAC/scripts/fixLDpath.py $LD_LIBRARY_PATH '+orig_ld_path+' inis\n')

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

    script.write('echo LD_LIBRARY_PATH is\n')
    script.write('echo $LD_LIBRARY_PATH\n')
    script.write('echo PATH is\n')
    script.write('echo $PATH\n')
    script.write('env | sort >> localEnv.log\n')

    #To Deal with compiler libraries if shipped
    comp_path = self.root+'/'+localDir+'/'+self.systemConfig
    if os.path.exists(comp_path):
      print 'Compiler libraries found...'
      # Use the application loader shipped with the application if any (ALWAYS will be here)
      if optionsType == 'py':
        comm = 'gaudirun.py  '+self.optfile+' ./'+self.optfile_extra+'\n'
      else:
        exe_path = app_dir_path_install+'/'+self.systemConfig+'/bin/'+self.appName+'.exe ' #default
        if os.path.exists('lib/'+self.appName+'.exe'):
          exe_path = 'lib/'+self.appName+'.exe '
          print 'Found user shipped executable '+self.appName+'.exe...'
        else:
          exe_path = app_dir_path_install+'/'+self.systemConfig+'/bin/'+self.appName+'.exe '

        #comm = comp_path+'/ld-linux.so.2 --library-path '+comp_path+':${LD_LIBRARY_PATH} '+
        comm = exe_path+' '+os.environ['JOBOPTPATH']+'\n'

      print 'Command = ',comm
      script.write(comm)

    script.write('exit\n')
    script.write('#EOF\n')
    script.close()

    if self.appLog == None:
      self.appLog = self.logfile
#    else:
#      self.appLog = self.appName+'_'+self.appVersion+'.log'

    if os.path.exists(self.appLog): os.remove(self.appLog)

    os.chmod(self.appName+'Run.sh',0755)
    comm = 'sh -c "./'+self.appName+'Run.sh"'
    self.__report('%s %s' %(self.appName,self.appVersion))
    self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput)
    resultTuple = self.result['Value']

    status = resultTuple[0]
    stdOutput = resultTuple[1]
    stdError = resultTuple[2]

    self.log.info( "Status after the application execution is %s" % str( status ) )

    logfile = open(self.appLog,'w')
    logfile.write(stdOutput)

    if len(stdError) > 0:
      logfile.write('\n\n\nError log:\n=============\n')
      logfile.write(stdError)
    logfile.close()

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
      return S_ERROR(self.appName+" execution completed with errors")

    # Return OK assuming that subsequent CheckLogFile will spot problems
    self.__report('%s %s Successful' %(self.appName,self.appVersion))
    return S_OK()

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

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#