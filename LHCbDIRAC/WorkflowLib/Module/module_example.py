########################################################################
# $Id$
########################################################################
""" Gaudi Application Class """

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Utilities.Os import fixLDPath
from DIRAC.Core.Utilities.Subprocess import shellCall

import shutil, re, string, os

class GaudiApplication(object):

  def __init__(self):
    self.enable = True
    self.version = __RCSID__
    self.debug = True
    self.log = gLogger.getSubLogger( "GaudiApplication" )
    self.appLog = None
    self.result = S_ERROR()
    self.logfile = 'None'
    self.generator_name=''


  def execute(self):
    self.root = os.getcwd() #TO FIX...
    self.log.debug(self.version)
    self.log.debug( "Executing application %s %s" % ( self.appName, self.appVersion ) )
    self.log.debug("Platform for job is %s" % ( self.systemConfig ) )
    self.log.debug("Root directory for job is %s" % ( self.root ) )
    localDir = 'lib' #default

    # Check if the specified options file exists in the current directory,
    # for example, it is supplied in the job input sandbox

    ############################################################
    # set value for prefix - CERN convention
    convention = { 'DaVinci':'Phys',
                   'Bender':'Phys',
                   'Gauss':'Sim',
                   'Boole':'Digi',
                   'Brunel':'Rec'}

    prefix = convention[self.appName]
    #will think about this later
    #############################################################

    app_dir_path = self.root+'/lib/lhcb/'+string.upper(self.appName)+'/'+ \
                   string.upper(self.appName)+'_'+self.appVersion+'/'+prefix+'/' \
                   +self.appName+'/'+self.appVersion
    app_dir_path_install = self.root+'/lib/lhcb/'+string.upper(self.appName)+'/'+ \
                   string.upper(self.appName)+'_'+self.appVersion+'/InstallArea'

    #TO FIX
    mysiteroot = self.root
    if os.path.exists(mysiteroot+'/'+self.optionsFile):
      optfile = mysiteroot+'/'+self.optionsFile

    # Otherwise take the one from the application options directory
    else:
      optpath = app_dir_path+'/options'
      if os.path.exists(optpath+'/'+self.optionsFile):
        optfile = optpath+'/'+self.optionsFile
      else:
        optfile = self.optionsFile

    if os.path.exists('gaudirun.opts'): os.remove('gaudirun.opts')
    if os.path.exists('gaudiruntmp.opts'): os.remove('gaudiruntmp.opts')
    if re.search('\$',optfile) is None:
      comm = 'cat '+optfile+' > gaudiruntmp.opts'
      output = shellCall(0,comm)
    else:
      comm = 'echo "#include '+optfile+'" > gaudiruntmp.opts'
      commtmp = open('gaudiruntmp.opts','w')
      newline = '#include "'+optfile+'"'
      commtmp.write(newline)
      commtmp.close()

    optfile = 'gaudiruntmp.opts'

    if os.environ.has_key('ETC_Filename'):
      inputtmp = open(optfile)
      outputtmp = open(optfile+'.tmp','w')
      for line in inputtmp:
        newline = line.replace('$ETC_Filename',os.environ['ETC_Filename'])
        outputtmp.write(newline)

      outputtmp.close()
      inputtmp.close()
      os.rename(optfile+'.tmp',optfile)

    if os.environ.has_key('Presel_Filename'):
      inputtmp = open(optfile)
      outputtmp = open(optfile+'.tmp','w')
      for line in inputtmp:
        newline = line.replace('$Presel_Filename',os.environ['Presel_Filename'])
        outputtmp.write(newline)

      outputtmp.close()
      inputtmp.close()
      os.rename(optfile+'.tmp',optfile)

    comm = 'cat '+optfile+' gaudi.opts > gaudirun.opts'
    output = shellCall(0,comm)
    os.environ['JOBOPTPATH'] = 'gaudirun.opts'

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
      self.info('original ld lib path is: '+orig_ld_path)

    fixLDPath(self.root,orig_ld_path,'localinis')

    script.write('declare -x MYSITEROOT='+self.root+'/'+localDir+'\n')
    script.write('declare -x CMTCONFIG='+self.systemConfig+'\n')
 #   script.write('declare -x CMTROOT='+self.root+'/'+localDir+'\n')
    script.write('declare -x JOBOPTPATH='+os.environ['JOBOPTPATH']+'\n')
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
      script.write('python '+self.root+'/'+localDir+'/scripts/python/SetupProject.py --shell=sh --mktemp --ignore-missing '+cmtFlag \
                 +self.appName+' '+self.appVersion+' gfal lfc CASTOR dcache_client oracle -v \n')
    else:
      script.write('python '+self.root+'/'+localDir+'/scripts/python/SetupProject.py --shell=sh --ignore-missing '+cmtFlag+' --tag_add='+self.generator_name+ ' '+\
                 self.appName+' '+self.appVersion+' gfal lfc CASTOR dcache_client oracle -verbose \n')

    script.write('echo $LD_LIBRARY_PATH | tr ":" "\n"\n')
    #To handle oversized LD_LIBARARY_PATHs
    script.write('./dirac-fix-ld-library-path '+self.root+' $LD_LIBRARY_PATH inis\n')

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

#TO FIX
    #To configure extra packages outwith the standard distributions
#    for n,v in self.packages.items():
#      if n != self.appName:
#        print 'Extra package '+n+' '+v+' found...'
#        script.write('. '+self.root+'/'+localDir+'/scripts/SetupProject.sh --external-only --use="'+n+' '+v+'" '+self.appName+'\n')

    #Check for user shipped executable, to solve linker and component library problem
    if self.generator_name == '':
      exe_path = app_dir_path_install+'/'+self.systemConfig+'/bin/'+self.appName+'.exe ' #default
      if os.path.exists('lib/'+self.appName+'.exe'):
        exe_path = 'lib/'+self.appName+'.exe '
        print 'Found user shipped executable '+self.appName+'.exe...'
      else:
        exe_path = app_dir_path_install+'/'+self.systemConfig+'/bin/'+self.appName+'.exe '
    else:
      exe_path = app_dir_path_install+'/'+self.systemConfig+'/bin/'+self.appName+self.generator_name+'.exe ' #default
      if os.path.exists('lib/'+self.appName+self.generator_name+'.exe'):
        exe_path = 'lib/'+self.appName+self.generator_name+'.exe '
        print 'Found user shipped executable '+self.appName+self.generator_name+'.exe...'
      else:
        exe_path = app_dir_path_install+'/'+self.systemConfig+'/bin/'+self.appName+self.generator_name+'.exe '

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
      comm = comp_path+'/ld-linux.so.2 --library-path '+comp_path+':${LD_LIBRARY_PATH}'
      print 'Command = ',comm+' '+app_dir_path

    script.write('exit\n')
    script.write('#EOF\n')
    script.close()


    ###############################################################
    # Execute the application now
    #print "Processing application",self.appName

#TO DISCUSS
#    if self.step.parameters.has_key('LOOP_INDEX'):
#      self.job.report(self.appName+" execution, step "+str(self.step.getNumber())+ \
#                      " loop "+self.step.parameters['LOOP_INDEX'].value+"/"+ \
#                      self.step.parameters['LOOP'].value)
#    else:
#      self.job.report(self.appName+" execution, step "+str(self.step.getNumber()))

    self.appLog = self.appName+'_'+self.appVersion+'.log'
    if self.logfile != 'None':
      self.appLog = self.logfile

    if os.path.exists(self.appLog): os.remove(self.appLog)

    os.chmod(self.appName+'Run.sh',0755)
    comm = 'sh -c "./'+self.appName+'Run.sh"'

    self.result = shellCall(0,comm,callbackFunction=self.redirectLogOutput)
    resultTuple = self.result['Value']

    status = resultTuple[0]
    stdOutput = resultTuple[1]
    stdError = resultTuple[2]

    self.log.info( "Status after the application execution is %s" % str( status ) )

    logfile = open(self.appLog,'w')
    logfile.write(stdOutput)
    if stdError:
      logfile.write('\n\n\nError log:\n=============\n')
      logfile.write(stdError)
    logfile.close()

    failed = False
    if status > 0:
      self.log.error( "%s execution completed with errors:" % self.appName )
      failed = True
    elif stdError:
      self.log.error( "%s execution completed with application errors:" % self.appName )
      failed = True
    else:
      self.log.info( "%s execution completed succesfully:" % self.appName )

    if failed==True:
      self.log.error( "==================================\n StdOutput (last 100 lines) :\n" )
      lines = stdOutput.split('\n')
      if len(lines) > 100:
        self.log.error( string.join( lines[ -100: ],'\n')  )
      else:
        self.log.info( string.join( lines,'\n'))

      self.log.error( "==================================\n StdError:\n" )
      self.log.error( stdError )
      #TO THINK ABOUT
      #self.job.report(self.appName+" for step "+str(self.step.number)+" FAILED")

      # Return error for non-production jobs
     # if not self.jobType == 0:
      return S_ERROR(self.appName+" execution completed with errors")

    else:
      self.log.info( "==================================\n StdOutput (last 100 lines) :\n" )
      lines = output.split('\n')
      if len(lines) > 100:
        self.log.info( string.join( lines[ -100: ],'\n')  )
      else:
        self.log.info( string.join( lines,'\n'))

      self.log.info( "==================================\n StdError:\n" )
      self.log.info(stdError)

    # Return OK assuming that subsequent CheckLogFile will spot problems
    return S_OK()

########################################################################
  def redirectLogOutput(self, fd, message):
    if message:
      if self.appLog:
        log = open(self.appLog,'a')
        log.write(message+'\n')
        log.close()
      else:
        self.log.error("Application Log file not defined")

########################################################################

