########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Utilities/ClientTools.py,v 1.2 2008/12/12 16:00:14 paterson Exp $
# File :   ClientTools.py
########################################################################

"""  The ClientTools module provides additional functions for use by users
     of the DIRAC client in the LHCb environment.
"""

__RCSID__ = "$Id: ClientTools.py,v 1.2 2008/12/12 16:00:14 paterson Exp $"

import string,re,os,shutil,types

import DIRAC

from DIRAC import gConfig, gLogger, S_OK, S_ERROR

#############################################################################
def packageInputs(appName,appVersion,optionsFiles=[],destinationDir='',optsFlag=True,libFlag=True):
  """Under development. Helper function.

     Relies on CMTPROJECTPATH and CMTCONFIG variables.

     Options files can be specified with environment variables e.g. $DAVINCIUSERROOT/options/myopts.opts.

     The destination directory defaults to the current working directory if not specified.

     Flags can be specified to enable or disable the retrieval of libraries or options.

     Example usage:

     >>> from DIRAC.LHCbSystem.Utilities.ClientTools import packageInputs
     >>> packageInputs('DaVinci','v20r3',optionsFiles=['$DAVINCIUSERROOT/myOpts.py'])

     @param condDict: CondDB tags
     @type condDict: Dict of DB, tag pairs
  """
  if not type(appName) in types.StringTypes or not type(appVersion) in types.StringTypes:
    gLogger.warn()
    return _errorReport('Expected strings for application name and version')

  if optsFlag:
    if not optionsFiles:
      return _errorReport('Expected string or list for optionsFiles')
    if type(optionsFiles) in types.StringTypes:
      optionsFiles = [optionsFiles]
    if not type(optionsFiles) == type([]):
      return _errorReport('Expected string or list for optionsFiles')

  if not destinationDir:
    destinationDir = os.getcwd()

  localEnv = dict(os.environ)
  if not localEnv.has_key('CMTPROJECTPATH'):
    gLogger.warn('Expected CMTPROJECTPATH to be defined')
  if not localEnv.has_key('CMTCONFIG'):
    gLogger.warn('Expected CMTCONFIG to be defined')

  systemConfig = localEnv['CMTCONFIG']
  userArea = ''
  for path in string.split(localEnv['CMTPROJECTPATH'],':'):
    if re.search('cmtuser',path):
      userArea = path

  if not userArea:
    gLogger.warn('Could not establish user CMT area from CMTPROJECTPATH')

  #not sure if there's also a lib there at the end or not
  inputPath = '%s/%s_%s/InstallArea/%s' %(userArea,appName,appVersion,systemConfig)

  finalResult = {'optionsFile':'','libraries':''}

  # Only run gaudirun if opts flag is specified
  if optsFlag:
    result = _getOptsFiles(appName,appVersion,optionsFiles,destinationDir)
    if not result['OK']:
      return result
    finalResult['optionsFile']=result['Value']

  # Only retrieve libraries if lib flag is specified
  if libFlag:
    result = _getLibFiles(inputPath,destinationDir)
    if not result['OK']:
      return result
    finalResult['libraries']=result['Value']

  return S_OK(finalResult)

#############################################################################
def _getOptsFiles(appName,appVersion,optionsFiles,destinationDir):
  """Set up project environment and expand options.
  """
  gLogger.verbose('Options files to locate are: %s' %string.join(optionsFiles,', '))
  setupProject = ['SetupProject']
  setupProject.append( '--ignore-missing' )
  setupProject.append( appName )
  setupProject.append( appVersion )

  if os.environ.has_key('VO_LHCB_SW_DIR'):
    sharedArea = os.path.join(os.environ['VO_LHCB_SW_DIR'],'lib')
    gLogger.verbose( 'Using VO_LHCB_SW_DIR at "%s"' % sharedArea )
  elif DIRAC.gConfig.getValue('/LocalSite/SharedArea',''):
    sharedArea = DIRAC.gConfig.getValue('/LocalSite/SharedArea')
    gLogger.verbose( 'Using SharedArea at "%s"' % sharedArea )
  lbLogin = '%s/LbLogin' %sharedArea
  ret = DIRAC.Source( 60,[lbLogin], dict(os.environ))
  if not ret['OK']:
    gLogger.warn('Error during lbLogin\n%s' %ret)

  setupProject = ['%s/%s' %(os.path.dirname(os.path.realpath('%s.sh' %lbLogin)),'SetupProject')]
  setupProject.append(appName)
  setupProject.append(appVersion)
  ret = DIRAC.Source( 60, setupProject, ret['outputEnv'] )
  if not ret['OK']:
    gLogger.warn('Error during SetupProject\n%s' %ret)

  appEnv = ret['outputEnv']
  toCheck = []
  toInclude = []
  for optFile in optionsFiles:
    if not re.search('\$',optFile):
      toCheck.append(optFile)
    else:
      toInclude.append(optFile)

  for n,v in appEnv.items():
    for optFile in toInclude:
      if re.search('\$%s' %n,optFile):
        toCheck.append(string.replace(optFile,'$%s' %n,v))

#  if toInclude:
#    gLogger.verbose('Options files are: %s' %(string.join(toInclude,'\n')))
  if toCheck:
    gLogger.verbose('Environment expanded options files are: %s' %(string.join(toCheck,'\n')))

  if not len(toCheck)==len(optionsFiles):
    gLogger.warn('Could not account for all options files')

  missing = []
  for optFile in toCheck:
    if not os.path.exists(optFile):
      missing.append(optFile)
#      shutil.copy(optFile,'%s/%s' %(destinationDir,os.path.basename(optFile)))

  if missing:
    gLogger.error('The following options files could not be found:\n%s' %(string.join(missing,'\n')))
    return S_ERROR(missing)

  newOptsName = '%s/%s_%s.opts' %(destinationDir,appName,appVersion)
  if os.path.exists(newOptsName):
    gLogger.warn('%s already exists, will be overwritten' %newOptsName)
  gLogger.verbose('Attempting to run gaudirun.py')
  cmdTuple = ['gaudirun.py']
  cmdTuple.append('-n')
  cmdTuple.append('-v')
  cmdTuple.append('--old-opts')
  for i in toCheck:
    cmdTuple.append(i)
#  cmdTuple.append('>!')
#  cmdTuple.append(newOptsName)
  gLogger.verbose('Commmand is: %s'%(string.join(cmdTuple,' ')))
  ret = DIRAC.systemCall( 1800, cmdTuple, env=appEnv, callbackFunction=log )
  if not ret['OK']:
    gLogger.error('Problem during gaudirun.py call\n%s' %ret)
    return S_ERROR('Could not package job inputs')

  fopen = open(newOptsName,'w')
  fopen.write(ret['Value'][1])
  fopen.close()
  return S_OK(newOptsName)

#############################################################################
def _getLibFiles(inputPath,destinationDir):
  """ Simple function to retrieve user libraries.
  """
  gLogger.verbose('dir is at :"%s"' % inputPath)
  if not os.path.exists(inputPath):
    return S_ERROR('Directory %s does not exist' %inputPath)

  if not os.path.exists('%s/lib' %destinationDir):
    try:
      os.makedirs('%s/lib' %destinationDir)
    except Exception,x:
      gLogger.warn('Could not create directory lib in %s' %destinationDir)
      return S_ERROR(x)
  shutil.rmtree('%s/lib' % destinationDir,True)
  shutil.copytree(inputPath+'/lib','%s/lib' % destinationDir)
  for fname in os.listdir(destinationDir+'/lib'):
    gLogger.verbose('Copied file:"%s"' % fname)
  return S_OK('%s/lib' %destinationDir)


#############################################################################
def _errorReport(self,error,message=None):
  """Internal function to return errors and exit with an S_ERROR()
  """
  if not message:
    message = error

  gLogger.warn(error)
  return S_ERROR(message)

#############################################################################
def log( n, line ):
  gLogger.verbose( line )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#