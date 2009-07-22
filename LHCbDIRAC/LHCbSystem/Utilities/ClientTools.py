########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Utilities/ClientTools.py,v 1.11 2009/07/22 14:36:12 paterson Exp $
# File :   ClientTools.py
########################################################################

"""  The ClientTools module provides additional functions for use by users
     of the DIRAC client in the LHCb environment.
"""

__RCSID__ = "$Id: ClientTools.py,v 1.11 2009/07/22 14:36:12 paterson Exp $"

import string,re,os,shutil,types

import DIRAC

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.List import breakListIntoChunks

#############################################################################
def packageInputs(appName,appVersion,optionsFiles=[],destinationDir='',optsFlag=True,libFlag=True,pklOpts=False):
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
    result = _getOptsFiles(appName,appVersion,optionsFiles,destinationDir,pklOpts)
    if not result['OK']:
      return result
    finalResult['optionsFile']=result['Value']

  # Only retrieve libraries if lib flag is specified
  if libFlag:
    result = _getLibFiles(inputPath,destinationDir)
    if not result['OK']:
      return result
    finalResult['libraries']=result['Value']
    result = _getPythonDir(inputPath,destinationDir)
    if not result['OK']:
      return result
    finalResult['pythondir']=result['Value']

  return S_OK(finalResult)

#############################################################################
def _getOptsFiles(appName,appVersion,optionsFiles,destinationDir,pklOpts):
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
  if pklOpts:
    newOptsName = '%s/%s_%s.pkl' %(destinationDir,appName,appVersion)

  if os.path.exists(newOptsName):
    gLogger.warn('%s already exists, will be overwritten' %newOptsName)
  gLogger.verbose('Attempting to run gaudirun.py')
  cmdTuple = ['gaudirun.py']
  cmdTuple.append('-n')
  cmdTuple.append('-v')
  if pklOpts:
    cmdTuple.append('--output')
    cmdTuple.append(newOptsName)
  else:
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

  if not pklOpts:
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
def _getPythonDir(inputPath,destinationDir):
  """ Simple function to retrieve user python modules.
  """
  gLogger.verbose('dir is at :"%s"' % inputPath)
  if not os.path.exists(inputPath):
    return S_ERROR('Directory %s does not exist' %inputPath)
  if not os.path.exists('%s/python' %destinationDir):
    try:
      os.makedirs('%s/python' %destinationDir)
    except Exception,x:
     gLogger.warn('Could not create directory python in %s' %destinationDir)
     return S_ERROR(x)
  shutil.rmtree('%s/python' % destinationDir,True)
  shutil.copytree(inputPath+'/python','%s/python' % destinationDir)
  for fname in os.listdir(destinationDir+'/python'):
    gLogger.verbose('Copied file:"%s"' % fname)
  return S_OK('%s/python' %destinationDir)

#############################################################################
def _errorReport(error,message=None):
  """Internal function to return errors and exit with an S_ERROR()
  """
  if not message:
    message = error

  gLogger.warn(error)
  return S_ERROR(message)

#############################################################################
def getRootFileGUID(fileName,cleanUp=True):
  """ Function to retrieve a file GUID using Root.
  """
  setupProject = ['SetupProject']
  setupProject.append( '--ignore-missing' )
  setupProject.append( 'DaVinci' )
  sharedArea = ''
  if os.environ.has_key('VO_LHCB_SW_DIR'):
    sharedArea = os.path.join(os.environ['VO_LHCB_SW_DIR'],'lib')
    gLogger.verbose( 'Using VO_LHCB_SW_DIR at "%s"' % sharedArea )
  elif DIRAC.gConfig.getValue('/LocalSite/SharedArea',''):
    sharedArea = DIRAC.gConfig.getValue('/LocalSite/SharedArea')
    gLogger.verbose( 'Using SharedArea at "%s"' % sharedArea )

  if not sharedArea:
    gLogger.error('Could not find VO_LHCB_SW_DIR or /LocalSite/SharedArea in local configuration')
    return S_ERROR('Could not find local shared software area')

  lbLogin = '%s/LbLogin' %sharedArea
  ret = DIRAC.Source( 60,[lbLogin], dict(os.environ))
  if not ret['OK']:
    gLogger.warn('Error during lbLogin\n%s' %ret)
    return ret

  setupProject = ['%s/%s' %(os.path.dirname(os.path.realpath('%s.sh' %lbLogin)),'SetupProject')]
  setupProject.append('DaVinci')
  ret = DIRAC.Source( 60, setupProject, ret['outputEnv'] )
  if not ret['OK']:
    gLogger.warn('Error during SetupProject\n%s' %ret)
    return ret

  appEnv = ret['outputEnv']
  fopen = open('tmpRootScript.py','w')
  fopen.write('from ROOT import TFile\n')
  fopen.write("l=TFile.Open('%s')\n" %fileName)
  fopen.write("t=l.Get(\'##Params\')\n")
  fopen.write('t.Show(0)\n')
  fopen.write('leaves=t.GetListOfLeaves()\n')
  fopen.write('leaf=leaves.UncheckedAt(0)\n')
  fopen.write('val=leaf.GetValueString()\n')
  fopen.write("fid=val.split('=')[2].split(']')[0]\n")
  fopen.write("print 'GUID%sGUID' %fid\n")
  fopen.write('l.Close()\n')
  fopen.close()
  cmd = ['python']
  cmd.append('tmpRootScript.py')
  gLogger.debug(cmd)
  ret = DIRAC.systemCall( 1800, cmd, env=appEnv, callbackFunction=log )
  if not ret['OK']:
    gLogger.error('Problem using root\n%s' %ret)
    return ret

  if cleanUp:
    os.remove('tmpRootScript.py')

  stdout = ret['Value'][1]
  try:
    guid = stdout.split('GUID')[1]
  except Exception,x:
    gLogger.error('Could not obtain GUID from file')
    return S_ERROR('Failed to get GUID from file')

  gLogger.verbose('GUID found to be %s' %guid)
  return S_OK(guid)

#############################################################################
def mergeRootFiles(outputFile,inputFiles,daVinciVersion='',cleanUp=True):
  # Setup the root enviroment
  res = _setupRootEnvironment(daVinciVersion)
  if not res['OK']:
    return _errorReport(res['Message'],"Failed to setup the ROOT environment")
  rootEnv = res['Value']
  # Perform the merging
  lists = breakListIntoChunks(inputFiles,20)
  tempFiles = []
  counter = 0
  for list in lists:
    counter += 1
    tempOutputFile = "/tmp/tempRootFile-%s.root" % counter
    res = _mergeRootFiles(tempOutputFile,list,rootEnv)
    if not res['OK']:
      return _errorReport(res['Message'],"Failed to perform ROOT merger")
    tempFiles.append(tempOutputFile)
  res = _mergeRootFiles(outputFile,tempFiles,rootEnv)
  if not res['OK']:
    return _errorReport(res['Message'],"Failed to perform final ROOT merger")
  if cleanUp:
    for file in tempFiles: os.remove(file)
  return S_OK(outputFile)

#############################################################################
def _mergeRootFiles(outputFile,inputFiles,rootEnv):
  cmd = "hadd %s" % outputFile
  for file in inputFiles:
    cmd = "%s %s" % (cmd,file)
  res = DIRAC.shellCall(1800, cmd, env=rootEnv)
  return res

#############################################################################
def _setupRootEnvironment(daVinciVersion=''):
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
    return ret
  setupProject = ['%s/%s' %(os.path.dirname(os.path.realpath('%s.sh' %lbLogin)),'SetupProject')]
  if daVinciVersion:
    setupProject.append('DaVinci %s ROOT' % daVinciVersion)
  else:
    setupProject.append('DaVinci ROOT')
  ret = DIRAC.Source( 60, setupProject, ret['outputEnv'])
  if not ret['OK']:
    gLogger.warn('Error during SetupProject\n%s' %ret)
    return ret
  appEnv = ret['outputEnv']
  return S_OK(appEnv)

#############################################################################
def log( n, line ):
  gLogger.debug( line )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
