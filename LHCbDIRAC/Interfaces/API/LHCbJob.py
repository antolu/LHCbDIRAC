"""LHCb Job Class

   The LHCb Job class inherits generic VO functionality from the Job base class
   and provides VO-specific functionality to aid in the construction of
   workflows.

   Helper functions are documented with example usage for the DIRAC API.

   Below are several examples of LHCbJob usage.

   An example DaVinci application script would be::

     from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
     from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

     j = LHCbJob()
     j.setCPUTime(5000)
     j.setApplication('DaVinci','v19r12','DaVinciv19r12.opts',
     optionsLine='ApplicationMgr.EvtMax=1',
     inputData=['/lhcb/production/DC06/phys-v2-lumi2/00001650/DST/0000/00001650_00000054_5.dst'])
     j.setName('MyJobName')
     #j.setDestination('LCG.CERN.cern')

     dirac = DiracLHCb()
     jobID = dirac.submitJob(j)
     print 'Submission Result: ',jobID

   The setDestination() method is optional and takes the DIRAC site name as an argument.

   Another example for executing a script in the Gaudi Application environment is::

     from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
     from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

     j = LHCbJob()
     j.setCPUTime(5000)
     j.setApplicationScript('DaVinci','v19r11','myGaudiPythonScript.py',
     inputData=['/lhcb/production/DC06/phys-lumi2/00001501/DST/0000/00001501_00000320_5.dst'])
     j.setName('MyJobName')
     #j.setDestination('LCG.CERN.cern')

     dirac = DiracLHCb()
     jobID = dirac.submitJob(j)
     print 'Submission Result: ',jobID

   For execution of a python Bender module::

     from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
     from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

     j = LHCbJob()
     j.setCPUTime(5000)
     j.setBenderModule('v8r3','BenderExample.PhiMC',
     inputData=['LFN:/lhcb/production/DC06/phys-v2-lumi2/00001758/DST/0000/00001758_00000001_5.dst'],numberOfEvents=100)
     j.setName('MyJobName')

     dirac = DiracLHCb()
     jobID = dirac.submitJob(j)
     print 'Submission Result: ',jobID

   To execute a ROOT Macro, Python script and Executable consecutively an example script would be::

     from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
     from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

     j = LHCbJob()
     j.setCPUTime(50000)
     j.setRootMacro('5.18.00a','test.C')
     j.setRootPythonScript('5.18.00a','test.py')
     j.setRootExecutable('5.18.00a','minexam')

     dirac = DiracLHCb()
     jobID = dirac.submitJob(j,mode='local')
     print 'Submission Result: ',jobID

"""

import os
import re
from distutils.version import LooseVersion  # pylint: disable=import-error,no-name-in-module

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Core.Utilities.File import makeGuid, mkDir
from DIRAC.Core.Utilities.List import uniqueElements
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Workflow.Utilities.Utils import getStepDefinition, addStepToWorkflow

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.Core.Utilities.ProductionEnvironment import getPlatformFromLHCbConfig
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb


class LHCbJob(Job):
  """ LHCbJob class as extension of DIRAC Job class
  """

  #############################################################################

  def __init__(self, script=None, stdout='std.out', stderr='std.err'):
    """Instantiates the Workflow object and some default parameters.
    """
    super(LHCbJob, self).__init__(script, stdout, stderr)
    self.stepCount = 0
    self.inputDataType = 'DATA'  # Default, other options are MDF, ETC
    self.importLocation = 'LHCbDIRAC.Workflow.Modules'
    self.opsHelper = Operations()

  #############################################################################

  def setApplication(self, appName, appVersion, optionsFiles, inputData=None,
                     optionsLine=None, inputDataType=None, logFile=None, events=-1,
                     extraPackages=None, systemConfig='ANY',
                     modulesNameList=None, parametersList=None):
    """Specifies gaudi application for DIRAC workflows, executed via gaudirun.

       For LHCb these could be e.g. Gauss, Boole, Brunel, DaVinci, LHCb, etc.

       The optionsFiles parameter can be the path to an options file or a list of paths to options files.
       All options files are automatically appended to the job input sandBox but the first in the case of a
       list is assumed to be the 'master' options file.

       Input data for application script must be specified here, please note that if this is set at the job level,
       e.g. via setInputData() but not above, input data is not in the scope of the specified application.

       Any input data specified at the step level that is not already specified at the job level is added automatically
       as a requirement for the job.

       Example usage:

       >>> job = LHCbJob()
       >>> job.setApplication('DaVinci','v19r5',optionsFiles='MyDV.opts',
       inputData=['/lhcb/production/DC06/phys-lumi2/00001501/DST/0000/00001501_00000320_5.dst'],logFile='dv.log')

       :param appName: Application name
       :type appName: string
       :param appVersion: Application version
       :type appVersion: string
       :param optionsFiles: Path to options file(s) for application
       :type optionsFiles: string or list
       :param inputData: Input data for application (if a subset of the overall input data for a given job is required)
       :type inputData: single LFN or list of LFNs
       :param optionsLine: Additional options lines for application
       :type optionsLine: string
       :param inputDataType: Input data type for application (e.g. DATA, MDF, ETC)
       :type inputDataType: string
       :param logFile: Optional log file name
       :type logFile: string
       :param events: Optional number of events
       :type events: integer
       :param extraPackages: Optional extra packages
       :type extraPackages: string
       :param modulesNameList: list of module names. The default is given.
       :type modulesNameList: list
       :param parametersList: list of parameters. The default is given.
       :type parametersList: list
    """
    kwargs = {'appName': appName, 'appVersion': appVersion, 'optionsFiles': optionsFiles,
              'inputData': inputData, 'optionsLine': optionsLine, 'inputDataType': inputDataType, 'logFile': logFile}
    if not isinstance(appName, str) or not isinstance(appVersion, str):
      return self._reportError('Expected strings for application name and version', __name__, **kwargs)

    if logFile:
      if isinstance(logFile, str):
        logName = logFile
      else:
        return self._reportError('Expected string for log file name', __name__, **kwargs)
    else:
      logName = '%s_%s.log' % (appName, appVersion)

    if not inputDataType:
      inputDataType = self.inputDataType
    if not isinstance(inputDataType, str):
      return self._reportError('Expected string for input data type', __name__, **kwargs)

    optionsFile = None
    if not optionsFiles:
      return self._reportError('Expected string or list for optionsFiles', __name__, **kwargs)
    if isinstance(optionsFiles, str):
      optionsFiles = [optionsFiles]
    if not isinstance(optionsFiles, list):
      return self._reportError('Expected string or list for optionsFiles', __name__, **kwargs)
    for optsFile in optionsFiles:
      if not optionsFile:
        self.log.verbose('Found master options file %s' % optsFile)
        optionsFile = optsFile
      if os.path.exists(optsFile):
        self.log.verbose('Found specified options file: %s' % optsFile)
        self.addToInputSandbox.append(optsFile)
        optionsFile += ';%s' % optsFile
      elif re.search(r'\$', optsFile):
        self.log.verbose('Assuming %s is using an environment variable to be resolved during execution' % optsFile)
        if not optionsFile == optsFile:
          optionsFile += ';%s' % optsFile
      else:
        return self._reportError('Specified options file %s does not exist' % (optsFile), __name__, **kwargs)

    # ensure optionsFile list is unique:
    tmpList = optionsFile.split(';')
    optionsFile = ';'.join(list(set(tmpList)))
    self.log.verbose('Final options list is: %s' % optionsFile)

    if inputData:
      if isinstance(inputData, str):
        inputData = [inputData]
      if not isinstance(inputData, list):
        return self._reportError('Expected single LFN string or list of LFN(s) for inputData', __name__, **kwargs)
      if inputData != ['previousStep']:
        for i in xrange(len(inputData)):
          inputData[i] = inputData[i].replace('LFN:', '')
        inputData = ['LFN:' + x for x in inputData]
        inputDataStr = ';'.join(inputData)
        self.addToInputData.append(inputDataStr)

    self.stepCount += 1
    stepName = '%sStep%s' % (appName, self.stepCount)

    if not modulesNameList:
      modulesNameList = ['CreateDataFile',
                         'GaudiApplication',
                         'FileUsage',
                         'AnalyseFileAccess',
                         'UserJobFinalization']
    if not parametersList:
      parametersList = [('applicationName', 'string', '', 'Application Name'),
                        ('applicationVersion', 'string', '', 'Application Version'),
                        ('applicationLog', 'string', '', 'Name of the output file of the application'),
                        ('optionsFile', 'string', '', 'Options File'),
                        ('extraOptionsLine', 'string', '', 'This is appended to standard options'),
                        ('inputDataType', 'string', '', 'Input Data Type'),
                        ('inputData', 'string', '', 'Input Data'),
                        ('numberOfEvents', 'string', '', 'Events treated'),
                        ('extraPackages', 'string', '', 'ExtraPackages'),
                        ('SystemConfig', 'string', '', 'CMT Config')]

    step = getStepDefinition(stepName,
                             modulesNameList=modulesNameList,
                             importLine="LHCbDIRAC.Workflow.Modules",
                             parametersList=parametersList)

    logPrefix = 'Step%s_' % (self.stepCount)
    logName = '%s%s' % (logPrefix, logName)
    self.addToOutputSandbox.append(logName)

    stepInstance = addStepToWorkflow(self.workflow, step, stepName)

    stepInstance.setValue('applicationName', appName)
    stepInstance.setValue('applicationVersion', appVersion)
    stepInstance.setValue('applicationLog', logName)
    if optionsFile:
      stepInstance.setValue('optionsFile', optionsFile)
    if optionsLine:
      stepInstance.setValue('extraOptionsLine', optionsLine)
    if inputDataType:
      stepInstance.setValue('inputDataType', inputDataType)
    if inputData:
      stepInstance.setValue('inputData', ';'.join(inputData))
    stepInstance.setValue('numberOfEvents', str(events))
    stepInstance.setValue('extraPackages', extraPackages)
    stepInstance.setValue('SystemConfig', systemConfig)

    return S_OK(stepInstance)

  #############################################################################

  def setApplicationScript(self, appName, appVersion, script, arguments='', inputData='',
                           inputDataType='', poolXMLCatalog='pool_xml_catalog.xml', logFile='',
                           extraPackages='', systemConfig='ANY',
                           modulesNameList=None, parametersList=None):
    """Specifies application environment and script to be executed.

       For LHCb these could be e.g. Gauss, Boole, Brunel,
       DaVinci etc.

       The script name and any arguments should also be specified.

       Input data for application script must be specified here, please note that if this is set at the job level,
       e.g. via setInputData() but not above, input data is not in the scope of the specified application.

       Any input data specified at the step level that is not already specified at the job level is added automatically
       as a requirement for the job.

       Example usage:

       >>> job = LHCbJob()
       >>> job.setApplicationScript('DaVinci','v19r12','myScript.py')

       :param appName: Application name
       :type appName: string
       :param appVersion: Application version
       :type appVersion: string
       :param script: Script to execute
       :type script: string
       :param arguments: Optional arguments for script
       :type arguments: string
       :param inputData: Input data for application
       :type inputData: single LFN or list of LFNs
       :param inputDataType: Input data type for application (e.g. DATA, MDF, ETC)
       :type inputDataType: string
       :param arguments: Optional POOL XML Catalog name for any input data files (default is pool_xml_catalog.xml)
       :type arguments: string
       :param logFile: Optional log file name
       :type logFile: string
       :param modulesNameList: list of module names. The default is given.
       :type modulesNameList: list
       :param parametersList: list of parameters. The default is given.
       :type parametersList: list
    """
    kwargs = {
        'appName': appName,
        'appVersion': appVersion,
        'script': script,
        'arguments': arguments,
        'inputData': inputData,
        'inputDataType': inputDataType,
        'poolXMLCatalog': poolXMLCatalog,
        'logFile': logFile}
    if not isinstance(appName, str) or not isinstance(appVersion, str):
      return self._reportError('Expected strings for application name and version', __name__, **kwargs)

    if not script or not isinstance(script, str):
      return self._reportError('Expected strings for script name', __name__, **kwargs)

    if not os.path.exists(script):
      return self._reportError('Script must exist locally', __name__, **kwargs)

    if logFile:
      if isinstance(logFile, str):
        logName = logFile
      else:
        return self._reportError('Expected string for log file name', __name__, **kwargs)
    else:
      shortScriptName = os.path.basename(script).split('.')[0]
      logName = '%s_%s_%s.log' % (appName, appVersion, shortScriptName)

    self.addToInputSandbox.append(script)

    if arguments:
      if not isinstance(arguments, str):
        return self._reportError('Expected string for optional script arguments', __name__, **kwargs)

    if not isinstance(poolXMLCatalog, str):
      return self._reportError('Expected string for POOL XML Catalog name', __name__, **kwargs)

    if inputData:
      if isinstance(inputData, str):
        inputData = [inputData]
      if not isinstance(inputData, list):
        return self._reportError('Expected single LFN string or list of LFN(s) for inputData', __name__, **kwargs)
      if inputData != ['previousStep']:
        for i in xrange(len(inputData)):
          inputData[i] = inputData[i].replace('LFN:', '')
        inputData = ['LFN:' + x for x in inputData]
        inputDataStr = ';'.join(inputData)
        self.addToInputData.append(inputDataStr)

    self.stepCount += 1

    stepName = '%sStep%s' % (appName, self.stepCount)

    if not modulesNameList:
      modulesNameList = ['CreateDataFile',
                         'GaudiApplicationScript',
                         'FileUsage',
                         'UserJobFinalization']
    if not parametersList:
      parametersList = [('applicationName', 'string', '', 'Application Name'),
                        ('applicationVersion', 'string', '', 'Application Version'),
                        ('applicationLog', 'string', '', 'Name of the output file of the application'),
                        ('script', 'string', '', 'Script Name'),
                        ('arguments', 'string', '', 'Arguments for executable'),
                        ('poolXMLCatName', 'string', '', 'POOL XML Catalog file name'),
                        ('inputDataType', 'string', '', 'Input Data Type'),
                        ('inputData', 'string', '', 'Input Data'),
                        ('extraPackages', 'string', '', 'extraPackages'),
                        ('SystemConfig', 'string', '', 'CMT Config')]

    step = getStepDefinition(stepName,
                             modulesNameList=modulesNameList,
                             importLine="LHCbDIRAC.Workflow.Modules",
                             parametersList=parametersList)

    logPrefix = 'Step%s_' % (self.stepCount)
    logName = '%s%s' % (logPrefix, logName)
    self.addToOutputSandbox.append(logName)

    stepInstance = addStepToWorkflow(self.workflow, step, stepName)

    stepInstance.setValue("applicationName", appName)
    stepInstance.setValue("applicationVersion", appVersion)
    stepInstance.setValue("script", script)
    stepInstance.setValue("applicationLog", logName)
    if arguments:
      stepInstance.setValue("arguments", arguments)
    if inputDataType:
      stepInstance.setValue("inputDataType", inputDataType)
    if inputData:
      stepInstance.setValue("inputData", ';'.join(inputData))
    if poolXMLCatalog:
      stepInstance.setValue("poolXMLCatName", poolXMLCatalog)
    stepInstance.setValue('extraPackages', extraPackages)
    stepInstance.setValue('SystemConfig', systemConfig)

    return S_OK(stepInstance)

  #############################################################################

  def setBenderModule(self, benderVersion, modulePath, inputData=None, numberOfEvents=-1):
    """Specifies Bender module to be executed.

       Any additional files should be specified in the job input sandbox.  Input data for
       Bender should be specified here (can be string or list).

       Example usage:

       >>> job = LHCbJob()
       >>> job.setBenderModule('v8r3','BenderExample.PhiMC',
       inputData=['LFN:/lhcb/production/DC06/phys-v2-lumi2/00001758/DST/00001758_00000001_5.dst'],numberOfEvents=100)

       :param benderVersion: Bender Project Version
       :type benderVersion: string
       :param modulePath: Import path to module e.g. BenderExample.PhiMC
       :type modulePath: string
       :param inputData: Input data for application
       :type inputData: single LFN or list of LFNs
       :param numberOfEvents: Number of events to process e.g. -1
       :type numberOfEvents: integer
    """
    kwargs = {'benderVersion': benderVersion, 'modulePath': modulePath,
              'inputData': inputData, 'numberOfEvents': numberOfEvents}
    if not isinstance(benderVersion, str):
      return self._reportError('Bender version should be a string', __name__, **kwargs)
    if not isinstance(modulePath, str):
      return self._reportError('Bender module path should be a string', __name__, **kwargs)
    if not isinstance(numberOfEvents, int):
      try:
        numberOfEvents = int(numberOfEvents)
      except ValueError as _x:
        return self._reportError('Number of events should be an integer or convertible to an integer',
                                 __name__, **kwargs)
    if not inputData:
      return S_ERROR("Need input data for Bender applications")

    if isinstance(inputData, str):
      inputData = [inputData]
    if not isinstance(inputData, list):
      return self._reportError('Input data should be specified as a list or a string', __name__, **kwargs)

    poolCatName = 'xmlcatalog_file:pool_xml_catalog.xml'
    benderScript = ['#!/usr/bin/env python']
    benderScript.append('from Gaudi.Configuration import FileCatalog')
    benderScript.append('FileCatalog   ( Catalogs = [ "%s" ] )' % poolCatName)
    benderScript.append('import %s as USERMODULE' % modulePath)
    benderScript.append('USERMODULE.configure()')
    benderScript.append('gaudi = USERMODULE.appMgr()')
    benderScript.append('evtSel = gaudi.evtSel()')
    benderScript.append('evtSel.open ( %s ) ' % inputData)
    benderScript.append('USERMODULE.run( %s )\n' % numberOfEvents)
    guid = makeGuid()
    tmpdir = '/tmp/' + guid
    self.log.verbose('Created temporary directory for submission %s' % (tmpdir))
    mkDir(tmpdir)
    fopen = open('%s/BenderScript.py' % tmpdir, 'w')
    self.log.verbose('Bender script is: %s/BenderScript.py' % tmpdir)
    fopen.write('\n'.join(benderScript))
    fopen.close()
    # should try all components of the PYTHONPATH before giving up...
    userModule = '%s.py' % (modulePath.split('.')[-1])
    self.log.verbose('Looking for user module with name: %s' % userModule)
    if os.path.exists(userModule):
      self.addToInputSandbox.append(userModule)
    self.setInputData(inputData)
    self.setApplicationScript('Bender', benderVersion, '%s/BenderScript.py' % tmpdir,
                              logFile='Bender%s.log' % benderVersion)
    return S_OK(benderScript)

  #############################################################################

  def setRootMacro(self, rootVersion, rootScript, arguments=None, logFile=None, systemConfig='ANY'):
    """Specifies ROOT version and macro to be executed (e.g. root -b -f <rootScript>).

       Can optionally specify arguments to the script and a name for the output log file.

       Example usage:

       >>> job = LHCbJob()
       >>> j.setRootMacro('5.18.00a','test.C')

       :param rootVersion: LHCb supported ROOT version
       :type rootVersion: string
       :param rootScript: Path to ROOT macro script
       :type rootScript: string
       :param arguments: Optional arguments for macro
       :type arguments: string or list
       :param logFile: Optional log file name
       :type logFile: string
    """
    rootType = 'c'
    return self.__configureRootModule(rootVersion, rootScript, rootType, arguments, logFile, systemConfig)

  #############################################################################

  def setRootPythonScript(self, rootVersion, rootScript, arguments=None, logFile=None, systemConfig='ANY'):
    """Specifies ROOT version and python script to be executed (e.g. python <rootScript>).

       Can optionally specify arguments to the script and a name for the output log file.

       Example usage:

       >>> job = LHCbJob()
       >>> j.setRootPythonScript('5.18.00a','test.py')

       :param rootVersion: LHCb supported ROOT version
       :type rootVersion: string
       :param rootScript: Path to ROOT python script
       :type rootScript: string
       :param arguments: Optional arguments for python script
       :type arguments: string or list
       :param logFile: Optional log file name
       :type logFile: string
    """
    rootType = 'py'
    return self.__configureRootModule(rootVersion, rootScript, rootType, arguments, logFile, systemConfig)

  #############################################################################

  def setRootExecutable(self, rootVersion, rootScript, arguments=None, logFile=None, systemConfig='ANY'):
    """Specifies ROOT version and executable (e.g. ./<rootScript>).

       Can optionally specify arguments to the script and a name for the output log file.

       Example usage:

       >>> job = LHCbJob()
       >>> j.setRootExecutable('5.18.00a','minexam')

       :param rootVersion: LHCb supported ROOT version
       :type rootVersion: string
       :param rootScript: Path to ROOT macro script
       :type rootScript: string
       :param arguments: Optional arguments for macro
       :type arguments: string or list
       :param logFile: Optional log file name
       :type logFile: string
    """
    rootType = 'bin'
    return self.__configureRootModule(rootVersion, rootScript, rootType, arguments, logFile, systemConfig)

  #############################################################################

  def __configureRootModule(self, rootVersion, rootScript, rootType, arguments, logFile, systemConfig):
    """ Internal function.

        Supports the root macro, python and executable wrapper functions.
    """
    kwargs = {'rootVersion': rootVersion, 'rootScript': rootScript, 'rootType': rootType,
              'arguments': arguments, 'logFile': logFile}
    if not isinstance(arguments, list):
      arguments = [arguments]

    for param in [rootVersion, rootScript, rootType, logFile]:
      if not isinstance(param, str):
        return self._reportError('Expected strings for Root application input parameters', __name__, **kwargs)

    if not os.path.exists(rootScript):
      return self._reportError('ROOT Script %s must exist locally' % (rootScript), __name__, **kwargs)

    self.addToInputSandbox.append(rootScript)

    rootName = os.path.basename(rootScript).replace('.', '')
    if logFile:
      logName = logFile
    else:
      logName = '%s_%s.log' % (rootName, rootVersion.replace('.', ''))

    self.stepCount += 1
    stepName = '%sStep%s' % (rootName, self.stepCount)

    modulesNameList = ['CreateDataFile',
                       'RootApplication',
                       'FileUsage',
                       'UserJobFinalization']
    parametersList = [('rootVersion', 'string', '', 'Root version'),
                      ('rootScript', 'string', '', 'Root script'),
                      ('rootType', 'string', '', 'Root type'),
                      ('arguments', 'list', [], 'Optional arguments for payload'),
                      ('applicationLog', 'string', '', 'Log file name'),
                      ('SystemConfig', 'string', '', 'CMT Config')]
    step = getStepDefinition(stepName,
                             modulesNameList=modulesNameList,
                             importLine="LHCbDIRAC.Workflow.Modules",
                             parametersList=parametersList)

    logPrefix = 'Step%s_' % (self.stepCount)
    logName = '%s%s' % (logPrefix, logName)
    self.addToOutputSandbox.append(logName)

    stepInstance = addStepToWorkflow(self.workflow, step, stepName)

    # Define Step and its variables
    stepInstance.setValue("rootVersion", rootVersion)
    stepInstance.setValue("rootType", rootType)
    stepInstance.setValue("rootScript", os.path.basename(rootScript))
    stepInstance.setValue("applicationLog", logName)
    stepInstance.setValue("SystemConfig", systemConfig)
    if arguments:
      stepInstance.setValue("arguments", arguments)

    return S_OK(stepInstance)

  #############################################################################

  def setAncestorDepth(self, depth):
    """Helper function.

       Level at which ancestor files are retrieved from the bookkeeping.

       For analysis jobs running over RDSTs the ancestor depth may be specified
       to ensure that the parent DIGI / DST files are staged before job execution.

       Example usage:

       >>> job = LHCbJob()
       >>> job.setAncestorDepth(2)

       :param depth: Ancestor depth
       :type depth: string or int

    """
    kwargs = {'depth': depth}
    description = 'Level at which ancestor files are retrieved from the bookkeeping'
    if isinstance(depth, str):
      try:
        self._addParameter(self.workflow, 'AncestorDepth', 'JDL', int(depth), description)
      except Exception as _x:
        return self._reportError('Expected integer for Ancestor Depth', __name__, **kwargs)
    elif isinstance(depth, int):
      self._addParameter(self.workflow, 'AncestorDepth', 'JDL', depth, description)
    else:
      return self._reportError('Expected integer for Ancestor Depth', __name__, **kwargs)
    return S_OK()

  #############################################################################

  def setInputDataType(self, inputDataType):
    """Explicitly set the input data type to be conveyed to Gaudi Applications.

       Default is DATA, e.g. for DST / RDST files.  Other options include:
        - MDF, for .raw files
        - ETC, for running on a public or private Event Tag Collections.

       Example usage:

       >>> job = LHCbJob()
       >>> job.setInputDataType('ETC')

       :param inputDataType: Input Data Type
       :type inputDataType: String

    """
    description = 'User specified input data type'
    if not isinstance(inputDataType, str):
      try:
        inputDataType = str(inputDataType)
      except TypeError as _x:
        return self._reportError('Expected string for input data type', __name__, **{'inputDataType': inputDataType})

    self.inputDataType = inputDataType
    self._addParameter(self.workflow, 'InputDataType', 'JDL', inputDataType, description)
    return S_OK()

  #############################################################################

  def setCondDBTags(self, condDict):
    """Under development. Helper function.

       Specify Conditions Database tags by by Logical File Name (LFN).

       The input dictionary is of the form: {<DB>:<TAG>} as in the example below.

       Example usage:

       >>> job = LHCbJob()
       >>> job.setCondDBTags({'DDDB':'DC06','LHCBCOND':'DC06'})

       :param condDict: CondDB tags
       :type condDict: Dict of DB, tag pairs
    """
    kwargs = {'condDict': condDict}
    if not isinstance(condDict, dict):
      return self._reportError('Expected dictionary for CondDB tags', __name__, **kwargs)

    conditions = []
    for db, tag in condDict.items():
      try:
        db = str(db)
        tag = str(tag)
        conditions.append('.'.join([db, tag]))
      except Exception as _x:
        return self._reportError('Expected string for conditions', __name__, **kwargs)

    condStr = ';'.join(conditions)
    description = 'List of CondDB tags'
    self._addParameter(self.workflow, 'CondDBTags', 'JDL', condStr, description)
    return S_OK()

  #############################################################################

  def setOutputData(self, lfns, OutputSE=None, OutputPath=None, replicate=None, filePrepend=None):
    """Helper function, used in preference to Job.setOutputData() for LHCb.

       For specifying user output data to be registered in Grid storage.

       Example usage:

       >>> job = Job()
       >>> job.setOutputData(['DVNtuple.root'])

       :param lfns: Output data file or files
       :type lfns: Single string or list of strings ['','']
       :param OutputSE: Optional parameter to specify the Storage
       :param OutputPath: Optional parameter to specify the Path in the Storage
       Element to store data or files, e.g. CERN-tape
       :type OutputSE: string or list
       :type OutputPath: string
    """
    # FIXME: the output data as specified here will be treated by the UserJobFinalization module
    # If we remove this method (which is totally similar to the Job() one, the output data will be
    # treated by the JobWrapper. So, can and maybe should be done, but have to pay attention
    kwargs = {'lfns': lfns, 'OutputSE': OutputSE, 'OutputPath': OutputPath}
    if isinstance(lfns, list) and lfns:
      outputDataStr = ';'.join(lfns)
      description = 'List of output data files'
      self._addParameter(self.workflow, 'UserOutputData', 'JDL', outputDataStr, description)
    elif isinstance(lfns, str):
      description = 'Output data file'
      self._addParameter(self.workflow, 'UserOutputData', 'JDL', lfns, description)
    else:
      return self._reportError('Expected file name string or list of file names for output data', **kwargs)

    if OutputSE:
      description = 'User specified Output SE'
      if isinstance(OutputSE, str):
        OutputSE = [OutputSE]
      elif not isinstance(OutputSE, list):
        return self._reportError('Expected string or list for OutputSE', **kwargs)
      OutputSE = ';'.join(OutputSE)
      self._addParameter(self.workflow, 'UserOutputSE', 'JDL', OutputSE, description)

    if OutputPath:
      description = 'User specified Output Path'
      if not isinstance(OutputPath, str):
        return self._reportError('Expected string for OutputPath', **kwargs)
      # Remove leading "/" that might cause problems with os.path.join
      while OutputPath[0] == '/':
        OutputPath = OutputPath[1:]
      self._addParameter(self.workflow, 'UserOutputPath', 'JDL', OutputPath, description)

    if replicate:
      self._addParameter(self.workflow, 'ReplicateUserOutputData', 'string', replicate, "Flag to replicate or not")

    if filePrepend:
      keepcharacters = ('.', '_')
      prependString = ''.join(c for c in filePrepend if c.isalnum() or c in keepcharacters).rstrip()
      self._addParameter(self.workflow, 'UserOutputLFNPrepend', 'string', prependString, "String to prepend")
      # no '_' in lfns are allowed
      for lfn in lfns:
        if '_' in lfn:
          self.log.error("When using filePrepend, file names can't contain '_'")
          return S_ERROR("When using filePrepend, file names can't contain '_'")

    return S_OK()

  #############################################################################

  def setExecutable(self, executable, arguments=None, logFile=None, systemConfig='ANY',
                    modulesNameList=None, parametersList=None):
    """Specifies executable script to run with optional arguments and log file
       for standard output.

       These can be either:

        - Submission of a python or shell script to DIRAC
           - Can be inline scripts e.g. C{'/bin/ls'}
           - Scripts as executables e.g. python or shell script file

       Example usage:

       >>> job = Job()
       >>> job.setExecutable('myScript.py')

       :param executable: Executable
       :type executable: string
       :param arguments: Optional arguments to executable
       :type arguments: string
       :param logFile: Optional log file name
       :type logFile: string
    """
    kwargs = {'executable': executable, 'arguments': arguments, 'applicationLog': logFile}
    if not isinstance(executable, str):
      return self._reportError('Expected strings for executable and arguments', **kwargs)

    if os.path.exists(executable):
      self.log.verbose('Found script executable file %s' % (executable))
      self.addToInputSandbox.append(executable)
      logName = '%s.log' % (os.path.basename(executable))
      moduleName = os.path.basename(executable)
    else:
      self.log.verbose('Found executable code')
      logName = 'CodeOutput.log'
      moduleName = 'CodeSegment'

    if logFile:
      if isinstance(logFile, str):
        logName = str(logFile)

    self.stepCount += 1

    moduleName = moduleName.replace('.', '')
    stepName = 'ScriptStep%s' % (self.stepCount)

    if not modulesNameList:
      modulesNameList = ['CreateDataFile',
                         'LHCbScript',
                         'FileUsage',
                         'UserJobFinalization']
    if not parametersList:
      parametersList = [('name', 'string', '', 'Name of executable'),
                        ('executable', 'string', '', 'Executable Script'),
                        ('arguments', 'string', '', 'Arguments for executable Script'),
                        ('applicationLog', 'string', '', 'Log file name'),
                        ('SystemConfig', 'string', '', 'CMT Config')]

    step = getStepDefinition(stepName,
                             modulesNameList=modulesNameList,
                             importLine="LHCbDIRAC.Workflow.Modules",
                             parametersList=parametersList)

    stepName = 'RunScriptStep%s' % (self.stepCount)
    logPrefix = 'Script%s_' % (self.stepCount)
    logName = '%s%s' % (logPrefix, logName)
    self.addToOutputSandbox.append(logName)

    stepInstance = addStepToWorkflow(self.workflow, step, stepName)

    stepInstance.setValue('name', moduleName)
    stepInstance.setValue('applicationLog', logName)
    stepInstance.setValue('executable', executable)
    stepInstance.setValue('SystemConfig', systemConfig)
    if arguments:
      stepInstance.setValue('arguments', arguments)

    return S_OK()

  #############################################################################
  def setPlatform(self, platform):
    """Developer function: sets the target platform, e.g. x86_64-slc5
       This platform is in the form of what it is returned by the dirac-architecture script)
    """
    kwargs = {'platform': platform}

    if not isinstance(platform, basestring):
      return self._reportError("Expected string for platform", **kwargs)

    if platform and platform.lower() != 'any':
      self._addParameter(self.workflow, 'Platform', 'JDL', platform, 'Platform ( Operating System )')
    return S_OK()

  def setDIRACPlatform(self):
    """ Looks inside the steps definition to find list of Configs, then translates it in a DIRAC platform
    """
    listOfConfigs = []
    for step_instance in self.workflow.step_instances:
      for parameter in step_instance.parameters:
        if parameter.name.lower() == 'systemconfig':
          if parameter.value:
            listOfConfigs.append(parameter.value)

    listOfConfigs = uniqueElements(listOfConfigs)
    listOfConfigs.sort(key=LooseVersion)

    if not listOfConfigs or listOfConfigs[0].lower() == 'any':
      # It was calling the base class setPlatfor('ANY') which just returns S_OK()
      return S_OK()

    try:
      platform = getPlatformFromLHCbConfig(listOfConfigs[0])
    except (ValueError, IndexError) as error:
      self.log.exception("Exception while getting platform, don't set it", lException=error)
      return S_OK()
    # At this stage this is never "AANY", so set it... although it is not clear it is of any use anywhere!
    self._addParameter(self.workflow, 'Platform', 'JDL', platform, 'Platform ( Operating System )')
    return S_OK()

  #############################################################################

  def setInputData(self, lfns, bkClient=None, runNumber=None, persistencyType=None):
    """ Add the input data and the run number, if available
    """

    if not lfns:
      self.log.warn("no lfns passed in setInputData, was that intentional?")
      return S_OK("Nothing to do")

    res = Job.setInputData(self, lfns)
    if not res['OK']:
      return res

    if not runNumber or not persistencyType:
      if not bkClient:
        bkClient = BookkeepingClient()

    if not runNumber:
      res = bkClient.getFileMetadata(lfns)
      if not res['OK']:
        return res

      runNumbers = []
      for fileMeta in res['Value']['Successful'].values():
        try:
          if fileMeta['RunNumber'] not in runNumbers and fileMeta['RunNumber'] is not None:
            runNumbers.append(fileMeta['RunNumber'])
        except KeyError:
          continue

      if len(runNumbers) > 1:
        runNumber = 'Multiple'
      elif len(runNumbers) == 1:
        runNumber = str(runNumbers[0])
      else:
        runNumber = 'Unknown'

    if not persistencyType:

      res = bkClient.getFileTypeVersion(lfns)
      if not res['OK']:
        return res

      typeVersions = res['Value']

      if not typeVersions:
        self.log.verbose('The requested files do not exist in the BKK')
        typeVersion = ''

      else:
        self.log.verbose('Found file types %s for LFNs: %s' % (typeVersions.values(), typeVersions.keys()))
        typeVersionsList = []
        for tv in typeVersions.values():
          if tv not in typeVersionsList:
            typeVersionsList.append(tv)

        if len(typeVersionsList) == 1:
          typeVersion = typeVersionsList[0]
        else:
          typeVersion = ''

    self._addParameter(self.workflow, 'runNumber', 'JDL', runNumber, 'Input run number')
    self._addParameter(self.workflow, 'persistency', 'String', typeVersion, 'Persistency type of the inputs')

    return S_OK()

  #############################################################################

  def setRunMetadata(self, runMetadataDict):
    """ set the run metadata
    """

    self._addParameter(self.workflow, 'runMetadata', 'String', str(runMetadataDict), 'Input run metadata')
    return S_OK()

  #############################################################################

  def runLocal(self, diracLHCb=None):
    """ The DiracLHCb (API) object is for local submission.
        A BKKClient might be needed.
        First, adds Ancestors (in any) to the InputData.
    """

    if diracLHCb is not None:
      diracLHCb = diracLHCb
    else:
      diracLHCb = DiracLHCb()

    return diracLHCb.submitJob(self, mode='local')

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
