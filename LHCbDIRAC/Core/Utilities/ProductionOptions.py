########################################################################
# $Id$
########################################################################
""" Production options is a utility to return options for projects based on
    current LHCb software versions.  This is used by the production API to
    create production workflows but also provides lists of options files for
    test jobs.
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger, gConfig

import string,re

gLogger = gLogger.getSubLogger('ProductionOptions')

#############################################################################
def getOptions(appName,appType,extraOpts=None,inputType=None,histogram='@{applicationName}_@{STEP_ID}_Hist.root',condDB='@{CondDBTag}',ddDB='@{DDDBTag}',production=True):
  """ Simple function to create the default options for a given project name.

      Assumes CondDB tags and event max are required.
  """
  options = []

  #To resolve MC / Upgrade case
  if condDB.lower() == 'global':
    condDB='@{CondDBTag}'
  if ddDB.lower() == 'global':
    ddDB = '@{DDDBTag}'

  #General options
  dddbOpt = "LHCbApp().DDDBtag = \"%s\"" %(ddDB)
  conddbOpt = "LHCbApp().CondDBtag = \"%s\"" %(condDB)
  evtOpt = "ApplicationMgr().EvtMax = @{numberOfEvents}"
#  options.append("MessageSvc().Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc().timeFormat = '%Y-%m-%d %H:%M:%S UTC'")
  options.append("HistogramPersistencySvc().OutputFile = \"%s\"" % (histogram))
  if appName.lower()=='gauss':
    options.append("OutputStream(\"GaussTape\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"")
  elif appName.lower()=='boole':
    if appType.lower()=='mdf':
      options.append("OutputStream(\"RawWriter\").Output = \"DATAFILE=\'PFN:@{outputData}\' SVC=\'LHCb::RawDataCnvSvc\' OPT=\'RECREATE\'\"")
      options.append("OutputStream(\"RawWriter\").OutputLevel = INFO")
    elif appType.lower()=='xdigi':
      #no explicit output type like Brunel for Boole...
      options.append('from Configurables import Boole')    
      options.append('Boole().DigiType = "Extended"')
      options.append("OutputStream(\"DigiWriter\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"")        
    else:
      options.append("OutputStream(\"DigiWriter\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"")
  elif appName.lower()=='brunel':
    options.append("Brunel().NoWarnings = True")
    options.append("OutputStream(\"DstWriter\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"")
    options.append("from Configurables import Brunel")
    if appType.lower()=='xdst':
      options.append("Brunel().OutputType = 'XDST'")
    elif appType.lower()=='dst':
      options.append("Brunel().OutputType = 'DST'")
    elif appType.lower()=='rdst':
      options.append("Brunel().OutputType = 'RDST'")
    elif appType.lower()=='sdst':
      options.append("Brunel().OutputType = 'SDST'")  
#    options.append("from Configurables import RecInit")
#    options.append('RecInit("Brunel").PrintFreq = 100')

  elif appName.lower()=='davinci':
    options.append('from DaVinci.Configuration import *')
    #DaVinci AppConfig options persistently override the EvtMax...
    options.append('DaVinci().EvtMax=@{numberOfEvents}')
    #for the stripping some options override the above Gaudi level setting
    options.append("DaVinci().HistogramFile = \"%s\"" % (histogram))
    # If we want to generate an FETC for the first step of the stripping workflow
    if appType.lower()=='fetc':
      options.append("DaVinci().ETCFile = \"@{outputData}\"")
    elif appType.lower() == 'dst' and inputType not in ['sdst','dst']: #e.g. not stripping
      options.append("OutputStream(\"DstWriter\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"")
    elif appType.lower() == 'dst' and inputType in ['sdst','dst']: #e.g. stripping
      options.append('from Configurables import SelDSTWriter')
      options.append('SelDSTWriter.OutputFileSuffix = \'@{STEP_ID}\'')
#      options.append('from StrippingConf.Configuration import StrippingConf')
#      options.append('StrippingConf().StreamFile["BExclusive"] = \'@{outputData}\'')
#      options.append('StrippingConf().StreamFile["Topological"] = \'@{outputData}\'')
    elif appType.lower() == 'davincihist':
      options.append('from Configurables import InputCopyStream')
      options.append('InputCopyStream().Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'REC\'\"')
      options.append('DaVinci().MoniSequence.append(InputCopyStream())')

  elif appName.lower()=='merge':
    #options.append('EventSelector.PrintFreq = 200')
    options.append('OutputStream(\"InputCopyStream\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"')
    return options
  elif appName.lower()=='moore':   
    options.append('from Configurables import Moore')
    options.append('Moore().outputFile = \'@{outputData}\'')
    #Note this is the only case where the DB Tags are overwritten
    dddbOpt = "Moore().DDDBtag = \"%s\"" %(ddDB)
    conddbOpt = "Moore().CondDBtag = \"%s\"" %(condDB) 
  else:
    gLogger.warn('No specific options found for project %s' %appName)

  postConfig = 'def forceOptions():\n  MessageSvc().Format = "%u % F%18W%S%7W%R%T %0W%M"\n  MessageSvc().timeFormat = "%Y-%m-%d %H:%M:%S UTC"\nappendPostConfigAction(forceOptions)'

  if extraOpts:
    options.append(extraOpts)

  if production:
    options.append(dddbOpt)
    options.append(conddbOpt)
    options.append(evtOpt)

  options.append(postConfig)
  return options

#############################################################################
def getModuleOptions(applicationName,numberOfEvents,inputDataOptions,extraOptions='',runNumber=0,firstEventNumber=1,jobType=''):
  """ Return the standard options for a Gaudi application project to be used at run time
      by the workflow modules.  The input data options field is a python list (output of 
      getInputDataOptions() below). The runNumber and firstEventNumber only apply in the Gauss case 
      and when the job type is not 'user'.
  """
  optionsLines=[]
  optionsLines.append('\n\n#//////////////////////////////////////////////////////')
  optionsLines.append('# Dynamically generated options in a production or analysis job\n')
  if applicationName.lower()=='davinci' or applicationName.lower()=='lhcb':
    optionsLines.append('from Gaudi.Configuration import *')
  else:
    optionsLines.append('from %s.Configuration import *' %applicationName)

  if extraOptions:
    for opt in extraOptions.split(';'):
      if opt:
        optionsLines.append(opt)
        
  if inputDataOptions:
    optionsLines+=inputDataOptions
    
  if applicationName == 'Gauss' and not jobType.lower()=='user':
    optionsLines.append("GaussGen = GenInit(\"GaussGen\")")
    optionsLines.append("GaussGen.RunNumber = %s" %(runNumber))
    optionsLines.append("GaussGen.FirstEventNumber = %s" %(firstEventNumber))

  if numberOfEvents != 0:
      optionsLines.append("ApplicationMgr().EvtMax = %s" %(numberOfEvents))

  finalLines = string.join(optionsLines,'\n')+'\n'
  return S_OK(finalLines)

#############################################################################
def getDataOptions(applicationName,inputDataList,inputDataType,poolXMLCatalogName):
  """Given a list of input data and a specified input data type this function will
     return the correctly formatted EventSelector options for Gaudi applications
     specified by name.  The options are returned as a python list.
  """
  options = []
  if inputDataList:
    gLogger.info('Formatting options for %s: %s LFN(s) of type %s' %(applicationName,len(inputDataList),inputDataType))  

    inputDataOpt = getEventSelectorInput(inputDataList,inputDataType)['Value']
    evtSelOpt = """EventSelector().Input=[%s];\n""" %(inputDataOpt)
    options.append(evtSelOpt)

    if applicationName.lower()=='moore':
      options = []
      options.append('from Configurables import Moore')
      mooreInput = ['LFN:%s' %i.replace('lfn:','').replace('LFN:','') for i in inputDataList]        
      options.append("Moore().inputFiles = %s" %(mooreInput))

  poolOpt = """\nFileCatalog().Catalogs= ["xmlcatalog_file:%s"]\n""" %(poolXMLCatalogName)
  options.append(poolOpt)
  return S_OK(options)

#############################################################################
def getEventSelectorInput(inputDataList,inputDataType):
  """ Returns the correctly formatted event selector options for accessing input
      data using Gaudi applications.
  """
  inputDataFiles = []
  for lfn in inputDataList:
    lfn = lfn.replace('LFN:','').replace('lfn:','')
    if inputDataType == "MDF":
      inputDataFiles.append(""" "DATAFILE='LFN:%s' SVC='LHCb::MDFSelector'", """ %(lfn))
    elif inputDataType in ("ETC","SETC","FETC"):
      inputDataFiles.append(""" "COLLECTION='TagCreator/EventTuple' DATAFILE='LFN:%s' TYP='POOL_ROOT' SEL='(StrippingGlobal==1)' OPT='READ'", """%(lfn))
    elif inputDataType == 'RDST':
      if re.search('rdst$',lfn):
        inputDataFiles.append(""" "DATAFILE='LFN:%s' TYP='POOL_ROOTTREE' OPT='READ'", """ %(lfn))
      else:
        gLogger.info('Ignoring file %s for %s step with input data type %s' %(lfn,applicationName,inputDataType))
    else:
      inputDataFiles.append(""" "DATAFILE='LFN:%s' TYP='POOL_ROOTTREE' OPT='READ'", """ %(lfn))

  inputDataOpt = string.join(inputDataFiles,'\n')[:-2]
  return S_OK(inputDataOpt)

#############################################################################
def printOptions(project='',printOutput=True):
  """ A simple method to print all currently used project options in a nicely
      formatted way.  This also allows to restrict printing to options for a
      given project if desired. The list of projects is:
      Gauss, Boole, Brunel, DaVinci and Merge (case insensitive).
  """
  appDict = {}
  appDict['Gauss']=['sim;None']
  appDict['Boole']=['mdf;sim','digi;sim']
  appDict['Brunel']=['dst;digi','rdst;mdf','dst;fetc','xdst;digi']
  appDict['DaVinci']=['dst;rdst','dst;dst','fetc;rdst']
  appDict['Merge']=['dst;dst']
  apps = appDict.keys()
  apps.sort()

  for i in apps:
    if project.lower()==i.lower():
      project = i

  finalApps = {}
  if appDict.has_key(project):
    apps = [project]

  for app in apps:
    dataFlows = appDict[app]
    for data in dataFlows:
      appType = data.split(';')[0]
      inputFileType = data.split(';')[1]
      print '\n========> %s ( %s -> %s )' %(app,inputFileType.upper(),appType.upper())
      opts = getOptions(app,appType,inputType=inputFileType)
      for opt in opts:
        print opt

#############################################################################
def getTestOptions(projectName):
  """ Under development. Function to retrieve arbitrary working options for running a test job. Not
      all processing cases are supported, this is intended only to return working options
      for a simple test job as a computing exercise only.
  """
  testOpts = {}
  testOpts['gauss']='$GAUSSROOT/tests/options/testGauss-gen-10evts-defaults.py'
  testOpts['boole']='$BOOLEROOT/tests/options/testBoole-defaults.py'
  testOpts['brunel']='$BRUNELROOT/tests/options/testBrunel-defaults.py'
  testOpts['davinci']='$DAVINCIROOT/options/DaVinci-MC09.py'

  apps = testOpts.keys()

  if not projectName.lower() in apps:
    return S_ERROR('Could not find test options for project %s' %projectName)

  return S_OK(testOpts[projectName.lower()])

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#