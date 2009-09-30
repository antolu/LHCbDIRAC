########################################################################
# $Id: ProductionOptions.py,v 1.4 2009/09/30 15:55:51 paterson Exp $
########################################################################
""" Production options is a utility to return options for projects based on
    current LHCb software versions.  This is used by the production API to
    create production workflows but also provides lists of options files for
    test jobs.
"""

__RCSID__ = "$Id: ProductionOptions.py,v 1.4 2009/09/30 15:55:51 paterson Exp $"

from DIRAC import S_OK, S_ERROR, gLogger, gConfig

import string

#############################################################################
def getOptions(appName,appType,extraOpts=None,inputType=None,histogram='@{applicationName}_@{STEP_ID}_Hist.root',production=True):
  """ Simple function to create the default options for a given project name.

      Assumes CondDB tags and event max are required.
  """
  options = []
  #General options
  dddbOpt = "LHCbApp().DDDBtag = \"@{DDDBTag}\""
  conddbOpt = "LHCbApp().CondDBtag = \"@{CondDBTag}\""
  evtOpt = "ApplicationMgr().EvtMax = @{numberOfEvents}"
  options.append("MessageSvc().Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc().timeFormat = '%Y-%m-%d %H:%M:%S UTC'")
  options.append("HistogramPersistencySvc().OutputFile = \"%s\"" % (histogram))
  if appName.lower()=='gauss':
    options.append("OutputStream(\"GaussTape\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"")
  elif appName.lower()=='boole':
    if appType.lower()=='mdf':
      options.append("OutputStream(\"RawWriter\").Output = \"DATAFILE=\'PFN:@{outputData}\' SVC=\'LHCb::RawDataCnvSvc\' OPT=\'RECREATE\'\"")
      options.append("OutputStream(\"RawWriter\").OutputLevel = INFO")
    else:
      options.append("OutputStream(\"DigiWriter\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"")
  elif appName.lower()=='brunel':
    options.append("#include \"$BRUNELOPTS/SuppressWarnings.opts\"")
    options.append("OutputStream(\"DstWriter\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"")
    options.append("from Configurables import Brunel")
    if appType.lower()=='xdst':
      options.append("Brunel().OutputType = 'XDST'")
    elif appType.lower()=='dst':
      options.append("Brunel().OutputType = 'DST'")
    elif appType.lower()=='rdst':
      options.append("Brunel().OutputType = 'RDST'")

  elif appName.lower()=='davinci':
    options.append('from DaVinci.Configuration import *')
#    options.append('DaVinci().EvtMax=@{numberOfEvents}')
    #for the stripping some options override the above Gaudi level setting
    options.append("DaVinci().HistogramFile = \"%s\"" % (histogram))
    # If we want to generate an FETC for the first step of the stripping workflow
    if appType.lower()=='fetc':
      options.append("DaVinci().ETCFile = \"@{outputData}\"")
    elif appType.lower() == 'dst' and inputType!='dst': #e.g. not stripping
      options.append("OutputStream(\"DstWriter\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"")
    elif appType.lower() == 'dst' and inputType=='dst': #e.g. stripping
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

  else:
    self.log.warn('No specific options found for project %s' %appName)

  if extraOpts:
    options.append(extraOpts)

  if production:
    options.append(dddbOpt)
    options.append(conddbOpt)
    options.append(evtOpt)

  return options

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
  """ Function to retrieve arbitrary working options for running a test job. Not
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