########################################################################
# $Id: ProductionOptions.py,v 1.1 2009/08/06 14:10:23 paterson Exp $
########################################################################
""" Production options is a utility to return options for projects based on
    current LHCb software versions.  This is used by the production API to
    create production workflows but also provides lists of options files for
    test jobs (to do).
"""

__RCSID__ = "$Id: ProductionOptions.py,v 1.1 2009/08/06 14:10:23 paterson Exp $"

from DIRAC import S_OK, S_ERROR, gLogger, gConfig

import string

#############################################################################
def getOptions(appName,appType,extraOpts=None,inputType=None,histogram='@{applicationName}_@{STEP_ID}_Hist.root'):
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
#    options.append("DaVinci().HistogramFile = \"%s\"" % (histogram))
    # If we want to generate an FETC for the first step of the stripping workflow
    if appType.lower()=='fetc':
      options.append("DaVinci().ETCFile = \"@{outputData}\"")
    elif appType.lower() == 'dst' and inputType!='dst': #e.g. not stripping
      options.append("OutputStream(\"DstWriter\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"")
    elif appType.lower() == 'dst' and inputType=='dst': #e.g. stripping
      options.append('from StrippingConf.Configuration import StrippingConf')
      options.append('StrippingConf().StreamFile["BExclusive"] = \'@{outputData}\'')
      options.append('StrippingConf().StreamFile["Topological"] = \'@{outputData}\'')
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

  options.append(dddbOpt)
  options.append(conddbOpt)
  options.append(evtOpt)
  return options

#############################################################################
def printOptions():
  """ A simple method to print all currently used project options in a nicely
      formatted way.
  """
  appDict = {}
  appDict['Gauss']=['sim;None']
  appDict['Boole']=['mdf;sim','digi;sim']
  appDict['Brunel']=['dst;digi','rdst;mdf','dst;fetc','xdst;digi']
  appDict['DaVinci']=['dst;rdst','dst;dst','fetc;rdst']
  appDict['Merge']=['dst;dst']

  apps = appDict.keys()
  apps.sort()
  for app in apps:
    dataFlows = appDict[app]
    for data in dataFlows:
      appType = data.split(';')[0]
      inputFileType = data.split(';')[1]
      print '\n========> %s ( %s -> %s )' %(app,inputFileType.upper(),appType.upper())
      _printOpts(getOptions(app,appType,inputType=inputFileType))

#############################################################################
def _printOpts(opts):
  """ Internal function to control formatting.
  """
  for opt in opts:
    print opt

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#