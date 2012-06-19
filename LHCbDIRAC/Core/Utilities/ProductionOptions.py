""" Production options is a utility to return options for projects based on
    current LHCb software versions.  This is used by the production API to
    create production workflows but also provides lists of options files for
    test jobs.
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, gLogger

gLogger = gLogger.getSubLogger( 'ProductionOptions' )

#############################################################################

def getModuleOptions( applicationName, numberOfEvents, inputDataOptions, extraOptions = '',
                      runNumber = 0, firstEventNumber = 1, jobType = '' ):
  """ Return the standard options for a Gaudi application project to be used at run time
      by the workflow modules.  The input data options field is a python list (output of
      getInputDataOptions() below). The runNumber and firstEventNumber only apply in the Gauss case
      and when the job type is not 'user'.
  """
  optionsLines = []
  optionsLines.append( '\n\n#//////////////////////////////////////////////////////' )
  optionsLines.append( '# Dynamically generated options in a production or analysis job\n' )
  if applicationName.lower() == 'davinci' or applicationName.lower() == 'lhcb':
    optionsLines.append( 'from Gaudi.Configuration import *' )
  else:
    optionsLines.append( 'from %s.Configuration import *' % applicationName )

  if extraOptions:
    for opt in extraOptions.split( ';' ):
      if opt:
        optionsLines.append( opt )

  if inputDataOptions:
    optionsLines += inputDataOptions

  if applicationName.lower() == 'gauss' and not jobType.lower() == 'user':
    optionsLines.append( "GaussGen = GenInit(\"GaussGen\")" )
    optionsLines.append( "GaussGen.RunNumber = %s" % ( runNumber ) )
    optionsLines.append( "GaussGen.FirstEventNumber = %s" % ( firstEventNumber ) )

  if numberOfEvents != 0:
    optionsLines.append( "ApplicationMgr().EvtMax = %s" % ( numberOfEvents ) )

  finalLines = '\n'.join( optionsLines ) + '\n'
  return S_OK( finalLines )

#############################################################################

def getDataOptions( applicationName, inputDataList, inputDataType, poolXMLCatalogName ):
  """Given a list of input data and a specified input data type this function will
     return the correctly formatted EventSelector options for Gaudi applications
     specified by name.  The options are returned as a python list.
  """
  options = []
  if inputDataList:
    gLogger.info( 'Formatting options for %s: %s LFN(s) of type %s' % ( applicationName, len( inputDataList ), inputDataType ) )

    inputDataOpt = getEventSelectorInput( inputDataList, inputDataType )['Value']
    evtSelOpt = """EventSelector().Input=[%s];\n""" % ( inputDataOpt )
    options.append( evtSelOpt )

  poolOpt = """\nFileCatalog().Catalogs= ["xmlcatalog_file:%s"]\n""" % ( poolXMLCatalogName )
  options.append( poolOpt )
  return S_OK( options )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
