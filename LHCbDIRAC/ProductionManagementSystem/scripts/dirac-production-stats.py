#!/usr/bin/env python
""" Get statistics on productions related to a given processing pass
"""

__RCSID__ = "$Id$"

from LHCbDIRAC.ProductionManagementSystem.Client.ProcessingProgress import ProcessingProgress, HTMLProgressTable

if __name__ == "__main__":
  import DIRAC
  from DIRAC.Core.Base import Script
  from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
  from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery

  dmScript = DMScript()
  dmScript.registerBKSwitches()
  Script.registerSwitch( '', 'Conditions=', '   comma separated list of DataTakingConditions' )
  Script.registerSwitch( '', 'HTML=', '   <file> : Output in html format to <file>' )
  Script.registerSwitch( '', 'NoPrint', '   No printout' )

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] [<LFN>] [<LFN>...]' % Script.scriptName, ] ) )

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )
  Script.parseCommandLine( ignoreErrors = False )

  switches = Script.getUnprocessedSwitches()
  outputHTML = None
  printResult = True
  daqConditions = None
  processingPass = None
  for opt, val in switches:
    if opt == "Conditions":
      daqConditions = val.split( ',' )
    elif opt == "ProcessingPass":
      processingPass = val
    elif opt == "HTML":
      outputHTML = val
    elif opt == "NoPrint":
      printResult = False

  bkQuery = dmScript.getBKQuery( visible = 'All' )
  if not bkQuery.getQueryDict():
    Script.showHelp()
    DIRAC.exit( 2 )
  if daqConditions:
    bkQueries = []
    for cond in daqConditions:
      bkQueries.append( BKQuery( bkQuery.setConditions( cond ) ) )
  else:
    bkQueries = [bkQuery]

  statCollector = ProcessingProgress()
  printOutput = ''
  htmlOutput = ''
  summaryProdStats = []
  if outputHTML:
    htmlTable = HTMLProgressTable( bkQuery.getProcessingPass() )
  for bkQuery in bkQueries:
    prodStats = statCollector.getFullStats( bkQuery, printResult = printResult )
    summaryProdStats.append( prodStats )

    if printResult:
      printOutput += statCollector.outputResults( bkQuery.getConditions(), bkQuery.getProcessingPass(), prodStats )

    if outputHTML:
      htmlTable.writeHTML( bkQuery.getConditions(), prodStats )

  if printResult:
    print printOutput
  if outputHTML:
    htmlSummary = ''
    if len( summaryProdStats ) > 1:
      htmlTable.writeHTMLSummary( summaryProdStats )
    try:
      f = open( outputHTML, 'w' )
      f.write( "<head>\n<title>Progress of %s</title>\n</title>\n" % bkQueries[0].getProcessingPass() )
      f.write( str( htmlTable.getTable() ) )
      f.close()
      print "Successfully wrote HTML file", outputHTML
    except:
      print "Failed to write HTML file", outputHTML
