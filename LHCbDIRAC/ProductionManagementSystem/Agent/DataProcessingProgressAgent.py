''' DataProcessingProgressAgent
'''

import datetime
import os
import time
import shutil

from DIRAC                         import S_OK, gConfig, gLogger
from DIRAC.Core.Base.AgentModule   import AgentModule

from LHCbDIRAC.ProductionManagementSystem.Client.ProcessingProgress import ProcessingProgress, HTMLProgressTable

__RCSID__ = "$Id$"

AGENT_NAME = 'ProductionManagement/DataProcessingProgressAgent'

class DataProcessingProgressAgent( AgentModule ):

  #############################################################################
  def initialize( self ):
    """Sets default values.
    """
    from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery

    self.pollingTime = self.am_getOption( 'PollingTime', 6 * 60 * 60 )
    self.printResult = self.am_getOption( 'Verbose', False )
    if self.printResult:
      gLogger.setLevel( 'VERBOSE' )
    self.workDirectory = self.am_getWorkDirectory()
    self.statCollector = ProcessingProgress( os.path.join( self.workDirectory, "dirac-production-stats.pkl" ) )
    self.uploadDirectory = self.am_getOption( 'UploadDirectory', None )

    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/ProductionManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'ProductionManager' )

    # Get back the loop number
    self.cacheFile = os.path.join( self.workDirectory, "cacheFile" )
    try:
      cfile = open( self.cacheFile, 'r' )
      self.iterationNumber = int( cfile.read() )
      cfile.close()
    except:
      self.iterationNumber = 0

    # Get the list of processing passes
    reportList = self.am_getSection( 'ProgressReports' )
    self.progressReports = {}
    for reportName in  reportList:
      optionPath = os.path.join( 'ProgressReports', reportName )
      processingPasses = self.am_getOption( os.path.join( optionPath, 'ProcessingPass' ), [] )
      conditions = self.am_getOption( os.path.join( optionPath, 'ConditionDescription' ), [] )
      bkConfig = self.am_getOption( os.path.join( optionPath, 'BKConfig' ), '/LHCb/Collision11' )
      eventType = self.am_getOption( os.path.join( optionPath, 'EventType' ), 90000000 )
      fileType = self.am_getOption( os.path.join( optionPath, 'FileType' ), 'BHADRON.DST' )
      report = { 'ConditionDescription' :conditions }
      report['Frequency'] = self.am_getOption( os.path.join( optionPath, 'Frequency' ), 1 )
      report['HTMLFile'] = self.am_getOption( os.path.join( optionPath, 'HTMLFile' ), reportName.replace( '.', '' ) + '-Progress' + os.path.extsep + 'htm' )
      report['BKQuery'] = []
      report['ClearCache'] = self.am_getOption( os.path.join( optionPath, 'ClearCache' ), [] )
      for processingPass in processingPasses:
        bkPath = os.path.join( bkConfig, '*/Real Data', processingPass, str( eventType ), fileType )
        bkQuery = BKQuery( bkPath, visible=False )
        if not bkQuery:
          self.log.error( "Cannot build bkQuery for %s" % bkPath )
        else:
          report['BKQuery'].append( bkQuery )
      self.progressReports[reportName.replace( '.', '/' )] = report

    self.log.info( "List of progress reports:" )
    self.previousProdStats = {}
    for reportName in self.progressReports:
      printStr = '%s: ' % reportName
      for key, value in self.progressReports[reportName].items():
        if key != 'BKQuery':
          printStr += ", %s : %s" % ( key, str( value ) )
        else:
          printStr += ", %s :" % key
          for bkQuery in self.progressReports[reportName]['BKQuery']:
            printStr += " - %s" % bkQuery.getPath()
      self.log.info( printStr )
    return S_OK()

  def execute( self ):
    self.log.info( "Now getting progress of processing (iteration %d)..." % self.iterationNumber )

    for reportName in sorted( self.progressReports.keys() ):
      htmlTable = HTMLProgressTable( reportName.replace( '.', '/' ) )
      reportLen = len( reportName ) + 4
      self.log.info( "\n%s\n* %s *\n%s" % ( reportLen * '*', reportName, reportLen * '*' ) )
      report = self.progressReports[reportName]
      # Skip all by each "frequency" loop
      if report['Frequency'] == 0 or ( self.iterationNumber % report['Frequency'] ) != 0:
        self.log.info( "Skipping this iteration for %s" % reportName )
        continue
      self.statCollector.setClearCache( report['ClearCache'] )
      summaryProdStats = []
      printOutput = ''
      outputHTML = os.path.join( self.workDirectory, report['HTMLFile'] )
      for cond in report['ConditionDescription']:
        prodStats = 4 * [None]
        for bkQuery in report['BKQuery']:
          bkPath = bkQuery.getPath()
          if not bkQuery.getQueryDict():
            continue
          self.log.info( "\n=========================\nBookkeeping query %s\n=========================" % bkPath.replace( '*', cond ) )
          bkQuery.setConditions( cond )
          stats = self.statCollector.getFullStats( bkQuery, printResult=self.printResult )
          processingPass = bkQuery.getProcessingPass().split( '/' )
          for ind in range( len( prodStats ) ):
            if not prodStats[ind]:
              prodStats[ind] = stats[ind]
            else:
              prodStats[ind] += stats[ind]
        summaryProdStats.append( prodStats )
        if self.printResult:
          printOutput += self.statCollector.outputResults( cond, reportName.replace( '.', '/' ), prodStats )
        htmlTable.writeHTML( cond, prodStats )

      if self.printResult:
        lines = printOutput.split( '\n' )
        for line in lines:
          self.log.info( line )

      if len( summaryProdStats ) > 1:
        htmlTable.writeHTMLSummary( summaryProdStats )
      if reportName not in self.previousProdStats:
        x = self.statCollector.getPreviousStats( reportName )
        if x:
          self.previousProdStats[reportName] = x
      if reportName in self.previousProdStats:
        htmlTable.writeHTMLDifference( summaryProdStats, self.previousProdStats[reportName] )
      else:
        print reportName, 'not in previous stats'
      self.previousProdStats[reportName] = { "Time":time.ctime( time.time() ), "ProdStats":summaryProdStats}
      self.statCollector.setPreviousStats( reportName, self.previousProdStats[reportName] )
      try:
        fOpen = open( outputHTML, 'w' )
        fOpen.write( "<head>\n<title>Progress of %s</title>\n</title>\n" % bkQuery.getProcessingPass() )
        fOpen.write( str( htmlTable.getTable() ) )
        fOpen.close()
        print "Successfully wrote HTML file", outputHTML
        self.uploadHTML( outputHTML )
      except:
        print "Failed to write HTML file", outputHTML

    # Save the loop number
    self.iterationNumber += 1
    fOpen = open( self.cacheFile, 'w' )
    fOpen.write( str( self.iterationNumber ) )
    fOpen.close()
    return S_OK()

  def uploadHTML( self, htmlFile ):
    if not self.uploadDirectory:
      return
    try:
      today = str( datetime.datetime.today() ).split()[0]
      uploadDirBase = os.path.join( self.uploadDirectory, 'Daily' )
      if not os.path.exists( uploadDirBase ):
        os.mkdir( uploadDirBase )
      uploadDirBase = os.path.join( uploadDirBase, today )
      if not os.path.exists( uploadDirBase ):
        os.mkdir( uploadDirBase )
      i = 0
      while True:
        uploadDir = os.path.join( uploadDirBase, str( i ) )
        if not os.path.exists( uploadDir ):
          os.mkdir( uploadDir )
        uploadedFile = os.path.join( uploadDir, os.path.basename( htmlFile ) )
        if not os.path.exists( uploadedFile ):
          break
        i += 1
      shutil.copy( htmlFile, uploadedFile )
      remoteLink = os.path.join( self.uploadDirectory, os.path.basename( htmlFile ) )
      if os.path.exists( remoteLink ):
        os.remove( remoteLink )
      os.symlink( uploadedFile, remoteLink )
      print htmlFile, "copied to", uploadedFile, "and link set at", remoteLink
    except:
      print "Failed to upload", htmlFile, "to", self.uploadDirectory

  def am_getSection( self, section ):
    res = gConfig.getSections( "%s/%s" % ( self.am_getModuleParam( 'section' ), section ) )
    if res['OK']:
      return res['Value']
    else:
      return []

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
