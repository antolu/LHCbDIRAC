# '''
# MergingForDQAgent automatize some operations done by the data quality crew. It looks
# for Runs that have not been flagged (i.e. that are UNCHECKED). If it finds out some
# of them it performs a merging of the BRUNELHIST and DAVINCIHIST root files.
# The main steps are the following:
#
# - The initialize method gather informations from the CS
#
# - Retrieving the UNCHECKED (or OK) DAVINCIHISTs and BRUNELHISTs by run.
#
# - The files are saved locally and merged in three steps. The first step consists of
#   merging separately the two kinds of histograms in groups of 50. This grouping is
#   made because the merging is CPU consuming and if you attempt to merge a lot of file in a row
#   you probably stuck the machine. The output files of this first step are then merged as well.
#   Finally DAVINCI and BRUNEL histograms are then put together in a single file.
#
# - The results (data an logs) are uploaded in the bookkeeping.
#
# Systems/DataManagement/Production/Agents/
#
#         MergingForDQAgent
#         {
#           ExeDir = /opt/dirac/pro/Merge
#           homeDir = /tmp/dirac/MergingForDQ
#           senderAddress = someaddress
#           mailAddress = someaddress
#           applicationName = LCGCMT ROOT
#           eventType = FULL
#           cfgNameList = LHCb
#           cfgVersionList = Collision11
#           dqFlag = UNCHECKED
#           evtTypedict = EXPRESS:91000000, FULL:90000000
#           histTypeList = BRUNELHIST,DAVINCIHIST
#           testMode = False
#           specialMode = False
#           specialRuns =
#          }
#
# '''
#
# import datetime, time, os, string, glob, shutil
# from  xml.dom import minidom
#
# import DIRAC
#
# from DIRAC                                                      import S_OK, S_ERROR, gLogger
# from DIRAC.Core.Base.AgentModule                                import AgentModule
# from DIRAC.FrameworkSystem.Client.NotificationClient            import NotificationClient
# from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
# from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient       import BookkeepingClient
# from LHCbDIRAC.Core.Utilities.ProductionEnvironment             import getProjectEnvironment
# from LHCbDIRAC.DataManagementSystem.Utilities.MergeForDQ        import getRuns, getProductionId, buildLFNs, \
#   mergeRun, finalization
#
#
# AGENT_NAME = 'DataManagement/MergingForDQAgent'
# __RCSID__ = "$Id$"
#
# class MergingForDQAgent( AgentModule ):
#
#   def __init__( self, agentName, loadName, baseAgentName = False, properties = dict() ):
#     ''' c'tor
#
#     :param self: self reference
#     :param str agentName: name of agent
#     :param bool baseAgentName: whatever
#     :param dict properties: whatever else
#     '''
#     AgentModule.__init__( self, agentName, loadName, baseAgentName, properties )
#
#     self.systemConfiguration = 'x86_64-slc5-gcc43-opt'
#
#     self.logFileName = ''
#     self.logFile = ''
#
#     self.bkClient = BookkeepingClient()
#     self.transClient = TransformationClient()
#
#     self.applicationName = self.am_getOption ( "applicationName" )
#     self.homeDir = self.am_getOption( "homeDir" )
#     self.mergeExeDir = self.am_getOption( "ExeDir" )
#     self.senderAddress = self.am_getOption( "senderAddress" )
#     self.mailAddress = self.am_getOption( "mailAddress" )
#     self.thisEventType = self.am_getOption( "eventType" )
#     self.cfgVersion = self.am_getOption( "cfgVersionList" )
#     self.cfgName = self.am_getOption( "cfgNameList" )
#     self.dqFlag = self.am_getOption( "dqFlag" )
#     self.brunelCount = 0
#     self.daVinciCount = 0
#     self.typeDict = self.am_getOption( "evtTypeDict" )
#     self.typeList = self.am_getOption( "histTypeList" )
#     self.passes = self.am_getOption( "ProcessingPasses" )
#     self.conditions = self.am_getOption( "DataTakingConditions" )
#     self.testMode = self.am_getOption( "testMode" )
#     self.specialMode = self.am_getOption( "specialMode" )
#     self.runs = self.am_getOption( "specialRuns" )
#     self.addFlag = self.am_getOption( "addFlag" )
#     self.threshold = self.am_getOption( "threshold" )
#
#
#   def initialize( self ):
#     ''' initialization
#     '''
#
#     self.am_setOption( 'shifterProxy', 'DataManager' )
#
#     #Dictionary used for check that every configuration have been retrieved
#     configuration = {'ExeDir' : False,
#                      'homeDir' : False,
#                      'senderAddress' : False,
#                      'mailAddress' : False,
#                      'applicationName' : False,
#                      'eventType' : False,
#                      'cfgNameList' : False,
#                      'cfgVersionList' : False,
#                      'dqFlag' : False,
#                      'evtTypeDict' : False,
#                      'histTypeList' : False,
#                      'DataTakingConditions' : False,
#                      'ProcessingPasses' : False,
#                      'testMode' : False,
#                      'specialMode' : False,
#                      'threshold' : False,
#                      'specialRuns' : False,
#                      'addFlag' : False}
#
#     gLogger.info( '=====Gathering information from dirac.cfg=====' )
#     gLogger.info( 'applicationName %s' % self.applicationName )
#     if self.applicationName:
#       configuration['applicationName'] = True
#     if self.homeDir:
#       configuration['homeDir'] = True
#       gLogger.info( self.homeDir )
#
#       #Local directory creation
#
#       tmpDir = os.path.dirname( self.homeDir )
#       gLogger.info( 'Checking temp dir %s' % ( tmpDir ) )
#       if not os.path.exists( tmpDir ):
#         gLogger.info( '%s not found. Going to create' % ( tmpDir ) )
#         os.makedirs( tmpDir )
#       else:
#         removal = self.homeDir + '*'
#         j = glob.glob( removal )
#         for i in j:
#           if os.path.isdir( i ) == True:
#             shutil.rmtree( i )
#           else:
#             os.remove( i )
#
#
#     #Compiled C++ root macros for the three Merging steps.
#
#     if self.mergeExeDir:
#       configuration['ExeDir'] = True
#       self.mergeStep1Command = self.mergeExeDir + '/Merge'
#       self.mergeStep2Command = self.mergeExeDir + '/Merge2'
#       self.mergeStep3Command = self.mergeExeDir + '/Merge3'
#
#     if not ( os.path.exists( self.mergeStep1Command ) and os.path.exists( self.mergeStep2Command )
#              and os.path.exists( self.mergeStep3Command ) ):
#       gLogger.error( 'Executables not found' )
#       return S_ERROR()
#     else :
#       gLogger.info( 'Executables found in %s' % self.mergeExeDir )
#
#     if self.senderAddress:
#       configuration['senderAddress'] = True
#     if self.mailAddress:
#       configuration['mailAddress'] = True
#     if self.thisEventType:
#       configuration['eventType'] = True
#     if self.cfgName:
#       configuration['cfgNameList'] = True
#       cfgNameStr = string.join( self.cfgName.split(), "" )
#       self.cfgNameList = cfgNameStr.split( "," )
#
#     if self.cfgVersion:
#       configuration['cfgVersionList'] = True
#       cfgVersionStr = string.join( self.cfgVersion.split(), "" )
#       self.cfgVersionList = cfgVersionStr.split( "," )
#
#     if self.dqFlag:
#       configuration['dqFlag'] = True
#       dqFlagStr = string.join( self.dqFlag.split(), "" )
#       self.dqFlagList = dqFlagStr.split( "," )
#
#     if self.typeDict:
#       evtStr = string.join( self.typeDict.split(), "" )
#       ll = evtStr.split( "," )
#       self.evtTypeDict = {}
#       for tt in ll:
#         s = tt.split( ":" )
#         self.evtTypeDict[s[0]] = s[1]
#     configuration['evtTypeDict'] = True
#
#     if self.typeList:
#       typeStr = string.join( self.typeList.split(), "" )
#       self.histTypeList = typeStr.split( "," )
#       configuration['histTypeList'] = True
#
#     self.processingPasses = []
#     if self.passes:
#       passesStr = string.join( self.passes.split(), "" )
#       temp = passesStr.split( "," )
#       for ll in temp:
#         if ll.find( 'RealData' ):
#           ll = ll.replace( 'RealData', 'Real Data' )
#           self.processingPasses.append( ll )
#       configuration['ProcessingPasses'] = True
#
#
#     if self.conditions:
#       conditionsStr = string.join( self.conditions.split(), "" )
#       self.dataTakingConditions = conditionsStr.split( "," )
#       configuration['DataTakingConditions'] = True
#
#
#     if self.testMode:
#       configuration['testMode'] = True
#     if self.specialMode:
#       configuration['specialMode'] = True
#     if self.runs:
#       runsStr = string.join( self.runs.split(), "" )
#       self.specialRuns = runsStr.split( "," )
#     configuration['specialRuns'] = True
#
#     if self.addFlag:
#       configuration['addFlag'] = True
#
#     try:
#       self.threshold = float( self.threshold )
#       if self.threshold > 1.0 or self.threshold < 0.0:
#         gLogger.error( "Threshold should be between [0,1]!" )
#         configuration['threshold'] = False
#       else:
#         configuration['threshold'] = True
#     except ValueError:
#       gLogger.error( "Threshold bad defined!" )
#       configuration['threshold'] = False
#
#
#     #If one of the configuration is missing the initialization fails
#     nConf = False
#     for confVar in configuration:
#       if not configuration[confVar]:
#         gLogger.error( '%s not specified in CS' % confVar )
#         gLogger.error( '---------------------------------' )
#         nConf = True
#     if nConf:
#       return S_ERROR()
#
#     gLogger.info( '=====All information from dirac.cfg retrieved=====' )
#
#     env = dict( os.environ )
#     #Need to set a user to let getProjectEnvironment run correctly.
#     env['USER'] = 'dirac'
#     res = getProjectEnvironment( self.systemConfiguration, self.applicationName,
#                                  poolXMLCatalogName = 'pool_xml_catalog.xml', env = env )
#
#     #If the environment cannot be setup the init fails
#     if not res['OK']:
#       gLogger.error( '===== Cannot create the environment check LocalArea or SharedArea in dirac.cfg =====' )
#       return S_ERROR()
#
#     self.environment = res['Value']
#
#     return S_OK()
#
#   def execute( self ):
#     ''' execution
#     '''
#
#     rootVersion = "5.32.00"
#
#     evtTypeRef = {}
#     #Conversion table for the stream names
#     evtTypeRef['90000000'] = 'Full stream'
#     evtTypeRef['91000000'] = 'Express stream'
#     evtTypeRef['93000000'] = 'Luminosity stream online'
#     evtTypeRef['95000000'] = 'Calib stream'
#     evtTypeRef['96000000'] = 'NoBias stream'
#     evtTypeRef['97000000'] = 'Beam Gas'
#
#     #The bkqueries for brunel and davinci are the same except for the filetype
#     bkDict_brunel = {}
#     bkDict_davinci = {}
#     lfns = {}
#
#     #Loop over all the possible bk queries the can be build given the configurations present in dirac.cfg
#     for name in self.cfgNameList:
#       gLogger.info( self.cfgNameList )
#       for version in self.cfgVersionList:
#         gLogger.info( self.cfgVersionList )
#         for event in self.evtTypeDict.values():
#           gLogger.info( event )
#           for processing in self.processingPasses:
#             gLogger.info( processing )
#             for dataTaking in self.dataTakingConditions:
#               gLogger.info( dataTaking )
#               for dqFlag in self.dqFlagList:
#                 gLogger.info( dqFlag )
#                 bkDict_brunel[ 'ConfigName' ] = name
#                 bkDict_brunel[ 'ConfigVersion' ] = version
#                 bkDict_brunel[ 'EventTypeId' ] = event
#                 bkDict_brunel['FileType'] = self.histTypeList[0]
#                 bkDict_brunel[ 'EventTypeDescription' ] = evtTypeRef[event]
#                 bkDict_brunel[ 'ProcessingPass' ] = processing
#                 bkDict_brunel['DataTakingConditions'] = dataTaking
#                 bkDict_brunel['DataQualityFlag'] = dqFlag
#
#
#
#                 gLogger.info( '==================================================' )
#                 gLogger.info( '=====Retrieving the list of runs with bkquery=====' )
#                 gLogger.info( '==================================================' )
#                 gLogger.info( str( bkDict_brunel ) )
#                 gLogger.info( '==================================================' )
#                 gLogger.info( '==================================================' )
#                 #Retrieve the of file and order them per run
#                 res_0 = getRuns( bkDict_brunel, self.bkClient )
#                 gLogger.info( "Number of Runs found %s" % len( res_0.keys() ) )
#
#                 bkDict_davinci = bkDict_brunel
#
#                 bkDict_davinci['FileType'] = self.histTypeList[1]
#
#                 gLogger.info( '==================================================' )
#                 gLogger.info( '=====Retrieving the list of runs with bkquery=====' )
#                 gLogger.info( '==================================================' )
#                 gLogger.info( str( bkDict_davinci ) )
#                 gLogger.info( '==================================================' )
#                 gLogger.info( '==================================================' )
#
#                 #Retrieve the of file and order them per run
#                 res_1 = getRuns( bkDict_davinci, self.bkClient )
#                 gLogger.info( "Number of Runs found %s" % len( res_1.keys() ) )
#
#                 #If the number of BrunelHist and DaVinciHist is not equal it skips the run
#                 if len( res_0.keys() ) == 0 or len( res_1.keys() ) == 0:
#                   gLogger.error( 'No BRUNELHIST or DAVINCIHIST present for this bkQuery. ' )
#                 else:
#                   for run in res_0.keys():
#                     iD = self.bkClient.getProcessingPassId( bkDict_brunel[ 'ProcessingPass' ] )
#                     quality = self.bkClient.getRunAndProcessingPassDataQuality( run, iD['Value'] )
#                     if quality['OK']:
#                       gLogger.info( "Run %s has already been flagged skipping" )
#                       continue
#
#                     resProdId = getProductionId( run, processing, event , self.bkClient )
#                     if not resProdId['OK']:
#                       gLogger.error( "Production ID not found for run %s and processing pass %s Continue with the other runs." % ( run , processing ) )
#                       continue
#                     lfns = buildLFNs( bkDict_brunel, run, resProdId['prodId'], self.addFlag )
#                     #If the LFN is already in the BK it will be skipped
#                     res = self.transClient.getRunsMetadata( run )
#
#                     if res['OK'] and res['Value'].has_key( run ):
#                       try:
#                         if ( res['Value'][run]['DQFlag'] == 'M' ) and ( res['Value'][run]['ProcessingPass'] == bkDict_brunel[ 'ProcessingPass' ] ):
#                           _msg = "%s already in the bookkeeping. Continue with the other runs."
#                           gLogger.info( _msg % str( lfns['DATA'] ) )
#                           continue
#                         if res['Value'][run]['DQFlag'] == 'P':
#                           _msg = "Run %s problematic: %s"
#                           gLogger.info( _msg % ( run, res['Value'][run]['Info'] ) )
#
#                       except KeyError:
#                         _msg = "No info saved for run %s" % run
#                         gLogger.info( _msg )
#
#                     res = self.bkClient.getFileMetadata( [lfns['DATA']] )
#
#                     if res['Value']['Successful']:
#                       if res['Value']['Successful'][lfns['DATA']]['GotReplica'] == 'Yes':
#                         _msg = "%s already in the bookkeeping. Continue with the other runs."
#                         metaDataDict = {}
#                         metaDataDict['ProcessingPass'] = bkDict_brunel[ 'ProcessingPass' ]
#                         metaDataDict['DQFlag'] = 'M'
#                         self.transClient.addRunsMetadata( run, metaDataDict )
#                         gLogger.info( _msg % str( lfns['DATA'] ) )
#                         continue
#                     #log directory that will be uploaded
#                     logDir = self.homeDir + 'MERGEFORDQ_RUN_' + str( run )
#                     if not os.path.exists( logDir ):
#                       os.makedirs( logDir )
#
#                     self.logFileName = '%s/Brunel_DaVinci_%s.log' % ( logDir , run )
#
#                     gLogger.info( 'Log file path for application execution %s' % self.logFileName )
#
#                     self.logFile = open( self.logFileName, 'w+' )
#
#                     gLogger.info ( 'Runs to be skipped %s' % self.specialRuns )
#                     if str( run ) in self.specialRuns:
#                       continue
#                     if not ( len( res_0[run]['LFNs'] ) == len( res_1[run]['LFNs'] ) ):
#                       gLogger.error( "Different number of BRUNELHIST and DAVINCIHIST. Skipping run %s" % run )
#                       metaDataDict = {}
#                       metaDataDict['DQFlag'] = 'P'
#                       metaDataDict['ProcessingPass'] = bkDict_brunel[ 'ProcessingPass' ]
#                       metaDataDict['Info'] = 'Different number of DAVINCI and BRUNEL hists'
#                       self.transClient.addRunsMetadata( run, metaDataDict )
#                     #Starting the three step Merging process
#                     _msg = '=======================Starting Merging Process for run %s========================'
#                     gLogger.info( _msg % run )
#                     res = self.bkClient.getRunInformations( run )
#                     run_end = time.mktime( res['Value']['RunEnd'].timetuple() )
#                     now = time.mktime( datetime.datetime.utcnow().timetuple() )
#                     if ( int( now - run_end ) / 86400 < 1 ):
#                       gLogger.error( "Run %s not yet finished" % run )
#                       continue
#                     else:
#                       delta = int( ( now - run_end ) / 86400 )
#                       gLogger.info( "Time after EndRun %s" % delta )
#                     res = mergeRun( bkDict_brunel, res_0, res_1, run, self.bkClient, self.transClient, self.homeDir ,
#                                     resProdId['prodId'], self.addFlag, self.specialMode,
#                                     self.mergeStep1Command, self.mergeStep2Command, self.mergeStep3Command,
#                                     self.brunelCount, self.daVinciCount, self.threshold,
#                                     self.logFile, self.logFileName, self.environment )
#                     #if the Merging process went fine the Finalization method is called
#                     if res['Merged']:
#                       inputData = res_0[run]['LFNs'] + res_1[run]['LFNs']
#                       '''
#                       Finalization Step:
#                       '''
#                       #gLogger.info("Finalization")
#                       #continue
#                       res = finalization( self.homeDir, logDir, lfns, res['OutputFile'],
#                                           ( 'Brunel_DaVinci_%s.log' % run ), inputData,
#                                           run, bkDict_brunel, rootVersion )
#                       if res['OK']:
#
#                         # If the finalization went fine an automated message is sent to the
#                         # relevant mailing list specified in the cfg.
#
#                         outMess = "**********************************************************************************************************************************\n"
#                         outMess = outMess + "\nThis is an automatic message:\n"
#                         outMess = outMess + "\n**********************************************************************************************************************************\n"
#                         outMess = outMess + '\nRun: %s\n Processing Pass: %s\n Stream: %s\n DataTaking Conditions: %s\n\n' % ( str( run ), processing, evtTypeRef[event], dataTaking )
#                         xmldoc = minidom.parse( res['XML'] )
#                         node = xmldoc.getElementsByTagName( 'Replica' )[0]
#                         web = node.attributes['Name'].nodeValue
#                         outMess = outMess + '\nROOT file LFN: %s' % ( lfns['DATA'] ) + '\n'
#                         outMess = outMess + '\nLocation of logfile %s\n' % str( web )
#                         outMess = outMess + "\n**********************************************************************************************************************************"
#                         notifyClient = NotificationClient()
#                         subject = 'New %s merged ROOT file for run %s ready' % ( evtTypeRef[event], str( run ) )
#
#                         res = notifyClient.sendMail( self.mailAddress, subject, outMess, self.senderAddress, localAttempt = False )
#
#                         metaDataDict = {}
#                         metaDataDict['ProcessingPass'] = bkDict_brunel[ 'ProcessingPass' ]
#                         metaDataDict['DQFlag'] = 'M'
#                         metaDataDict['Info'] = 'Merged and Uploaded'
#
#                         res = self.transClient.getRunsMetadata( run )
#                         if res['OK'] and res['Value'].has_key( run ):
#                           try:
#                             if ( res['Value'][run].has_key( 'DQFlag' ) and res['Value'][run]['ProcessingPass'] == bkDict_brunel[ 'ProcessingPass' ] ):
#                               self.transClient.updateRunsMetadata( run , metaDataDict )
#                           except KeyError:
#                             _msg = "No info saved for run %s. Filling entry now." % run
#                             gLogger.info( _msg )
#                             self.transClient.addRunsMetadata( run, metaDataDict )
#
#
#                         #Cleaning of all the local files and directories.
#                         removal = self.homeDir + '*'
#                         toremove = glob.glob( removal )
#                         for i in toremove:
#                           if os.path.isdir( i ) == True:
#                             shutil.rmtree( i )
#                           else:
#                             os.remove( i )
#                       else:
#                         removal = self.homeDir + '*'
#                         toremove = glob.glob( removal )
#                         for i in toremove:
#                           if os.path.isdir( i ) == True:
#                             shutil.rmtree( i )
#                           else:
#                             os.remove( i )
#                         DIRAC.exit( 2 )
#
#                   gLogger.info( '=== End of Loop over Runs ===' )
#     return S_OK()
