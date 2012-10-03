'''
MergingForDQAgent automatize some operations done by the data quality crew. It looks 
for Runs that have not been flagged (i.e. that are UNCHECKED). If it finds out some 
of them it performs a merging of the BRUNELHIST and DAVINCIHIST root files.
The main steps are the following:

- The initialize method gather informations from the CS

- Retrieving the UNCHECKED (or OK) DAVINCIHISTs and BRUNELHISTs by run.

- The files are saved locally and merged in three steps. The first step consists of 
  merging separately the two kinds of histograms in groups of 50. This grouping is 
  made because the merging is CPU consuming and if you attempt to merge a lot of file in a row 
  you probably stuck the machine. The output files of this first step are then merged as well. 
  Finally DAVINCI and BRUNEL histograms are then put together in a single file.
 
- The results (data an logs) are uploaded in the bookkeeping.

Systems/DataManagement/Production/Agents/

        MergingForDQAgent
        {
          ExeDir = /opt/dirac/pro/Merge
          homeDir = /tmp/dirac/MergingForDQ
          senderAddress = someaddress
          mailAddress = someaddress
          applicationName = LCGCMT ROOT
          eventType = FULL
          cfgNameList = LHCb
          cfgVersionList = Collision11
          dqFlag = UNCHECKED
          evtTypedict = EXPRESS:91000000, FULL:90000000
          histTypeList = BRUNELHIST,DAVINCIHIST
          testMode = False
          specialMode = False
          specialRuns =
         }
          
'''

import os, string, glob, shutil

import DIRAC

from DIRAC                                                import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Base.AgentModule                          import AgentModule
from DIRAC.FrameworkSystem.Client.NotificationClient      import NotificationClient

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.Core.Utilities.ProductionEnvironment       import getProjectEnvironment
from LHCbDIRAC.DataManagementSystem.Utilities.MergeForDQ  import getRuns, getProductionId, buildLFNs, \
  mergeRun, finalization

from  xml.dom import minidom
import datetime
import time

AGENT_NAME = 'DataManagement/MergingForDQAgent'
__RCSID__  = "$Id: $"

class MergingForDQAgent( AgentModule ):

  def initialize( self ):
    self.am_setOption( 'shifterProxy', 'DataManager' )
    self.systemConfiguration = 'x86_64-slc5-gcc43-opt'
    #Dictionary used for check that every configuration have been retrieved
    configuration = {'ExeDir' : False,
                     'homeDir' : False,
                     'senderAddress' : False,
                     'mailAddress' : False,
                     'applicationName' : False,
                     'eventType' : False,
                     'cfgNameList' : False,
                     'cfgVersionList' : False,
                     'dqFlag' : False,
                     'evtTypeDict' : False,
                     'histTypeList' : False,
                     'DataTakingConditions' : False,
                     'ProcessingPasses' : False,
                     'testMode' : False,
                     'specialMode' : False,
                     'specialRuns' : False,
                     'addFlag' : False}

    gLogger.info( '=====Gathering information from dirac.cfg=====' )
    options = "Systems/DataManagement/Production/Agents/MergingForDQAgent"
    if not options:
      gLogger.error( "No configuration for MergingForDQAgent found in the CS" )
      return S_ERROR()
    self.applicationName = gConfig.getValue( "%s/applicationName" % options )
    gLogger.info( 'applicationName %s' % self.applicationName )
    if self.applicationName: 
      configuration['applicationName'] = True
    self.homeDir = gConfig.getValue( "%s/homeDir" % options )
    if self.homeDir:
      configuration['homeDir'] = True
      gLogger.info( self.homeDir )

      #Local directory creation 
      
      tmpDir = os.path.dirname( self.homeDir )
      gLogger.info( 'Checking temp dir %s' % ( tmpDir ) )
      if not os.path.exists( tmpDir ):
        gLogger.info( '%s not found. Going to create' % ( tmpDir ) )
        os.makedirs(tmpDir)
      else:
        removal = self.homeDir+'*'
        j = glob.glob(removal)
        for i in j:
          if os.path.isdir(i) == True:
            shutil.rmtree(i)
          else:
            os.remove(i)
      
    
    #Compiled C++ root macros for the three Merging steps. 
    
    self.mergeExeDir = gConfig.getValue( "%s/ExeDir" % options )

    if self.mergeExeDir:
      configuration['ExeDir'] = True
      self.mergeStep1Command = self.mergeExeDir + '/Merge'
      self.mergeStep2Command = self.mergeExeDir + '/Merge2'
      self.mergeStep3Command = self.mergeExeDir + '/Merge3'

    if not ( os.path.exists( self.mergeStep1Command ) and os.path.exists( self.mergeStep2Command ) 
             and os.path.exists( self.mergeStep3Command ) ):
      gLogger.error( 'Executables not found' )
      return S_ERROR()
    else :
      gLogger.info( 'Executables found in %s' % self.mergeExeDir )


    self.senderAddress = gConfig.getValue( "%s/senderAddress" % options )
    if self.senderAddress: 
      configuration['senderAddress'] = True
    self.mailAddress = gConfig.getValue( "%s/mailAddress" % options )
    if self.mailAddress:
      configuration['mailAddress'] = True
    self.thisEventType = gConfig.getValue( "%s/eventType" % options )
    if self.thisEventType: 
      configuration['eventType'] = True
    cfgName = gConfig.getValue( "%s/cfgNameList" % options )
    if cfgName:
      configuration['cfgNameList'] = True
      cfgNameStr = string.join( cfgName.split(), "" )
      self.cfgNameList = cfgNameStr.split( "," )

    cfgVersion = gConfig.getValue( "%s/cfgVersionList" % options )
    if cfgVersion:
      configuration['cfgVersionList'] = True
      cfgVersionStr = string.join( cfgVersion.split(), "" )
      self.cfgVersionList = cfgVersionStr.split( "," )

    dqFlag = gConfig.getValue( "%s/dqFlag" % options )
    if dqFlag:
      configuration['dqFlag'] = True
      dqFlagStr = string.join( dqFlag.split(), "" )
      self.dqFlagList = dqFlagStr.split( "," )

    self.brunelCount  = 0
    self.daVinciCount = 0

    typeDict = gConfig.getValue( "%s/evtTypeDict" % options )
    if typeDict:
      evtStr = string.join( typeDict.split(), "" )
      ll = evtStr.split( "," )
      self.evtTypeDict = {}
      for tt in ll:
        s = tt.split( ":" )
        self.evtTypeDict[s[0]] = s[1]
    configuration['evtTypeDict'] = True

    typeList = gConfig.getValue( "%s/histTypeList" % options )
    if typeList:
      typeStr = string.join( typeList.split(), "" )
      self.histTypeList = typeStr.split( "," )
      configuration['histTypeList'] = True

    passes = gConfig.getValue( "%s/ProcessingPasses" % options )
    self.processingPasses = []
    if passes:
      passesStr = string.join( passes.split(), "" )
      temp = passesStr.split( "," )
      for ll in temp:
        if ll.find( 'RealData' ):
          ll = ll.replace( 'RealData', 'Real Data' )
          self.processingPasses.append( ll )
      configuration['ProcessingPasses'] = True


    conditions = gConfig.getValue( "%s/DataTakingConditions" % options )
    if conditions:
      conditionsStr = string.join( conditions.split(), "" )
      self.dataTakingConditions = conditionsStr.split( "," )
      configuration['DataTakingConditions'] = True


    self.testMode = gConfig.getValue( "%s/testMode" % options )
    if self.testMode: 
      configuration['testMode'] = True
    self.specialMode = gConfig.getValue( "%s/specialMode" % options )
    if self.specialMode: 
      configuration['specialMode'] = True
    runs = gConfig.getValue( "%s/specialRuns" % options )
    if runs:
      runsStr = string.join( runs.split(), "" )
      self.specialRuns = runsStr.split( "," )
    configuration['specialRuns'] = True
    
    self.addFlag = gConfig.getValue( "%s/addFlag" % options )
    if self.addFlag: 
      configuration['addFlag'] = True
    
    #If one of the configuration is missing the initialization fails
    nConf = False
    for confVar in configuration:
      if not configuration[confVar]:
        gLogger.error( '%s not specified in CS' % confVar )
        gLogger.error( '---------------------------------' )
        nConf = True
    if nConf: 
      return S_ERROR()

    gLogger.info( '=====All informations from dirac.cfg retrieved=====' )


    env = dict( os.environ )
    #Need to set a user to let getProjectEnvironment run correctly. 
    env['USER'] = 'dirac'
    res = getProjectEnvironment( self.systemConfiguration, self.applicationName, 
                                 applicationVersion = '', extraPackages = '',
                                 runTimeProject = '', site = '', directory = '', generatorName = '',
                                 poolXMLCatalogName = 'pool_xml_catalog.xml', env = env )

    #If the environment cannot be setup the init fails
    if not res['OK']:
      gLogger.error( '===== Cannot create the environment check LocalArea or SharedArea in dirac.cfg =====' )
      return S_ERROR()

    self.environment = res['Value']

    self.logFileName = ''

    self.logFile = ''

    self.bkClient = BookkeepingClient()

    return S_OK()

  def execute( self ):

    rootVersion = "5.32.00"

    evtTypeRef = {}
    #Conversion table for the stream names
    evtTypeRef['90000000'] = 'Full stream'
    evtTypeRef['91000000'] = 'Express stream'
    evtTypeRef['93000000'] = 'Luminosity stream online'
    evtTypeRef['95000000'] = 'Calib stream'
    evtTypeRef['96000000'] = 'NoBias stream'
    evtTypeRef['97000000'] = 'Beam Gas'

    #The bkqueries for brunel and davinci are the same except for the filetype
    bkDict_brunel = {}
    bkDict_davinci = {}
    lfns = {}

    #Loop over all the possible bk queries the can be build given the configurations present in dirac.cfg
    for name in self.cfgNameList:
      gLogger.info( self.cfgNameList )
      for version in self.cfgVersionList:
        gLogger.info( self.cfgVersionList )
        for event in self.evtTypeDict.values():
          gLogger.info( event )
          for processing in self.processingPasses:
            gLogger.info( processing )
            for dataTaking in self.dataTakingConditions:
              gLogger.info( dataTaking )
              for dqFlag in self.dqFlagList:
                gLogger.info( dqFlag )
                bkDict_brunel[ 'ConfigName' ] = name
                bkDict_brunel[ 'ConfigVersion' ] = version
                bkDict_brunel[ 'EventTypeId' ] = event
                bkDict_brunel['FileType'] = self.histTypeList[0]
                bkDict_brunel[ 'EventTypeDescription' ] = evtTypeRef[event]
                bkDict_brunel[ 'ProcessingPass' ] = processing
                bkDict_brunel['DataTakingConditions'] = dataTaking
                bkDict_brunel['DataQualityFlag'] = dqFlag



                gLogger.info( '==================================================' )
                gLogger.info( '=====Retrieving the list of runs with bkquery=====' )
                gLogger.info( '==================================================' )
                gLogger.info( str( bkDict_brunel ) )
                gLogger.info( '==================================================' )
                gLogger.info( '==================================================' )
                #Retrieve the of file and order them per run
                res_0 = getRuns( bkDict_brunel, self.bkClient )
                gLogger.info( "Number of Runs found %s" % len( res_0.keys() ) )

                bkDict_davinci = bkDict_brunel

                bkDict_davinci['FileType'] = self.histTypeList[1]

                gLogger.info( '==================================================' )
                gLogger.info( '=====Retrieving the list of runs with bkquery=====' )
                gLogger.info( '==================================================' )
                gLogger.info( str( bkDict_davinci ) )
                gLogger.info( '==================================================' )
                gLogger.info( '==================================================' )

                #Retrieve the of file and order them per run
                res_1 = getRuns( bkDict_davinci, self.bkClient )
                gLogger.info( "Number of Runs found %s" % len( res_1.keys() ) )

                #If the number of BrunelHist and DaVinciHist is not equal it skips the run
                if len( res_0.keys() ) == 0 or len( res_1.keys() ) == 0:
                  gLogger.error( 'No BRUNELHIST or DAVINCIHIST present for this bkQuery. ' )
                else:
                  for run in res_0.keys():
                    iD = self.bkClient.getProcessingPassId( bkDict_brunel[ 'ProcessingPass' ] )
                    quality = self.bkClient.getRunAndProcessingPassDataQuality( run, iD['Value'] )
                    if quality['OK']:
                      gLogger.info( "Run %s has already been flagged skipping" )
                      continue
                    
                    resProdId  = getProductionId(run, processing, event , self.bkClient)
                    if not resProdId['OK']:
                      gLogger.error("Production ID not found for run %s and processing pass %s Continue with the other runs." %(run , processing))
                      continue
                    lfns = buildLFNs( bkDict_brunel, run, resProdId['prodId'], self.addFlag )
                    #If the LFN is already in the BK it will be skipped
                    res = self.bkClient.getFileMetadata( [lfns['DATA']] )
                    if res['Value']:
                      if res['Value'][lfns['DATA']]['GotReplica'] == 'Yes':
                        _msg = "%s already in the bookkeeping. Continue with the other runs."
                        
                        gLogger.info( _msg % str( lfns['DATA'] ) )
                        continue
                    #log directory that will be uploaded 
                    logDir = self.homeDir + 'MERGEFORDQ_RUN_' + str( run )
                    if not os.path.exists( logDir ):
                      os.makedirs( logDir )

                    self.logFileName = '%s/Brunel_DaVinci_%s.log' % ( logDir , run )

                    gLogger.info( 'Log file path for application execution %s' % self.logFileName )

                    self.logFile = open( self.logFileName, 'w+' )

                    gLogger.info ( 'Runs to be skipped %s' % self.specialRuns )
                    if str( run ) in self.specialRuns:
                      continue
                    if not ( len( res_0[run]['LFNs'] ) == len( res_1[run]['LFNs'] ) ):
                      gLogger.error( "Different number of BRUNELHIST and DAVINCIHIST. Skipping run %s" % run )
                      continue
                    #Starting the three step Merging process
                    _msg = '=======================Starting Merging Process for run %s========================'
                    gLogger.info( _msg % run )
                    res = self.bkClient.getRunInformations(run)
                    run_end = time.mktime(res['Value']['RunEnd'].timetuple())
                    now = time.mktime(datetime.datetime.utcnow().timetuple())
                    if (int(now-run_end)/86400 < 1):
                      gLogger.error("Run %s not yet finished"%run)
                      continue
                    else:
                      delta = int((now-run_end)/86400)
                      gLogger.info("Time after EndRun %s"%delta)
                    res = mergeRun( bkDict_brunel, res_0, res_1, run, self.bkClient, self.homeDir , 
                                    resProdId['prodId'], self.addFlag, self.testMode, self.specialMode,
                                    self.mergeStep1Command, self.mergeStep2Command, 
                                    self.mergeStep3Command,self.brunelCount, self.daVinciCount, 
                                    self.logFile, self.logFileName, self.environment )
                    #if the Merging process went fine the Finalization method is called
                    if res['Merged']:
                      inputData = res_0[run]['LFNs'] + res_1[run]['LFNs']
                      '''
                      Finalization Step:
                      '''
                      #gLogger.info("Finalization")
                      #continue
                      res = finalization( self.homeDir, logDir, lfns, res['OutputFile'], 
                                          ( 'Brunel_DaVinci_%s.log' % run ), inputData, 
                                          run, bkDict_brunel, rootVersion )
                      if res['OK']:
                        
                        # If the finalization went fine an automated message is sent to the 
                        # relevant mailing list specified in the cfg.
                        
                        outMess = "**********************************************************************************************************************************\n"
                        outMess = outMess + "\nThis is an automatic message:\n"
                        outMess = outMess + "\n**********************************************************************************************************************************\n"
                        outMess = outMess + '\nRun: %s\n Processing Pass: %s\n Stream: %s\n DataTaking Conditions: %s\n\n' % ( str( run ), processing, evtTypeRef[event], dataTaking )
                        xmldoc = minidom.parse( res['XML'] )
                        node = xmldoc.getElementsByTagName( 'Replica' )[0]
                        web = node.attributes['Name'].nodeValue
                        outMess = outMess + '\nROOT file LFN: %s' % ( lfns['DATA'] ) + '\n'
                        outMess = outMess + '\nLocation of logfile %s\n' % str( web )
                        outMess = outMess + "\n**********************************************************************************************************************************"
                        notifyClient = NotificationClient()
                        subject = 'New %s merged ROOT file for run %s ready' % ( evtTypeRef[event], str( run ) )

                        res = notifyClient.sendMail( self.mailAddress, subject, outMess, self.senderAddress, localAttempt = False )

                        #Cleaning of all the local files and directories.
                        removal = self.homeDir + '*'
                        toremove = glob.glob( removal )
                        for i in toremove:
                          if os.path.isdir( i ) == True:
                            shutil.rmtree( i )
                          else:
                            os.remove( i )
                      else:
                        removal = self.homeDir + '*'
                        toremove = glob.glob( removal )
                        for i in toremove:
                          if os.path.isdir( i ) == True:
                            shutil.rmtree( i )
                          else:
                            os.remove( i )
                        DIRAC.exit( 2 )

                  else: gLogger.error( 'The number of runs selected by the bkQuery for %s in diffent from the one selected by the bkQuery for %s' % ( self.histTypeList[0], self.histTypeList[1] ) )
    return S_OK()
