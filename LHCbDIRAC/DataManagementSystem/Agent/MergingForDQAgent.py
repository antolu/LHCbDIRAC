__RCSID__ = "$Id: $"
'''
MergingForDQAgent automatize some operations done by the data quality crew. It looks for Runs that have not been flagged 
(i.e. that are UNCHECKED). If it finds out some of them it performs a merging of the BRUNELHIST and DAVINCIHIST root files.
The main steps are the following:

- The initialize method gather informations from the CS

- Retrieving the UNCHECKED (or OK) DAVINCIHISTs and BRUNELHISTs by run.

- The files are saved locally and merged in three steps. The first step consists of merging separately the two kinds of histograms 
  in groups of 50. This grouping is made because the merging is CPU consuming and if you attempt to merge a lot of file in a row 
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

import DIRAC
from DIRAC                                                     import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.AgentModule                               import AgentModule
from LHCbDIRAC.DataManagementSystem.Utilities.MergeForDQ       import *
from DIRAC.Core.Base                                           import Script
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient   import BookkeepingClient
from   DIRAC.FrameworkSystem.Client.NotificationClient       import NotificationClient
from DIRAC import gConfig
import re,os,string,glob,shutil
import subprocess
from LHCbDIRAC.Core.Utilities.ProductionEnvironment import getProjectEnvironment

from  xml.dom import minidom

AGENT_NAME = 'DataManagement/MergingForDQAgent'

class MergingForDQAgent( AgentModule ):

  def initialize( self ):
    self.am_setOption( 'shifterProxy', 'DataManager' )
    self.systemConfiguration = 'x86_64-slc5-gcc43-opt' 
    #Dictionary used for check that every configuration have been retrieved
    Configuration = {'ExeDir' : False,
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
                     'specialRuns' : False}

    gLogger.info('=====Gathering information from dirac.cfg=====')
    options = "Systems/DataManagement/Production/Agents/MergingForDQAgent"
    if not options:
      gLogger.error("No configuration for MergingForDQAgent found in the CS")
      DIRAC.exit(2) 
    self.applicationName = gConfig.getValue( "%s/applicationName"%options )
    gLogger.info('applicationName %s'%self.applicationName)
    if self.applicationName: Configuration['applicationName']=True     
    self.homeDir = gConfig.getValue( "%s/homeDir"%options )
    if self.homeDir: 
      Configuration['homeDir']=True
      gLogger.info(self.homeDir)
      '''
      Local directory creation 
      '''
      d = os.path.dirname(self.homeDir)
      gLogger.info( 'Checking temp dir %s' % ( d ) )
      if not os.path.exists(d):
        gLogger.info( '%s not found. Going to create' % ( d ) )
        os.makedirs(d)
        
    '''
    Compiled C++ root macros for the three Merging steps. 
    '''
    self.mergeExeDir = gConfig.getValue( "%s/ExeDir"%options )

    if self.mergeExeDir: 
      Configuration['ExeDir']=True
      self.mergeStep1Command = self.mergeExeDir + '/Merge'
      self.mergeStep2Command = self.mergeExeDir + '/Merge2'
      self.mergeStep3Command = self.mergeExeDir + '/Merge3'
    
    if not (os.path.exists(self.mergeStep1Command) and os.path.exists(self.mergeStep2Command) and os.path.exists(self.mergeStep3Command)):
      gLogger.error( 'Executables not found')
      DIRAC.exit(2)
    else :
      gLogger.info( 'Executables found in %s' %self.mergeExeDir)
    

    self.senderAddress = gConfig.getValue( "%s/senderAddress"%options )
    if self.senderAddress: Configuration['senderAddress']=True
    self.mailAddress = gConfig.getValue( "%s/mailAddress"%options )
    if self.mailAddress:Configuration['mailAddress']=True
    self.thisEventType = gConfig.getValue( "%s/eventType"%options )
    if self.thisEventType: Configuration['eventType']=True
    cfgName = gConfig.getValue( "%s/cfgNameList"%options )
    if cfgName: 
      Configuration['cfgNameList']=True
      l = string.join(cfgName.split(),"")
      self.cfgNameList = l.split(",")
      

    cfgVersion = gConfig.getValue( "%s/cfgVersionList"%options )
    if cfgVersion: 
      Configuration['cfgVersionList']=True
      l = string.join(cfgVersion.split(),"")
      self.cfgVersionList = l.split(",")
      
    dqFlag = gConfig.getValue( "%s/dqFlag"%options )
    if dqFlag: 
      Configuration['dqFlag']=True
      l = string.join(dqFlag.split(),"")
      self.dqFlagList = l.split(",")
    
    self.brunelCount = 0
    self.daVinciCount = 0

    TypeDict = gConfig.getValue( "%s/evtTypeDict"%options )
    if TypeDict:
      l = string.join(TypeDict.split(),"")
      ll = l.split(",")
      self.evtTypeDict = {}
      for t in ll:
        s =  t.split(":")  
        self.evtTypeDict[s[0]]=s[1]
    Configuration['evtTypeDict']=True
    
    List = gConfig.getValue( "%s/histTypeList"%options ) 
    if List:
      l = string.join(List.split(),"")
      self.histTypeList = l.split(",")
      Configuration['histTypeList']=True
    
    List = gConfig.getValue( "%s/ProcessingPasses"%options ) 
    self.ProcessingPasses=[]
    if List:
      l = string.join(List.split(),"")
      temp = l.split(",")
      for ll in temp:
        if ll.find('RealData'):
          ll = ll.replace('RealData','Real Data')
          self.ProcessingPasses.append(ll)
      Configuration['ProcessingPasses']=True

    
    List = gConfig.getValue( "%s/DataTakingConditions"%options ) 
    if List:
      l = string.join(List.split(),"")
      self.DataTakingConditions = l.split(",")
      Configuration['DataTakingConditions']=True


    self.testMode = gConfig.getValue( "%s/testMode"%options )
    if self.testMode: Configuration['testMode']=True
    self.specialMode = gConfig.getValue( "%s/specialMode"%options )
    if self.specialMode: Configuration['specialMode']=True
    Runs = gConfig.getValue( "%s/specialRuns"%options )
    if Runs: 
      l = string.join(Runs.split(),"")
      self.specialRuns = l.split(",")
    Configuration['specialRuns']=True
   
    #If one of the configuration is missing the initialization fails
    nConf=False
    for confVar in Configuration:
      if not Configuration[confVar]:
        gLogger.error('%s not specified in CS'% confVar)
        gLogger.error('---------------------------------')
        nConf=True
    if nConf: DIRAC.exit(2) 

    gLogger.info('=====All informations from dirac.cfg retrieved=====')     
    
    
    env = dict(os.environ)
    #Need to set a user to let getProjectEnvironment run correctly. 
    env['USER']='dirac'
    res = getProjectEnvironment(self.systemConfiguration, self.applicationName, applicationVersion = '', extraPackages = '',
                                runTimeProject = '', site = '', directory = '', generatorName = '',
                                poolXMLCatalogName = 'pool_xml_catalog.xml', env = env )

    #If the environment cannot be setup the init fails
    if not res['OK']:
      gLogger.error('===== Cannot create the environment check LocalArea or SharedArea in dirac.cfg =====')
      DIRAC.exit( 2 )
    
    self.environment = res['Value']
    
    self.logFileName = ''

    self.logFile = ''
    
    self.bkClient = BookkeepingClient()
 
    evtTypeId = int( self.evtTypeDict[self.thisEventType] )
    
    self.exitStatus = 2

    return S_OK()

  def execute( self ):
    
    rootVersion="5.32.00"
    
    evtTypeRef={}
    #Conversion table for the stream names
    evtTypeRef['90000000']='Full stream'
    evtTypeRef['91000000']='Express stream'
    evtTypeRef['93000000']='Luminosity stream online'
    evtTypeRef['95000000']='Calib stream'
    evtTypeRef['96000000']='NoBias stream'
    evtTypeRef['97000000']='Beam Gas'
    
    #The bkqueries for brunel and davinci are the same except for the filetype
    bkDict_brunel={}
    bkDict_davinci={}
    lfns={}

    #Loop over all the possible bk queries the can be build given the configurations present in dirac.cfg
    for n in self.cfgNameList:
      gLogger.info(self.cfgNameList)
      for v in self.cfgVersionList:
        gLogger.info(self.cfgVersionList)
        for e in self.evtTypeDict.values():
          gLogger.info(e)
          for p in self.ProcessingPasses:
            gLogger.info(p)
            for d in self.DataTakingConditions:
              gLogger.info(d)
              for q in self.dqFlagList:
                gLogger.info(q)
                bkDict_brunel[ 'ConfigName' ] = n
                bkDict_brunel[ 'ConfigVersion' ] = v
                bkDict_brunel[ 'EventTypeId' ] = e
                bkDict_brunel['FileType'] = self.histTypeList[0]
                bkDict_brunel[ 'EventTypeDescription' ] = evtTypeRef[e]
                bkDict_brunel[ 'ProcessingPass' ] = p
                bkDict_brunel['DataTakingConditions'] = d
                bkDict_brunel['DataQualityFlag'] = q
                
                
                
                gLogger.info('==================================================')
                gLogger.info('=====Retrieving the list of runs with bkquery=====')
                gLogger.info('==================================================')
                gLogger.info(str(bkDict_brunel))
                gLogger.info('==================================================')
                gLogger.info('==================================================')
                #Retrieve the of file and order them per run
                res_0 = GetRuns(bkDict_brunel,self.bkClient)
                gLogger.info("Numer of Runs found %s"%len(res_0.keys()))
                
                bkDict_davinci = bkDict_brunel 
                
                bkDict_davinci['FileType'] = self.histTypeList[1]

                gLogger.info('==================================================')
                gLogger.info('=====Retrieving the list of runs with bkquery=====')
                gLogger.info('==================================================')
                gLogger.info(str(bkDict_davinci))
                gLogger.info('==================================================')
                gLogger.info('==================================================')

                #Retrieve the of file and order them per run
                res_1 = GetRuns(bkDict_davinci,self.bkClient)
                gLogger.info("Number of Runs found %s"%len(res_1.keys()))

                #If the number of BrunelHist and DaVinciHist is not equal it skips the run
                if len(res_0.keys())==0 or len(res_1.keys())==0:
                  gLogger.error( 'No BRUNELHIST or DAVINCIHIST present for this bkQuery. ' )
                else:
                  for run in res_0.keys():
                    ID = self.bkClient.getProcessingPassId(bkDict_brunel[ 'ProcessingPass' ])
                    q = self.bkClient.getRunFlag( run, ID['Value'] )
                    if q['OK']:
                      gLogger.info("Run %s has already been flagged skipping")
                      continue
                    lfns=BuildLFNs(bkDict_brunel,run)
                    #If the LFN is already in the BK it will be skipped
                    res = self.bkClient.getFileMetadata([lfns['DATA']])
                    if res['Value']:
                      gLogger.info("%s already in the bookkeeping. Continue with the other runs."%str(lfns['DATA']))
                      continue
                    #log directory that will be uploaded 
                    logDir = self.homeDir + 'MERGEFORDQ_RUN_'+str(run)
                    if not os.path.exists(logDir):
                      os.makedirs(logDir)
                      
                    self.logFileName = '%s/Brunel_DaVinci_%s.log' % (logDir , run )
                    
                    gLogger.info('Log file path for application execution %s'%self.logFileName)
                      
                    self.logFile = open( self.logFileName, 'w+' ) 

                    gLogger.info('Starting Merging Process for run %s'%run)
                    gLogger.info ('Runs to be skipped %s'%self.specialRuns)
                    if str(run) in self.specialRuns:
                      continue
                    if not (len(res_0[run]['LFNs']) == len(res_1[run]['LFNs'])):
                      gLogger.error("Different number of BRUNELHIST and DAVINCIHIST. Skipping run %s"%run)
                      continue
                    #Starting the three step Merging process
                    res = MergeRun( bkDict_brunel, res_0, res_1, run, self.bkClient, self.homeDir , self.testMode, self.specialMode , 
                                    self.mergeExeDir , self.mergeStep1Command, self.mergeStep2Command,self.mergeStep3Command,
                                    self.brunelCount ,self.daVinciCount , self.logFile , self.logFileName,self.environment)
                    #if the Merging process went fine the Finalization method is called
                    if res['Merged']:
                      inputData = res_0[run]['LFNs'] + res_1[run]['LFNs']
                      '''
                      Finalization Step:
                      '''
                      #gLogger.info("Finalization")
                      #continue
                      res = Finalization(self.homeDir,logDir,lfns,res['OutputFile'],('Brunel_DaVinci_%s.log'%run),inputData,run,bkDict_brunel,rootVersion)
                      if res['OK']:
                        '''
                        If the finalization went fine an automated message is sent to the revelant mailing list specified in the cfg.
                        '''
                        outMess = "**********************************************************************************************************************************\n"
                        outMess = outMess + "\nThis is an automatic message:\n"
                        outMess = outMess +"\n**********************************************************************************************************************************\n"
                        outMess = outMess +'\nRun: %s\n Processing Pass: %s\n Stream: %s\n DataTaking Conditions: %s\n\n' %(str(run), p, evtTypeRef[e], d)
                        xmldoc = minidom.parse(res['XML'])
                        node = xmldoc.getElementsByTagName('Replica')[0]
                        web = node.attributes['Name'].nodeValue
                        outMess = outMess + '\nROOT file LFN: %s' %(lfns['DATA'])+'\n'
                        outMess = outMess + '\nLocation of logfile %s\n'%str(web)
                        outMess = outMess +"\n**********************************************************************************************************************************"
                        notifyClient = NotificationClient()
                        subject      = 'New %s merged ROOT file for run %s ready' %(evtTypeRef[e], str(run))
                          
                        res = notifyClient.sendMail(self.mailAddress, subject, outMess,self.senderAddress, localAttempt=False)
                          
                        res = notifyClient.sendMail('falabella@fe.infn.it', subject, outMess,self.senderAddress, localAttempt=False)

                        #Cleaning of all the local files and directories.
                        removal = self.homeDir+'*'
                        r = glob.glob(removal)
                        for i in r:
                          if os.path.isdir(i) == True:
                            shutil.rmtree(i)
                          else:
                            os.remove(i)
                      else:
                        removal = self.homeDir+'*'
                        r = glob.glob(removal)
                        for i in r:
                          if os.path.isdir(i) == True:
                            shutil.rmtree(i)
                          else:
                            os.remove(i)
                        DIRAC.exit(2)
                      
                  else: gLogger.error( 'The number of runs selected by the bkQuery for %s in diffent from the one selected by the bkQuery for %s'%(self.histTypeList[0],self.histTypeList[1]) )
                        
    if self.exitStatus:
      gLogger.info( 'No file available for the selected bkQueries' )
    
    return S_OK()