#__RCSID__ = "$Id:  $"
#
#import unittest, itertools, copy
#import re
#import DIRAC
#from LHCbDIRAC.Core.Utilities.ProductionData import constructProductionLFNs, _makeProductionLFN, _applyMask, getLogPath
#from LHCbDIRAC.DataManagementSystem.Utilities.MergeForDQ import *
#from DIRAC                                               import gConfig
#from LHCbDIRAC.Core.Utilities.ProductionEnvironment import getProjectEnvironment
#from LHCbDIRAC.Core.Utilities.ProductionEnvironment import getScriptsLocation
#from LHCbDIRAC.Workflow.Modules.RootApplication import RootApplication
#from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient   import BookkeepingClient
#from DIRAC.Core.Base.Script import parseCommandLine
#from DIRAC.Interfaces.API.Dirac import Dirac
#import subprocess
#from subprocess import PIPE
#from mock import Mock
#
#
#class UtilitiesTestCase( unittest.TestCase ):
#  """ Base class for the Utilities test cases
#  """
#  def setUp( self ):
#
#    pass
#
#class MergeForDQ_GetRunningConditions( UtilitiesTestCase ):
#  def test_GetRunningConditions( self ):
#    allConfigurationsValue = [( 'CCRC08', 'Test' ), ( 'ECAL', 'Physics|timescan' ),
#                            ( 'ECAL', 'Test' ), ( 'FEST', '2009' ), ( 'FEST', 'Fest' ),
#                            ( 'Fest', 'Fest' ), ( 'Fest', 'Test' ), ( 'HCAL', 'Physics|timescan' ),
#                            ( 'HCAL', 'Test' ), ( 'IT', 'Physics' ), ( 'IT', 'Physics|pulseshape_scan' ),
#                            ( 'IT', 'Test' ), ( 'It', 'Test' ), ( 'LHCb', 'BEAM1' ), ( 'LHCb', 'Beam' ),
#                            ( 'LHCb', 'Beam1' ), ( 'LHCb', 'Bxidtest' ), ( 'LHCb', 'CCRC' ),
#                            ( 'LHCb', 'CCRC08' ), ( 'LHCb', 'COLLISION10' ),
#                            ( 'LHCb', 'Calibration10' ), ( 'LHCb', 'Calibration11' ),
#                            ( 'LHCb', 'Ccrc' ), ( 'LHCb', 'Collision09' ), ( 'LHCb', 'Collision10' ),
#                            ( 'LHCb', 'Collision11' ), ( 'LHCb', 'Cosmics' ), ( 'LHCb', 'DC06' ),
#                            ( 'LHCb', 'Physics' ), ( 'LHCb', 'Physics_cosmics' ),
#                            ( 'LHCb', 'Physicsntp' ), ( 'LHCb', 'Physicstp' ),
#                            ( 'LHCb', 'Physicstp_lcmsonly' ), ( 'LHCb', 'Physicstp_low' ),
#                            ( 'LHCb', 'Physicstp_mcmsonly' ), ( 'LHCb', 'Physicstp_nocms' ),
#                            ( 'LHCb', 'Physics|cosmics' ), ( 'LHCb', 'Ted' ),
#                            ( 'LHCb', 'Test' ), ( 'LHCb', 'Test|vfs400_notp' ),
#                            ( 'LHCb', 'Test|vfs400_tpall' ), ( 'LHCb', 'Ttcrxscan' ),
#                            ( 'Lhcb', 'Collision09' ), ( 'Lhcb', 'Collision10' ),
#                            ( 'Lhcb', 'Cosmics' ), ( 'Lhcb', 'Ted' ), ( 'Lhcb', 'Test' ),
#                            ( 'MC', '2007' ), ( 'MC', '2008' ), ( 'MC', '2008-HT' ),
#                            ( 'MC', '2008-LT' ), ( 'MC', '2008-QGSP' ), ( 'MC', '2009' ),
#                            ( 'MC', '2010' ), ( 'MC', '2011' ), ( 'MC', 'DC06' ), ( 'MC', 'FEST' ),
#                            ( 'MC', 'MC09' ), ( 'MC', 'MC10' ), ( 'MC', 'TEST' ),
#                            ( 'MC', 'Upgrade' ), ( 'MC', 'test' ), ( 'MUON', 'Physics' ),
#                            ( 'MUON', 'Physics?cosmics' ), ( 'MUON', 'Physics_tae' ),
#                            ( 'MUON', 'Physics|tae' ), ( 'MUON', 'Test' ), ( 'MUONA', 'Test' ),
#                            ( 'MUONC', 'Physics' ), ( 'Muon', 'Test' ), ( 'OT', 'TEST' ),
#                            ( 'OT', 'Test' ), ( 'OTA', 'Test' ), ( 'OTC', 'Physics' ),
#                            ( 'OTC', 'Test' ), ( 'Ot', 'Test' ), ( 'PRS', 'Physics' ),
#                            ( 'PRS', 'Test' ), ( 'RICH', 'Beam' ), ( 'RICH', 'Physics' ),
#                            ( 'RICH', 'TEST' ), ( 'RICH', 'Test' ), ( 'RICH1', 'Alice' ),
#                            ( 'RICH1', 'Alice|testpulse' ), ( 'RICH1', 'Physics' ),
#                            ( 'RICH1', 'TEST' ), ( 'RICH1', 'Test' ), ( 'RICH2', 'Alice' ),
#                            ( 'RICH2', 'Calib' ), ( 'RICH2', 'Physics' ), ( 'RICH2', 'Test' ),
#                            ( 'Rich', 'Test' ), ( 'TDET', 'TEST' ), ( 'TDET', 'Test' ),
#                            ( 'TPU_ECS', 'Physics' ), ( 'TPU_ECS', 'Test' ),
#                            ( 'TT', 'Adctiming_scan' ), ( 'TT', 'Cosmics' ), ( 'TT', 'Default' ),
#                            ( 'TT', 'Pedestal_scan' ), ( 'TT', 'Physics' ),
#                            ( 'TT', 'Pulseshape_scan' ), ( 'TT', 'Pulseshape_scanvfs100' ),
#                            ( 'TT', 'Pulseshape_scanvfs1000' ),
#                            ( 'TT', 'Pulseshape_scanvfs700' ),
#                            ( 'TT', 'Ted' ), ( 'TT', 'Test' ), ( 'TT', 'Test_tp8' ),
#                            ( 'TT', 'Test_vfs400' ), ( 'TT', 'Test_vfs400_notp' ),
#                            ( 'TT', 'Test|vfs400' ), ( 'TT', 'Test|vfs400_notp' ),
#                            ( 'TT', 'Vfs400' ), ( 'TT', 'Vfs400_notp' ),
#                            ( 'Tt', 'Default' ), ( 'Tt', 'Ted' ), ( 'Tt', 'Test' ),
#                            ( 'VELO', 'Adcdelayscan' ), ( 'VELO', 'Adcgainscan' ),
#                            ( 'VELO', 'Badstripscan' ), ( 'VELO', 'CCESCAN' ),
#                            ( 'VELO', 'Ccescan' ), ( 'VELO', 'Collision' ),
#                            ( 'VELO', 'Default' ), ( 'VELO', 'Delayscan' ),
#                            ( 'VELO', 'Fakebeamscan' ), ( 'VELO', 'Highoccupancynzs' ),
#                            ( 'VELO', 'Highratetest' ), ( 'VELO', 'Hvscan' ), ( 'VELO', 'NZS' ),
#                            ( 'VELO', 'Nzs' ), ( 'VELO', 'Pedbank' ), ( 'VELO', 'Physics' ),
#                            ( 'VELO', 'Physics_eb' ), ( 'VELO', 'Physicsntp' ),
#                            ( 'VELO', 'Physicsntp_eb' ), ( 'VELO', 'Physicstp' ),
#                            ( 'VELO', 'Pulseshapescan' ), ( 'VELO', 'Ted' ),
#                            ( 'VELO', 'Tedfir' ), ( 'VELO', 'Test' ), ( 'VELO', 'Testpulsenzs' ),
#                            ( 'VELO', 'Testpulsescan' ), ( 'VELO', 'Timingscan' ),
#                            ( 'VELO', 'Tpheightscan' ), ( 'VELO', 'Ttcrxscan' ),
#                            ( 'VELO', 'Velodev' ), ( 'VELO', 'Zs' ),
#                            ( 'VELOA', 'Adcdelayscan' ), ( 'VELOA', 'Adcgainscan' ),
#                            ( 'VELOA', 'CCESCAN' ), ( 'VELOA', 'Ccescan' ),
#                            ( 'VELOA', 'Delayscan' ), ( 'VELOA', 'Nzs' ), ( 'VELOA', 'Physics' ),
#                            ( 'VELOA', 'Physicsntp' ), ( 'VELOA', 'Physicstp' ),
#                            ( 'VELOA', 'Physicstp_mcmsonly' ), ( 'VELOA', 'Testpulsenzs' ),
#                            ( 'VELOA', 'Testpulsescan' ), ( 'VELOA', 'Ttcrxscan' ),
#                            ( 'VELOC', 'Adcdelayscan' ), ( 'VELOC', 'Adcgainscan' ),
#                            ( 'VELOC', 'Bitperfect' ), ( 'VELOC', 'Collision' ),
#                            ( 'VELOC', 'Delayscan' ), ( 'VELOC', 'Fakebeamscan' ),
#                            ( 'VELOC', 'Highratetest' ), ( 'VELOC', 'L0acceptscan' ),
#                            ( 'VELOC', 'L0feresetscan' ), ( 'VELOC', 'Nzs' ),
#                            ( 'VELOC', 'Physics' ), ( 'VELOC', 'Physicsntp' ),
#                            ( 'VELOC', 'Physicstp' ), ( 'VELOC', 'Testpulsenzs' ),
#                            ( 'VELOC', 'Testpulsescan' ), ( 'VELOC', 'Ttcrxscan' ),
#                            ( 'VELOC', 'Velodev' ), ( 'Velo', 'Nzs' ),
#                            ( 'Velo', 'Testpulsenzs' ), ( 'Velo', 'Testpulsescan' ),
#                            ( 'Velo', 'Velodev' ), ( 'Velo', 'Zs' ),
#                            ( 'Veloa', 'Testpulsescan' ), ( 'certification', '2009' ),
#                            ( 'certification', '2010' ), ( 'certification', 'MC09' ),
#                            ( 'certification', 'OnlineTest' ), ( 'certification', 'test' ),
#                            ( 'newproduction', 'MC09' ), ( 'test', '2008' ), ( 'test', '2009' ),
#                            ( 'test', 'DC06' ), ( 'test', 'MC09' ), ( 'test', 'test' ),
#                            ( 'validation', 'Collision10' ),
#                            ( 'validation', 'Collision11' )]
#
#    allConfigurationsValueCheated = [( 'CCRC08', 'Test' ), ( 'ECAL', 'Physics|timescan' ),
#                            ( 'ECAL', 'Test' ), ( 'FEST', '2009' ), ( 'FEST', 'Fest' ),
#                            ( 'Fest', 'Fest' ), ( 'Fest', 'Test' ), ( 'HCAL', 'Physics|timescan' ),
#                            ( 'HCAL', 'Test' ), ( 'IT', 'Physics' ), ( 'IT', 'Physics|pulseshape_scan' ),
#                            ( 'IT', 'Test' ), ( 'It', 'Test' ), ( 'LHCb', 'BEAM1' ), ( 'LHCb', 'Beam' ),
#                            ( 'LHCb', 'Beam1' ), ( 'LHCb', 'Bxidtest' ), ( 'LHCb', 'CCRC' ),
#                            ( 'LHCb', 'CCRC08' ), ( 'LHCb', 'COLLISION10' ),
#                            ( 'LHCb', 'Calibration10' ), ( 'LHCb', 'Calibration11' ),
#                            ( 'LHCb', 'Ccrc' ), ( 'LHCb', 'Collision09' ),
#                            ( 'LHCb', 'Collision10' ), ( 'LHCb', 'Cosmics' ),
#                            ( 'LHCb', 'DC06' ),
#                            ( 'LHCb', 'Physics' ), ( 'LHCb', 'Physics_cosmics' ),
#                            ( 'LHCb', 'Physicsntp' ), ( 'LHCb', 'Physicstp' ),
#                            ( 'LHCb', 'Physicstp_lcmsonly' ), ( 'LHCb', 'Physicstp_low' ),
#                            ( 'LHCb', 'Physicstp_mcmsonly' ), ( 'LHCb', 'Physicstp_nocms' ),
#                            ( 'LHCb', 'Physics|cosmics' ), ( 'LHCb', 'Ted' ),
#                            ( 'LHCb', 'Test' ), ( 'LHCb', 'Test|vfs400_notp' ),
#                            ( 'LHCb', 'Test|vfs400_tpall' ), ( 'LHCb', 'Ttcrxscan' ),
#                            ( 'Lhcb', 'Collision09' ), ( 'Lhcb', 'Collision10' ),
#                            ( 'Lhcb', 'Cosmics' ), ( 'Lhcb', 'Ted' ), ( 'Lhcb', 'Test' ),
#                            ( 'MC', '2007' ), ( 'MC', '2008' ), ( 'MC', '2008-HT' ),
#                            ( 'MC', '2008-LT' ), ( 'MC', '2008-QGSP' ), ( 'MC', '2009' ),
#                            ( 'MC', '2010' ), ( 'MC', '2011' ), ( 'MC', 'DC06' ), ( 'MC', 'FEST' ),
#                            ( 'MC', 'MC09' ), ( 'MC', 'MC10' ), ( 'MC', 'TEST' ),
#                            ( 'MC', 'Upgrade' ), ( 'MC', 'test' ), ( 'MUON', 'Physics' ),
#                            ( 'MUON', 'Physics?cosmics' ), ( 'MUON', 'Physics_tae' ),
#                            ( 'MUON', 'Physics|tae' ), ( 'MUON', 'Test' ), ( 'MUONA', 'Test' ),
#                            ( 'MUONC', 'Physics' ), ( 'Muon', 'Test' ), ( 'OT', 'TEST' ),
#                            ( 'OT', 'Test' ), ( 'OTA', 'Test' ), ( 'OTC', 'Physics' ),
#                            ( 'OTC', 'Test' ), ( 'Ot', 'Test' ), ( 'PRS', 'Physics' ),
#                            ( 'PRS', 'Test' ), ( 'RICH', 'Beam' ), ( 'RICH', 'Physics' ),
#                            ( 'RICH', 'TEST' ), ( 'RICH', 'Test' ), ( 'RICH1', 'Alice' ),
#                            ( 'RICH1', 'Alice|testpulse' ), ( 'RICH1', 'Physics' ),
#                            ( 'RICH1', 'TEST' ), ( 'RICH1', 'Test' ), ( 'RICH2', 'Alice' ),
#                            ( 'RICH2', 'Calib' ), ( 'RICH2', 'Physics' ), ( 'RICH2', 'Test' ),
#                            ( 'Rich', 'Test' ), ( 'TDET', 'TEST' ), ( 'TDET', 'Test' ),
#                            ( 'TPU_ECS', 'Physics' ), ( 'TPU_ECS', 'Test' ),
#                            ( 'TT', 'Adctiming_scan' ), ( 'TT', 'Cosmics' ), ( 'TT', 'Default' ),
#                            ( 'TT', 'Pedestal_scan' ), ( 'TT', 'Physics' ),
#                            ( 'TT', 'Pulseshape_scan' ), ( 'TT', 'Pulseshape_scanvfs100' ),
#                            ( 'TT', 'Pulseshape_scanvfs1000' ),
#                            ( 'TT', 'Pulseshape_scanvfs700' ),
#                            ( 'TT', 'Ted' ), ( 'TT', 'Test' ), ( 'TT', 'Test_tp8' ),
#                            ( 'TT', 'Test_vfs400' ), ( 'TT', 'Test_vfs400_notp' ),
#                            ( 'TT', 'Test|vfs400' ), ( 'TT', 'Test|vfs400_notp' ),
#                            ( 'TT', 'Vfs400' ), ( 'TT', 'Vfs400_notp' ),
#                            ( 'Tt', 'Default' ), ( 'Tt', 'Ted' ), ( 'Tt', 'Test' ),
#                            ( 'VELO', 'Adcdelayscan' ), ( 'VELO', 'Adcgainscan' ),
#                            ( 'VELO', 'Badstripscan' ), ( 'VELO', 'CCESCAN' ),
#                            ( 'VELO', 'Ccescan' ), ( 'VELO', 'Collision' ),
#                            ( 'VELO', 'Default' ), ( 'VELO', 'Delayscan' ),
#                            ( 'VELO', 'Fakebeamscan' ), ( 'VELO', 'Highoccupancynzs' ),
#                            ( 'VELO', 'Highratetest' ), ( 'VELO', 'Hvscan' ), ( 'VELO', 'NZS' ),
#                            ( 'VELO', 'Nzs' ), ( 'VELO', 'Pedbank' ), ( 'VELO', 'Physics' ),
#                            ( 'VELO', 'Physics_eb' ), ( 'VELO', 'Physicsntp' ),
#                            ( 'VELO', 'Physicsntp_eb' ), ( 'VELO', 'Physicstp' ),
#                            ( 'VELO', 'Pulseshapescan' ), ( 'VELO', 'Ted' ),
#                            ( 'VELO', 'Tedfir' ), ( 'VELO', 'Test' ), ( 'VELO', 'Testpulsenzs' ),
#                            ( 'VELO', 'Testpulsescan' ), ( 'VELO', 'Timingscan' ),
#                            ( 'VELO', 'Tpheightscan' ), ( 'VELO', 'Ttcrxscan' ),
#                            ( 'VELO', 'Velodev' ), ( 'VELO', 'Zs' ),
#                            ( 'VELOA', 'Adcdelayscan' ), ( 'VELOA', 'Adcgainscan' ),
#                            ( 'VELOA', 'CCESCAN' ), ( 'VELOA', 'Ccescan' ),
#                            ( 'VELOA', 'Delayscan' ), ( 'VELOA', 'Nzs' ), ( 'VELOA', 'Physics' ),
#                            ( 'VELOA', 'Physicsntp' ), ( 'VELOA', 'Physicstp' ),
#                            ( 'VELOA', 'Physicstp_mcmsonly' ), ( 'VELOA', 'Testpulsenzs' ),
#                            ( 'VELOA', 'Testpulsescan' ), ( 'VELOA', 'Ttcrxscan' ),
#                            ( 'VELOC', 'Adcdelayscan' ), ( 'VELOC', 'Adcgainscan' ),
#                            ( 'VELOC', 'Bitperfect' ), ( 'VELOC', 'Collision' ),
#                            ( 'VELOC', 'Delayscan' ), ( 'VELOC', 'Fakebeamscan' ),
#                            ( 'VELOC', 'Highratetest' ), ( 'VELOC', 'L0acceptscan' ),
#                            ( 'VELOC', 'L0feresetscan' ), ( 'VELOC', 'Nzs' ),
#                            ( 'VELOC', 'Physics' ), ( 'VELOC', 'Physicsntp' ),
#                            ( 'VELOC', 'Physicstp' ), ( 'VELOC', 'Testpulsenzs' ),
#                            ( 'VELOC', 'Testpulsescan' ), ( 'VELOC', 'Ttcrxscan' ),
#                            ( 'VELOC', 'Velodev' ), ( 'Velo', 'Nzs' ),
#                            ( 'Velo', 'Testpulsenzs' ), ( 'Velo', 'Testpulsescan' ),
#                            ( 'Velo', 'Velodev' ), ( 'Velo', 'Zs' ),
#                            ( 'Veloa', 'Testpulsescan' ), ( 'certification', '2009' ),
#                            ( 'certification', '2010' ), ( 'certification', 'MC09' ),
#                            ( 'certification', 'OnlineTest' ), ( 'certification', 'test' ),
#                            ( 'newproduction', 'MC09' ), ( 'test', '2008' ), ( 'test', '2009' ),
#                            ( 'test', 'DC06' ), ( 'test', 'MC09' ), ( 'test', 'test' ),
#                            ( 'validation', 'Collision10' ),
#                            ( 'validation', 'Collision11' )]
#    parseCommandLine()
#    #cfgName = 'LHCb'
#    cfgName = 'LHCb'
#    #if checkType == 'VALIDATION':
#    #  cfgName = 'validation'
#    exitStatus = DIRAC
#    bkClient = BookkeepingClient()
#    allConfig = bkClient.getAvailableConfigurations()
#    #print allConfig
#    bkTree = {cfgName : {}}
#    bkDict = {'ConfigName' : cfgName}
#    #print "Number of available configurations", len( allConfigurationsValue )
#    for i in range( len( allConfigurationsValue ) ):
#      #print "Available Conf ", i
#      if allConfigurationsValue[i][0] == cfgName:
#        #print "Found configurations with ", cfgName, " ---> ", allConfigurationsValue[i]
#        if re.search( 'Collision11', allConfigurationsValue[i][1] ):
#          #print "We will use ---> ", allConfigurationsValue[i]
#          cfgVersion = allConfigurationsValue[i][1]
#          if not bkTree[cfgName].has_key( cfgVersion ):
#            bkTree[cfgName][cfgVersion] = {}
#
#    bkTree, res = GetRunningConditions( bkTree , bkClient )
#    #print "Todos :" , OutPut
#    #print "Bookkeeping Tree : " , bkTree
#    #print "Message : ", res
#    #print "type of res " , type( res )
#
#
#class MergeForDQ_GetProcessingPasses( UtilitiesTestCase ):
#  def test_GetProcessingPasses( self ):
#
#    expectedbkTree = {'LHCb':
#                      {'Collision11': {'Beam1380GeV-VeloOpen-MagUp': {},
#                                       'Beam3500GeV-VeloClosed-MagDown': {},
#                                       'Beam3500GeV-VeloClosed-MagUp': {},
#                                       'Beam1380GeV-VeloOpen-MagDown': {}
#                                       }
#                       }
#                      }
#
#    wrongbkTree = {'Wrong':
#                   {'Collision11': {'Beam1380GeV-VeloOpen-MagUp': {},
#                                    'Beam3500GeV-VeloClosed-MagDown': {},
#                                    'Beam3500GeV-VeloClosed-MagUp': {},
#                                    'Beam1380GeV-VeloOpen-MagDown': {}
#                                    }
#                    }
#                   }
#    wrong2bkTree = {'LHCb':
#                    {'Collision111': {'Beam1380GeV-VeloOpen-MagUp': {},
#                                      'Beam3500GeV-VeloClosed-MagDown': {},
#                                      'Beam3500GeV-VeloClosed-MagUp': {},
#                                      'Beam1380GeV-VeloOpen-MagDown': {}
#                                      }
#                     }
#                    }
#
#
#    parseCommandLine()
#
#    bkTree = {}
#
#    exitStatus = DIRAC
#
#    bkClient = BookkeepingClient()
#
#    bkTree, res = GetProcessingPasses( expectedbkTree , bkClient )
#
#class MergeForDQ_GetRun( UtilitiesTestCase ):
#  def test_GetRun( self ):
#    #bkTree, bkClient , eventType, evtTypeList, dqFlag
#    expectedbkTree = {'LHCb':
#                      {'Collision11':
#                       {'Beam1380GeV-VeloOpen-MagUp': {},
#                        'Beam3500GeV-VeloClosed-MagDown':
#                         {'/Real Data/Reco09': {}, '/Real Data/Reco10': {}},
#                        'Beam3500GeV-VeloClosed-MagUp':
#                         {'/Real Data/Reco09': {}, '/Real Data/Reco10': {}},
#                        'Beam1380GeV-VeloOpen-MagDown':
#                         {'/Real Data/Reco09': {}}}}}
#
#    #eventType = 'EXPRESS'
#    eventType = 'EXPRESS'
#    eventTypeList = {'EXPRESS' : '91000000', 'FULL'    : '90000000'}
#    dqFlag = 'UNCHECKED'
#
#    parseCommandLine()
#
#    bkClient = BookkeepingClient()
#
#    ( bkDict , res ) = GetRuns( expectedbkTree, bkClient, eventType,
#                            eventTypeList, dqFlag )
#    #print "\n=====================================>bkDict After GetRuns",bkDict
#
#class MergeForDQ_Merge( UtilitiesTestCase ):
#  '''
#  This class can test the MergeRun function issueing a bkDict that corresponds to data present in the BK.
#  The directories must be changed accoding to the access rights of who is testing.
#  Most important, the castor upload directory must be own by who have the credentials to access BK, if not it should at
#  least be part of the same group.
#  '''
#  def test_Merge( self ):
#    parseCommandLine()
#    bkDict = {'StartRun': 81811, 'ConfigName': 'LHCb',
#              'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown-Excl-R2',
#              'EndRun': 81811, 'EventType': 90000000,
#              'ProcessingPass': '/Real Data/Reco07/Stripping11',
#              'ConfigVersion': 'Collision10',
#              'DataQualityFlag': 'OK'}
#
#    eventType = 'EXPRESS'
#    eventTypeList = {'EXPRESS' : '91000000', 'FULL'    : '90000000'}
#    dqFlag = 'UNCHECKED'
#    histTypeList = ['BRUNELHIST', 'DAVINCIHIST']
#
#    homeDir = '/afs/cern.ch/user/a/afalabel/test_MergeForDQAgent/ROOT'#testdirs
#    testDir = '/afs/cern.ch/user/a/afalabel/test_MergeForDQAgent/Test'#testdirs
#    workDir = '/afs/cern.ch/user/a/afalabel/test_MergeForDQAgent/Work'#testdirs
#    scriptsDir = '/afs/cern.ch/user/a/afalabel/test_MergeForDQAgent/scripts'#testdirs
#    mergeExeDir = '/afs/cern.ch/user/a/afalabel/test_MergeForDQAgent/exedir'#TO BE CHANGED
#    mergeStep1Command = mergeExeDir + '/Merge'
#    mergeStep2Command = mergeExeDir + '/Merge2'
#    mergeStep3Command = mergeExeDir + '/Merge3'
#    logFileName = '%s/logs/Merge_%s_histograms.log' % ( scriptsDir, 'EXPRESS' )
##    castorHistDir = '/castor/cern.ch/grid'
##    castorHistPre = 'castor://castorlhcb.cern.ch:9002//castor/cern.ch/grid'
##    castorHistPost='?svcClass=lhcbdisk&castorVersion=2'
#    #castorHistDir = 'castor://castorlhcb.cern.ch:9002//castor/cern.ch/grid'
#    castorHistPre = 'castor://castorlhcb.cern.ch:9002//castor/cern.ch/grid/lhcb/user/a/afalabel'
#    castorHistPost = '?svcClass=lhcbdisk&castorVersion=2'
#
#    senderAddress = 'falabella@fe.infn.it'
#    mailAddress = 'falabella@fe.infn.it'
#
#    testMode = False
#    specialMode = False
#    brunelCount = 0
#    daVinciCount = 0
#
#
#    logFile = open( logFileName, 'a' )
#
#    dirac = Dirac()
#
#    parseCommandLine()
#
#    bkClient = BookkeepingClient()
#    res = MergeRun( bkDict, eventType , histTypeList , bkClient , homeDir , testDir ,
#                    testMode, specialMode , mergeExeDir , mergeStep1Command,
#                    mergeStep2Command, mergeStep3Command, specialMode, testMode, castorHistPre, castorHistPost ,
#                    workDir , brunelCount , daVinciCount , logFile , logFileName , dirac , senderAddress , mailAddress )
#
#
#
#
#class MergeForDQ_MergeStep2( UtilitiesTestCase ):
#  def test_MergeStep2( self ):
#    scriptsDir = '/afs/cern.ch/user/a/afalabel/test_MergeForDQAgent/scripts'#testdirs
#    logFileName = '%s/logs/Merge_%s_histograms.log' % ( scriptsDir, 'EXPRESS' )
#    targetFile = '/afs/cern.ch/user/a/afalabel/test_MergeForDQAgent/Work/Brunel_1190.root'
#    mergeStep2Command = '/afs/cern.ch/user/a/afalabel/test_MergeForDQAgent/exedir/Merge2'
#
##    merge =[72, '/afs/cern.ch/user/a/afalabel/test_MergeForDQAgent/Work/Brunel_1190.root',
##            '/afs/cern.ch/user/a/afalabel/test_MergeForDQAgent/exedir/Merge2',
##            '/afs/cern.ch/user/a/afalabel/test_MergeForDQAgent/Work/Brunel_1190_0.root',
##            '/afs/cern.ch/user/a/afalabel/test_MergeForDQAgent/Work/Brunel_1190_1.root']
#
#    logFile = open( logFileName, 'a' )
#
#    parseCommandLine()
#    stepHist = ['/afs/cern.ch/user/a/afalabel/test_MergeForDQAgent/Work/Brunel_4253_0.root',
#                 '/afs/cern.ch/user/a/afalabel/test_MergeForDQAgent/Work/Brunel_4253_1.root']
#
#    dim = 72
#    #print "logFile ",logFile
#    #print " len( stepHist )",len( stepHist )
#
#    merge = stepHist[0:len( stepHist )]
#    merge.insert( 0, str( dim ) )
#    merge.insert( 0, targetFile )
#    merge.insert( 0, mergeStep2Command )
#
#    #print "Merge----",merge
#
#    p = subprocess.call( merge, stdout = logFile )
#    #p = subprocess.call( merge )
#
#
#
#
#
#class MergeForDQ_OutPut( UtilitiesTestCase ):
#  def test_EnV( self ):
#    parseCommandLine()
#    merge = []
#    histopath = '/afs/cern.ch/user/a/afalabel/private/ROOT/'
#    exe = '/afs/cern.ch/lhcb/group/dataquality/adinolfi/Merge'
#    targetFile = '/afs/cern.ch/user/a/afalabel/private/ROOT/merged_histos.root'
#    logFileName = '/afs/cern.ch/user/a/afalabel/private/ROOT/log.txt'
#    logFile = open( logFileName, 'a' )
#    filename = histopath + 'num1.root'
#    print "\nfilename %s" % ( filename )
#    merge.append( filename )
#    filename = histopath + 'num2.root'
#    merge.append( filename )
#    filename = histopath + 'num3.root'
#    merge.append( filename )
#    merge.insert( 0, targetFile )
#    merge.insert( 0, exe )
#    print "\nmerge %s" % ( merge )
#    p = subprocess.call( merge, stdout = logFile )
#    StreamToLog( logFileName, gLogger, exe )
#
#class MergeForDQ_Merge2( UtilitiesTestCase ):
#  def test_Merge( self ):
#    """ This second class can be used to test the Merge method so it tests ONLY the merging process given whatever bunch
#    of files who tests provide as input.  """
#    homeDir = '/afs/cern.ch/user/a/afalabel/private/ROOT'#testdirs
#    testDir = '/afs/cern.ch/user/a/afalabel/private/ROOT'#testdirs
#    workDir = '/afs/cern.ch/user/a/afalabel/private/ROOT'#testdirs
#    scriptsDir = '/afs/cern.ch/user/a/afalabel/private/ROOT'#testdirs
#    castorHistPre = '/afs/cern.ch/user/a/afalabel/private/ROOT'
#    castorHistPost = ''
#    mergeExeDir = '/afs/cern.ch/user/a/afalabel/test_MergeForDQAgent/exedir'#TO BE CHANGED
#    mergeStep1Command = mergeExeDir + '/Merge'
#    mergeStep2Command = mergeExeDir + '/Merge2'
#    mergeStep3Command = mergeExeDir + '/Merge3'
#    logFileName = '%s/Merge_%s_histograms.log' % ( scriptsDir, 'EXPRESS' )
#    senderAddress = 'falabella@fe.infn.it'
#    mailAddress = 'falabella@fe.infn.it'
#
#    eventType = 'EXPRESS'
#    eventTypeList = {'EXPRESS' : '91000000', 'FULL'    : '90000000'}
#    dqFlag = 'UNCHECKED'
#    histTypeList = ['BRUNELHIST', 'DAVINCIHIST']
#
#    bkDict = {'StartRun': 1, 'ConfigName': 'LHCb',
#              'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown-Excl-R2',
#              'EndRun': 1, 'EventType': 9,
#              'ProcessingPass': '/Real Data/Reco07/Stripping11',
#              'ConfigVersion': 'Collision10',
#              'DataQualityFlag': 'OK'}
#
#
#    testMode = False
#    specialMode = False
#    brunelCount = 0
#    daVinciCount = 0
#    bkClient = BookkeepingClient()
#
#    logFile = open( logFileName, 'a' )
#
#    dirac = Dirac()
#
#    parseCommandLine()
#
#
#    brunelHist = 'BRUNELHIST'
#    daVinciHist = 'DAVINCIHIST'
#    histType = ''
#    runNumber = '1'
#
#    targetFile = '/afs/cern.ch/user/a/afalabel/private/ROOT/Merged.root'
#
#    brunelHist = ['/BRUNEL_1.root',
#                '/BRUNEL_2.root',
#                '/BRUNEL_3.root']
#
#    daVinciHist = ['/DAVINCI_1.root',
#                 '/DAVINCI_2.root',
#                 '/DAVINCI_3.root']
#
#    run = '1'
#    res = Merge( targetFile, run, brunelHist, daVinciHist , mergeExeDir ,
#                 mergeStep1Command, mergeStep2Command, mergeStep3Command,
#                 specialMode, testMode, castorHistPre, castorHistPost, homeDir, workDir ,
#                 brunelCount , daVinciCount , logFile , logFileName , dirac )
#    os.remove( logFileName )
#
#
#
#
#
##############################################################################
## Test Suite run
##############################################################################
#
#if __name__ == '__main__':
#  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UtilitiesTestCase )
#  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeForDQ_GetRunningConditions ) )
#  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeForDQ_GetProcessingPasses ) )
#  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeForDQ_GetRun ) )
#  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeForDQ_Merge ) )
#  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeForDQ_MergeStep2 ) )
#  #suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeForDQ_OutPut ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeForDQ_Merge2 ) )
#  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
#
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
