""" Put doc here
"""

import DIRAC
from DIRAC                                                     import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.AgentModule                               import AgentModule
from LHCbDIRAC.DataManagementSystem.Utilities.MergeForDQ       import *
from DIRAC.Core.Base                                           import Script
from DIRAC.Interfaces.API.Dirac                                import Dirac
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient   import BookkeepingClient
import re

__RCSID__ = "$Id:"

AGENT_NAME = 'DataManagement/MergingForDQAgent'

class MergingForDQAgent( AgentModule ):

  def initialize( self ):
    self.am_setOption( 'shifterProxy', 'DataManager' )
    # put these into CS (or remove completely!)
    self.homeDir = '/afs/cern.ch/lhcb/group/dataquality/ROOT'
    self.testDir = '/afs/cern.ch/lhcb/group/dataquality/Test'
    self.workDir = '/afs/cern.ch/lhcb/group/dataquality/Work'
    self.scriptsDir = '/afs/cern.ch/lhcb/group/dataquality/scripts'#TO BE CHANGED
    # DIRAC can build these paths for you (e.g.: no need to do it here)
    self.castorHistDir = '/castor/cern.ch/grid'
    self.castorHistPre = 'castor://castorlhcb.cern.ch:9002//castor/cern.ch/grid'
    self.castorHistPost = '?svcClass=lhcbdisk&castorVersion=2'
    self.cfgName = "LHCb"
    self.cfgVersion = 'Collision11'
    self.thisEventType = 'EXPRESS'
    self.dqFlag = 'UNCHECKED'
    self.evtTypeList = {'EXPRESS' : '91000000', 'FULL'    : '90000000'}
    self.histTypeList = ['BRUNELHIST', 'DAVINCIHIST']
    self.brunelCount = 0
    self.daVinciCount = 0
    # can upload this log in the logSE (look for uploadLogFile module)
    self.logFileName = '%s/logs/Merge_%s_histograms.log' % ( self.scriptsDir, self.thisEventType )
    # no need?
    self.args = Script.getPositionalArgs()
    # ?
    self.checkType = 'DATA'
#    self.testMode = False
#    self.specialMode = False
#    self.specialRuns = {}
    self.mergeExeDir = '/afs/cern.ch/lhcb/group/dataquality/adinolfi'#TO BE CHANGED
    self.mergeStep1Command = self.mergeExeDir + '/Merge1'
    self.mergeStep2Command = self.mergeExeDir + '/Merge2'
    self.mergeStep3Command = self.mergeExeDir + '/Merge3'
    self.senderAddress = 'marco.adinolfi@cern.ch'
    self.mailAddress = 'lhcb-dataquality-shifters@cern.ch'
    #self.senderAddress = 'falabella@fe.infn.it'
    #self.mailAddress = 'falabella@fe.infn.it'
    self.exitStatus = 0

    return S_OK()

  def execute( self ):
    if re.search( 'FULL', Script.scriptName ):
      self.dqFlag = 'EXPRESS_OK'
      self.thisEventType = 'FULL'
    if re.search( 'VALIDATION', Script.scriptName ):
      self.checkType = 'VALIDATION'
      self.homeDir = self.homeDir + '/Validation'

    self.logFileName = '%s/logs/Merge_%s_histograms.log' % ( self.scriptsDir, self.thisEventType )
    logFile = open( self.logFileName, 'a' )
    logFile = None

    if len( self.args ):
      if self.args[0] == "test":
        self.testMode = True
      elif self.args[0] == "special":
        self.specialMode = True
        if not len( self.args ) >= 2:
          print 'You need to pass the run number in special mode'
          DIRAC.exit( -1 )
        for arg in self.args:
          if arg == "special":
            continue
          self.specialRuns[arg] = True
        gLogger.info( 'Special Mode selected, will download run %s only' % ( self.specialRuns.keys() ) )

    evtTypeId = int( self.evtTypeList[self.thisEventType] )

    dirac = Dirac()
    bkClient = BookkeepingClient()
    '''
    Load the configuration names and versions we're interested in.
    '''
    if self.checkType == 'VALIDATION':
      self.cfgName = 'validation'

    bkTree = {self.cfgName : {}}
    bkDict = {'ConfigName' : self.cfgName}
    allConfig = bkClient.getAvailableConfigurations()
    for i in range( len( allConfig['Value'] ) ):
      if allConfig['Value'][i][0] == self.cfgName:
        if re.search( self.cfgVersion, allConfig['Value'][i][1] ):
          self.cfgVersion = allConfig['Value'][i][1]
          if not bkTree[self.cfgName].has_key( self.cfgVersion ):
            bkTree[self.cfgName][self.cfgVersion] = {}

    bkTree, res = GetRunningConditions( bkTree , bkClient )
    if not res['OK']:
      outMess = 'Running Conditions not found'
      gLogger.error( outmess )
      DIRAC.exit( 2 )

    bkTree, res = GetProcessingPasses( bkTree , bkClient )
    if not res['OK']:
        DIRAC.exit( 2 )

    bkDict , res = GetRuns( bkTree, bkClient, self.thisEventType,
                            self.evtTypeList, self.dqFlag )
    res = MergeRun( bkDict, eventType , histTypeList , bkClient , homeDir , testDir , testMode,
                    specialMode , mergeExeDir , mergeStep1Command, mergeStep2Command,
                    mergeStep3Command, castorHistPre, castorHistPost , workDir, brunelCount ,
                    daVinciCount , logFile , self.logFileName , dirac , self.senderAddress, self.mailAddress )

    if not res['OK']:
      gLogger.error( 'Cannot load the run list for version %s' % ( cfgVersion ) )
      gLogger.error( res['Message'] )
      DIRAC.exit( 2 )

    return S_OK()

