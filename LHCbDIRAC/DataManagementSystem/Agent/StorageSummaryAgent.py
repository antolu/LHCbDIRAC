########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/WorkloadManagementSystem/Agent/BKInputDataAgent.py $
# File :   StorageSummaryAgent.py
########################################################################

"""   The Storage Summary Agent will create a summary of the 
      storage usage DB grouped by processing pass or other 
      interesting parameters.
      
      Initially this will dump the information to a file but eventually
      can be inserted in a new DB table and made visible via the web portal.
"""

__RCSID__ = "$Id: StorageSummaryAgent.py 31247 2010-12-04 10:32:34Z rgracian $"

from DIRAC.Core.Base.AgentModule                                        import AgentModule
from DIRAC.Core.Utilities.List                                          import sortList,intListToString
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient               import BookkeepingClient
from LHCbDIRAC.DataManagementSystem.Client.StorageUsageClient           import StorageUsageClient

from DIRAC  import gConfig, S_OK, S_ERROR

import sys,re,shutil,os

class StorageSummaryAgent(AgentModule):

  #############################################################################
  def initialize(self):
    """Sets defaults
    """
    self.pollingTime = self.am_setOption('PollingTime',14400)
    self.outputFileName = 'StorageSummaryAgentOutput.txt'
    return S_OK()

  #############################################################################
  def execute(self):
    """The StorageSummaryAgent execution method.
    """    
    if os.path.exists(self.outputFileName):
      shutil.move(self.outputFileName,'%s.backup' %(self.outputFileName))
      
    bkClient = BookkeepingClient()
    suClient = StorageUsageClient()    
    sesOfInterest = ''#['CERN_M-DST']
    #print bkClient.getAvailableConfigNames()
    configName = 'LHCb'
    #print bkClient.getConfigVersions(configName)
    configVersion = 'Collision10'
    res = bkClient.getSimulationConditions(configName,configVersion,1)
    if not res['OK']:
      self.log.error(res)
      return res
    
    dataTakingConds = {}
    for dataTakingTuple in res['Value']:
      dataTakingCondID = dataTakingTuple[0]
      dataTakingCond = dataTakingTuple[1]
      if not dataTakingCond in dataTakingConds.keys():
        dataTakingConds[dataTakingCond] = dataTakingCondID
    
    processingPassTotals = {}
    
    for dataTakingCond in sortList(dataTakingConds.keys()):
      print dataTakingCond
      dataTakingCondID = dataTakingConds[dataTakingCond] 
      res = bkClient.getProPassWithSimCondNew(configName,configVersion,str(dataTakingCondID))
      if not res['OK']:
        self.log.error(dataTakingCond,res['Message'])
        continue
      proPassTuples = res['Value']
      processedProcessingPasses = []
      for proPassTuple in sortList(proPassTuples):
        proPassID = proPassTuple[0]
        proPassName = proPassTuple[1]
    
        if proPassName == 'Real Data':
          continue
        if proPassName in processedProcessingPasses:
          continue
        processedProcessingPasses.append(proPassName)
        res = bkClient.getProductionsWithSimcond(configName, configVersion, str(dataTakingCondID), proPassName, 'ALL')
        if not res['OK']:
          self.log.error(proPassName,res['Message'])        
          continue
        productions = sortList([x[0] for x in res['Value']])
        procPassSummary = "\n\t%s (%s)\n" % (proPassName,intListToString(sortList(productions)))
        self.log.verbose(procPassSummary)
        fopen = open(self.outputFileName,'a')    
        fopen.write(procPassSummary)
        fopen.close()
        prodFileTypeEvents = {}
        my_total = 0
        my_total_perSE = {}
        for prodID in productions:
          #evalString = "bkClient.getFileTypesWithSimcond('%s','%s','%s','ALL','ALL','%s')" % (configName,configVersion,dataTakingCondID,prodID)
          #res = eval(evalString)
          #res = bkClient.getFileTypesWithSimcond(configName,configVersion,str(dataTakingCondID),proPassName,'ALL',str(prodID))
          #if not res['OK']:
          #  print 'ERROR',prodID, res['Message']
          #  continue
          res = bkClient.getNumberOfEvents(prodID)
          if not res['OK']:
            self.log.error(prodID,res['Message'])
            continue
          for eventNumberTuple in res['Value']:
            resFileType,resEvtNumber,resEventType,physEventNumber = eventNumberTuple
            if physEventNumber:
              if re.search('SETC',resFileType):
                resFileType = 'SETC'
              if not prodFileTypeEvents.has_key(resFileType):
                prodFileTypeEvents[resFileType] = 0
              prodFileTypeEvents[resFileType]+=physEventNumber
          prodFileTypes = sortList([x[0] for x in res['Value']])
          #print evalString,prodFileTypes
          for prodFileType in prodFileTypes:
            if re.search('HIST',prodFileType):
              prodFileTypes.remove(prodFileType)
          prodFileTypes.append('HIST')
          if 'DST' not in prodFileTypes:
            prodFileTypes.append('DST')
          if 'SETC' not in prodFileTypes:
            prodFileTypes.append('SETC')
          for prodFileType in prodFileTypes:
            res = suClient.getStorageSummary('/lhcb/data',prodFileType,prodID,sesOfInterest)
            if not res['OK']:
              self.log.error('Error for prod %s file type %s' %(prodID,prodFileType),res['Message'])
              continue
            usageDict = res['Value']
            for seName in sortList(usageDict.keys()):
              files = usageDict[seName]['Files']
              size = usageDict[seName]['Size']
              mysize = size/(1000.0*1000.0*1000.0)
              if not my_total_perSE.has_key(seName):
                my_total_perSE[seName] = {'Size':0}
              my_total_perSE[seName]['Size'] += size
              if not processingPassTotals.has_key(dataTakingCond):
                processingPassTotals[dataTakingCond] = {}
              if not processingPassTotals[dataTakingCond].has_key(proPassName):
                processingPassTotals[dataTakingCond][proPassName] = {}
              if not processingPassTotals[dataTakingCond][proPassName].has_key(prodFileType):
                processingPassTotals[dataTakingCond][proPassName][prodFileType] = {'Files':0,'Size':0,'Events':0}
              processingPassTotals[dataTakingCond][proPassName][prodFileType]['Files'] += files
              processingPassTotals[dataTakingCond][proPassName][prodFileType]['Size'] += size
#        for prodFileType,prodFileTypeEvents in prodFileTypeEvents.items():
#          processingPassTotals[dataTakingCond][proPassName][prodFileType]['Events'] = prodFileTypeEvents
        #print processingPassTotals[dotaTakingCond][proPassName]
        summary = '\n  Totals per SE ( %s )\n   SE                       Size(GB)\n' %(dataTakingCond)
        tot_geral = 0
        for SeTOT in sortList(my_total_perSE.keys()):     # sortList(fileDict.keys()):
          tot_SE = my_total_perSE[SeTOT]['Size']/(1000.0*1000.0*1000.0) 
          tot_geral += tot_SE
          summary+="%s %s\n" % (str(SeTOT).ljust(15), str("%.1f" % (tot_SE)).rjust(20))
          
        summary += "------------------------------------\n"
        summary += "%s %s\n" % (str("TOTAL").ljust(15), str("%.1f" % (tot_geral)).rjust(20))
        self.log.info(summary)
        fopen = open(self.outputFileName,'a')    
        fopen.write(summary)
        fopen.close()
        if not processingPassTotals.has_key(dataTakingCond):
          continue
        if not processingPassTotals[dataTakingCond].has_key(proPassName):
          continue

    return S_OK()
      
