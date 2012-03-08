#!/usr/bin/env python
########################################################################
# $HeadURL:  $
########################################################################

"""
   Get statistics on productions related to a given processing pass
"""

__RCSID__ = "$Id:  $"

import os, sys, pickle, datetime, time

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
from DIRAC import gLogger
from LHCbDIRAC.Core.Utilities.HTML import *
from LHCbDIRAC.DataManagementSystem.Client.DMScript import BKQuery
import DIRAC

class HTMLProgressTable:
  def __init__( self, processingPass):
    self.table = Table()
    self.__titleRow( '')
    self.table.rows.append( [TableCell( processingPass, attribs={"colspan":self.HTMLColumns}, header=True)] )
    self.table.rows.append( [TableCell( time.ctime(time.time()),   attribs={"colspan":self.HTMLColumns}, align='center')])

  def getTable( self ):
    return self.table

  def __titleRow( self, title):
    titles = StatInfo().getTitles()
    row1 =[TableCell('<b>'+str(title)+'</b>', align='left', attribs={'rowspan':2})]
    row2 = []
    for (head, subs) in titles:
      if type(subs) == type([]):
        cols = len(subs)
      else:
        cols = 1
      row1.append( TableCell( head, attribs={'colspan':cols}, align='center', header=True))
      row2 += subs
    self.HTMLColumns = len(row2) + 1
    return [TableRow(row1, header=True), TableRow(row2, header=True)]

  def __tableRow( self, info):
    row = [ info.getName() ]
    infoStrings = info.getItemsAsString()
    for s in infoStrings:
      row.append( TableCell( s, align='right', char='.'))
    return row

  def writeHTML( self, conditions, prodStats ):
    titleRow = self.__titleRow( conditions)
    self.table.rows.append([TableCell( "", attribs={"colspan":self.HTMLColumns})])
    self.table.rows += titleRow
    if not prodStats:
      self.table.rows.append(["None"])
    for prodStat in prodStats:
      self.table.rows.append(self.__tableRow( prodStat ))

  def writeHTMLSummary( self, summaryProdStats):
    titleRow = self.__titleRow( 'Total' )
    self.table.rows.append([TableCell( "", attribs={"colspan":self.HTMLColumns})])
    self.table.rows += titleRow
    prodStats = self.__sumProdStats( summaryProdStats)
    if prodStats:
      rawInfo = prodStats[0]
      recoInfo = prodStats[1]
      recoInfo.setRawInfo( rawInfo )
      mergingInfo = prodStats[-1]
      mergingInfo.setRawInfo( rawInfo)
      row = self.__tableRow( rawInfo )
      self.table.rows.append(row)
      row = self.__tableRow( recoInfo )
      self.table.rows.append(row)
      row = self.__tableRow( mergingInfo )
      self.table.rows.append(row)
    else:
      self.table.rows.append(["None"])

  def __sumProdStats( self, summaryProdStats):
    sumStats = []
    for ind in range(4):
      info = None
      for prodStats in summaryProdStats:
        if not prodStats:
          continue
        if info:
          info += prodStats[ind]
        else:
          info = prodStats[ind]
      sumStats.append(info)
    return sumStats

  def writeHTMLDifference( self, summaryProdStats, previousProdStatsDict):
    previousTime = previousProdStatsDict['Time']
    previousProdStats = previousProdStatsDict['ProdStats']
    titleRow = self.__titleRow( 'Progress' )
    self.table.rows.append([TableCell( "", attribs={"colspan":self.HTMLColumns})])
    self.table.rows.append([TableCell( 'Progress since %s' % previousTime, align='center', attribs={"colspan":self.HTMLColumns})])
    self.table.rows += titleRow
    prodStats = self.__sumProdStats( summaryProdStats )
    prevProdStats = self.__sumProdStats( previousProdStats )
    diffStats = 4*[None]
    for ind in range(4):
      diffStats[ind] = prodStats[ind] - prevProdStats[ind]
    row = self.__tableRow( diffStats[0] )
    self.table.rows.append(row)
    row = self.__tableRow( diffStats[1] )
    self.table.rows.append(row)
    row = self.__tableRow( diffStats[-1] )
    self.table.rows.append(row)

class StatInfo:
  def __init__(self, name='', info={}):
    self.name = name
    self.items = ['BadFiles', 'BadRuns', 'BadLumi', 'OKFiles', 'OKRuns', 'OKLumi', 'Events', 'Files', 'Runs', 'Lumi', 'ratio,prev,Lumi', 'ratio,prev,OKLumi', 'ratio,raw,Lumi']
    self.regularItems = [item for item in self.items if item.split(',')[0] != 'ratio']
    self.ratioItems =  [item for item in self.items if item.split(',')[0] == 'ratio']
    self.titles = [ ['DQ Bad', ['Files', 'Runs', 'Lumi (pb-1)']],
                    ['DQ OK', ['Files', 'Runs', 'Lumi (pb-1)']],
                    ['All not Bad', ['Events', 'Files', 'Runs', 'Lumi (pb-1)', '% of total', '% of OK', '% of RAW']] ]
    self.other = { 'prev':{}, 'raw':{} }
    self.contents = dict.fromkeys(self.regularItems, 0)
    self.contents.update( dict.fromkeys(self.ratioItems, None))
    # info is a dictionary of dictionaries info[item][dqFlqg] while contents is a flat dictionary (label = flag+item)
    for item in info:
      for flag in info[item]:
        label = flag + item
        if label in self.items:
          self.contents[label] = info[item][flag]

  def setValues( self, values ):
    for item in values:
      if item in self.items:
        self.contents[item] = values[item]

  def getValues( self ):
    return self.contents

  def __setRatios( self ):
    for item in self.ratioItems:
      x = item.split(',')
      if len(x) == 3:
        info = self.other.get(x[1],{})
        if info:
          den = info.getItem(x[2])
        else:
          den = 0
        if den:
          # avoid 100.0% if not really 100%...
          self.contents[item] = int((1000.*self.getItem('Lumi',0))/den) / 10.
        else:
          self.contents[item] = None

  def setPrevInfo( self, info):
    if not self.other['raw']:
      self.other['raw'] = info
    self.other['prev'] = info
    self.__setRatios()
  def setRawInfo( self, info):
    if not self.other['prev']:
      self.other['prev'] = info
    self.other['raw'] = info
    self.__setRatios()
  def getPrevInfo( self ):
    return self.other['prev']
  def getRawInfo( self ):
    return self.other['raw']
  def getName(self):
    return self.name
  def getItem( self, item, val=None):
    return self.contents.get(item, val)
  def getItemAsString( self, item):
    val = self.getItem(item)
    if not val or abs(val) < 0.001:
      return '-'
    if item not in self.regularItems:
      return '%.1f'% val
    if type(val) == type(0):
      return '%d' % val
    if type(val) == type(0.):
      return '%.3f'%val
    else:
      return str(val)

  def getTitles( self ):
    return self.titles
  def getItemNames( self ):
    return self.items
  def getItems( self ):
    values = []
    for item in self.items:
      values.append( self.getItem(item))
    return values
  def getItemsAsString( self ):
    values = []
    for item in self.items:
      values.append( self.getItemAsString(item))
    return values

  def __add__( self, other ):
    thisName = self.getName()
    thatName = other.getName()
    if thisName != thatName:
      if not thisName:
        thisName = thatName
      else:
        thisName += '-'+thatName
    values = {}
    for item in self.regularItems:
      values[item] = self.contents[item] + other.contents[item]
    info = StatInfo( thisName )
    info.setValues( values )
    rawInfo1 = self.getRawInfo()
    rawInfo2 = other.getRawInfo()
    if rawInfo1 and rawInfo2:
      info.setRawInfo( rawInfo1 + rawInfo2 )
      info.setPrevInfo( self.getPrevInfo() + other.getPrevInfo())
    self.__setRatios()
    return info

  def __sub__( self, other ):
    thisName = self.getName()
    thatName = other.getName()
    if thisName != thatName:
      print "Error substracting StatInfo for %s and %s" %(thisName, thatName)
      return StatInfo( '' )
    values = {}
    for item in self.items:
      if self.contents[item] != None and other.contents[item] != None:
        values[item] = self.contents[item] - other.contents[item]
      else:
        values[item] = None
    info = StatInfo( thisName)
    info.setValues(values )
    return info

class ProcessingProgress:

  def __init__(self, cacheFile=None):
    if not cacheFile:
      self.prodStatFile = os.path.join(os.environ['HOME'],".dirac/work","dirac-production-stats.pkl")
    else:
      self.prodStatFile = cacheFile
    self.cacheVersion = '0.0'
    self.clearCache = []

    # Recuperate the previous cached information
    self.readCache()

    self.bk = BookkeepingClient()
    self.transClient = TransformationClient()

  def setClearCache( self, clearCache ):
    self.clearCache = clearCache

  def __getProdBkDict( self, prod):
    res = self.transClient.getBookkeepingQueryForTransformation( prod )
    if not res['OK']:
      gLogger.error( "Couldn't get BK query on production %d" % prod)
      return {}
    prodBKDict = res['Value']
    return prodBKDict

  def getFullStats( self, bkQuery, printResult = False):
    processingPass = bkQuery.getProcessingPass()
    if printResult:
      gLogger.info( "\nStatistics for processing %s, condition %s\n"%(processingPass,bkQuery.getConditions()) )
    prodStats = []
    processingPass = processingPass.split('/')
    if len(processingPass) != 4 or processingPass[1] != "Real Data":
      gLogger.error( "Processing pass should be /Real Data/<Reco>/<Stripping>" )
      return []

    #Get production numbers for the Reco
    recoBKQuery = BKQuery( bkQuery )
    recoBKQuery.setProcessingPass( '/'.join(processingPass[0:3]) )
    recoList = recoBKQuery.getBKProductions()
    recoRunRanges = {}
    recoDQFlags = []
    for prod in recoList:
      prodBKDict = self.__getProdBkDict( prod)
      if prodBKDict:
        recoRunRanges[prod] = [prodBKDict.get("StartRun",0), prodBKDict.get("EndRun",sys.maxint)]
        dqFlag = prodBKDict.get("DataQualityFlag", ['UNCHECKED','EXPRESS_OK','OK'])
        if type(dqFlag) == type(''):
          dqFlags = dqFlag.split(',')
        recoDQFlags += [fl for fl in dqFlags if fl not in recoDQFlags]
      else:
        recoRunRanges[prod] = [0,0]
    # Sort productions by runs
    try:
      recoList.sort(cmp=(lambda p1,p2: int(recoRunRanges[p1][0] - recoRunRanges[p2][1])))
    except:
      print "Exception in sorting productions:"
      for p in recoList:
        print p,recoRunRanges[p]
    gLogger.verbose( "Reconstruction productions found: %s" % str(recoList))
    gLogger.verbose( "Reconstruction DQ flags: %s" % str(recoDQFlags))

    # Get productions for merging
    mergeList = []
    mergeStripProds = {}
    # Get stripping productions as parents of merging productions
    stripList = []
    for prod in bkQuery.getBKProductions():
      prodBKDict = self.__getProdBkDict( prod )
      gLogger.verbose( "BK query for production %s: %s" %(prod,str(prodBKDict)))
      if prodBKDict.get('FileType') in bkQuery.getFileTypeList() and 'ProductionID' in prodBKDict:
        mergeList.append(prod)
        prods = prodBKDict['ProductionID']
        if type(prods) != type([]):
          prods = [prods]
        stripList += prods
        mergeStripProds[prod] = [str(p) for p in prods]
      else:
        gLogger.verbose( "Could not find production or filetype %s in BKquery of production %d (%s)" %(str(bkQuery.getFileTypeList()), prod, str(prodBKDict)))
    mergeList.sort( cmp=(lambda p1,p2: int(mergeStripProds[p1][0]) - int(mergeStripProds[p2][0])))
    gLogger.verbose( "Merging productions found: %s" % str(mergeList))

    # get list of stripping productions (from merging)
    stripRunRanges = {}
    for prod in stripList:
      prodBKDict = self.__getProdBkDict( prod )
      if prodBKDict:
        stripRunRanges[prod] = [prodBKDict.get("StartRun",0), prodBKDict.get("EndRun",sys.maxint)]
      else:
        stripRunRanges[prod] = [0,0]
    # Sort productions by runs
    try:
      stripList.sort(cmp=(lambda p1,p2: int(stripRunRanges[p1][0] - stripRunRanges[p2][1])))
    except:
      print "Error when sorting stripping productions:"
      for p in stripList:
        print p, stripRunRanges[p]
    gLogger.verbose( "Stripping productions found: %s" % str(stripList))

    # Get all runs corresponding to the run range used by the Reco productions
    rawBKQuery = BKQuery( bkQuery )
    rawBKQuery.setProcessingPass( '/Real Data' )
    rawBKQuery.setFileType( "RAW" )
    # get the list of runs (-prodNum)
    fullRunList = rawBKQuery.getBKRuns()
    gLogger.verbose( "Initial list of runs: %s" %str(fullRunList))
    recoRunList = []
    open = False
    for prod in [p for p in recoList]:
      # Forget fully open productions
      # Don't consider productions without a BK query (these are individual files)
      if recoRunRanges[prod][1] == sys.maxint and recoRunRanges[prod][0] != -sys.maxint:
        open = True
        # Try and find if that open production overlaps wiht a closed one, in which case, remove it
        for p in [p for p in recoList if p != prod]:
          if recoRunRanges[prod][0] < recoRunRanges[p][1]:
            open = False
            recoList.remove(prod)
            break
        if not open: continue
      recoRunList += [run for run in fullRunList if run not in recoRunList and run >= recoRunRanges[prod][0] and run <= recoRunRanges[prod][1]]
    gLogger.verbose( "List of runs matching Reco: %s" %str(recoRunList))

    if False and not open and stripList:
      runList = []
      for prod in stripList:
        runList += [run for run in recoRunList if run not in runList and run >= stripRunRanges[prod][0] and run <= stripRunRanges[prod][1]]
    else:
      runList = recoRunList
    gLogger.verbose( "Final list of runs matching Reco and Stripping: %s" %str(runList))

    # Now get statistics from the runs
    info, runInfo = self._getStatsFromRuns( int(bkQuery.getEventTypeList()[0]), runList, recoDQFlags )
    rawInfo = StatInfo( processingPass[1], info )
    prodStats.append( rawInfo )
    if printResult:
       gLogger.info( "%s - All runs in Reco productions" %processingPass[1])
       for f in runInfo:
         if runInfo[f]:
           gLogger.info( "%s runs (%d): %s" %(f,len(runInfo[f]),str(runInfo[f])))
       summStr = "%s files, "  %rawInfo.getItemAsString('Files')
       summStr += "%s events in "%rawInfo.getItemAsString('Events')
       summStr +=  "%s runs, luminosity (pb-1):All=%s, Bad=%s, OK=%s" %(rawInfo.getItemAsString('Runs'),
                                                                        rawInfo.getItemAsString('Lumi'),rawInfo.getItemAsString('BadLumi'),rawInfo.getItemAsString('OKLumi'))
       gLogger.info( summStr )

    # Create the info for the 3 sets of productions
    prodSets = []
    fileType = bkQuery.getFileTypeList()[0]
    prodSets.append( {'Name': processingPass[2], 'FileType': "SDST", 'List':recoList, 'RunRange':recoRunRanges, 'MotherProds':None, 'AllReplicas':False } )
    prodSets.append( {'Name': processingPass[3], 'FileType': fileType, 'List':stripList, 'RunRange':stripRunRanges, 'MotherProds':None, 'AllReplicas':True } )
    prodSets.append( {'Name': "Merging (%s)"%fileType.split('.')[0], 'FileType': fileType, 'List':mergeList, 'RunRange':None, 'MotherProds':mergeStripProds, 'AllReplicas':False } )

    prevInfo = rawInfo
    for prodSet in prodSets:
      info = StatInfo( prodSet['Name'], self._getProdInfo( prodSet, runList, printResult=printResult ))
      info.setRawInfo( rawInfo)
      info.setPrevInfo( prevInfo)
      prevInfo = info
      prodStats.append(info)

    self.saveCache()
    return prodStats

  def __sumProdInfo( self, info, totInfo):
    for c in info:
      for f in info[c]:
        totInfo[c][f] = totInfo.setdefault(c,{}).setdefault(f,0) + info[c][f]
    return totInfo

  def _getProdInfo( self, prodSet, runList, printResult=False ):
    totInfo = {}
    if printResult:
      gLogger.info( "")
    for prod in prodSet['List']:
      info, runInfo = self._getStatsFromBK( prod, prodSet['FileType'], runList, prodSet['AllReplicas'] )
      if info['Files'][''] == 0:
        continue
      runRange = prodSet['RunRange']
      if runRange and prod in runRange and runRange[prod][0] == 0 and runRange[prod][0] == 0:
        for f in info['Runs']:
          info['Runs'][f] = 0
      totInfo = self.__sumProdInfo( info, totInfo)
      if printResult:
        summStr = "%s production %d -" %(prodSet['Name'],prod)
        if runRange and prod in runRange:
          firstRun = runRange[prod][0]
          lastRun = runRange[prod][1]
          if firstRun:
            summStr += " From run %d" % int(firstRun)
          if lastRun and lastRun != sys.maxint:
            summStr += " Up to run %d" % int(lastRun)
          if firstRun == 0 and lastRun == 0:
            summStr += "No run range specified"
        motherProds = prodSet['MotherProds']
        if motherProds and prod in motherProds:
          summStr += " from productions %s" % motherProds[prod]
        gLogger.info( summStr )
        for f in runInfo:
          if runInfo[f]:
            gLogger.info( "%s runs (%d): %s" %( f, len(runInfo[f]), str(runInfo[f])))
        summStr = "%d files, "  %info['Files']['']
        if info['Events']:
          summStr += "%d events in "%info['Events']['']
        summStr +=  "%d runs, luminosity (pb-1): All=%.3f, Bad=%.3f, OK=%.3f" %(info['Runs'][''],info['Lumi'][''],info['Lumi']['Bad'],info['Lumi']['OK'])
        gLogger.info( summStr )
    return totInfo

  def outputResults( self, conditions, processingPass, prodStats):
    outputString = ""
    outputString +=  "\nProduction progress for %s %s %s %s %s\n" %(conditions,",",processingPass,"on",time.ctime(time.time()))
    if len(prodStats) < 4:
      outputString += "No statistics found for this BK query"
      return outputString
    for i in range(4):
      info = prodStats[i]
      if not info:
        continue
      name = info.getName()
      outputString += "\nSummary for %s\n" % name
      outputString += "%d files, " %info.getItem('Files')
      if info.getItem('Events'):
        outputString +=  "%d events in " %info.getItem('Events')
      outputString +=  "%d runs, luminosity (pb-1): All=%.3f, Bad=%.3f, OK=%.3f\n" %(info.getItem('Runs'),info.getItem('Lumi'),info.getItem('BadLumi'),info.getItem('OKLumi'))
      prevStats = prodStats[:i]
      prevStats.reverse()
      for prevInfo in prevStats:
        name = prevInfo.getName()
        if prevInfo.getItem('Runs') == 0:
          outputString += "From %s : - No runs...\n" % name
        else:
          outputString +=  "From %s : %.1f%% files, "  %( name, 100.*info.getItem('Files')/prevInfo.getItem('Files'))
          if info.getItem('Events') and prevInfo.getItem('Events'):
            outputString +=  "%.1f%% events\n" %(100.*info.getItem('Events')/prevInfo.getItem('Events'))
          outputString += "%.1f%% runs, %.1f%% luminosity\n" \
                          %( 100.*info.getItem('Runs')/prevInfo.getItem('Runs'), 100.*info.getItem('Lumi')/prevInfo.getItem('Lumi'))
    return outputString

  def __getRunsDQFlag( self, runList, evtType):
    res = self.bk.getDataQualityForRuns( runList )
    runFlags = {}
    if res['OK']:
      dqFlags = res['Value']
      for dq in dqFlags:
        if dq[2] == evtType:
          runFlags.setdefault(dq[0],[]).append(dq[1])
    runDQFlags = {}
    flags = ('BAD', 'OK', 'EXPRESS_OK', 'UNCHECKED')
    for run in runFlags:
      for fl in flags:
        if fl in runFlags[run]:
          runDQFlags[run] = fl
          break
    return runDQFlags

  def _getStatsFromRuns( self, evtType, runList, recoDQFlags):
    info = dict.fromkeys( ('Events','Runs','Files','Lumi'), {})
    for c in info:
      info[c] = dict.fromkeys( ('Bad','OK',''), 0)
    now = datetime.datetime.utcnow()
    # Set to True to renew the cache
    clearCache = 'RAW' in self.clearCache
    newRuns = [ run for run in runList if clearCache
                or run not in self.cachedInfo
                or 'DQFlag' not in self.cachedInfo[run]
                or (now - self.cachedInfo[run]['Time']) < datetime.timedelta( days = 2 )   ]
    if newRuns:
      runFlags = self.__getRunsDQFlag( newRuns, evtType )
    else:
      runFlags = {}
    runsByDQFlag = {}
    runInfo = {}
    for run in runList:
      cached = self.cachedInfo.get(run,{})
      cachedTime = cached.get('Time',None)
      if run not in newRuns:
        cachedFiles = cached.get('Files',0)
        cachedEvents = cached.get('EventStat',0)
        cachedLumi = cached.get('Luminosity',0)
        dqFlag = cached.get('DQFlag',None)
      else:
        res = self.bk.getRunInformations( run )
        if res['OK']:
          val = res['Value']
          ind = val['Stream'].index(90000000)
          cachedFiles = val['Number of file'][ind]
          cachedEvents = val['Number of events'][ind]
          cachedLumi = val['luminosity'][ind]
          cachedTime = val['RunStart']
        else:
          gLogger.error( "Unable to get run information for run %s" % str(run))
          continue
        dqFlag = runFlags[run]
      self.cachedInfo[run] = { 'Time':cachedTime, 'Files':cachedFiles, 'EventStat': cachedEvents, 'Luminosity': cachedLumi, 'DQFlag':dqFlag }
      runsByDQFlag[dqFlag] = runsByDQFlag.setdefault(dqFlag,0) + 1
      if dqFlag == "BAD":
        runInfo.setdefault('BAD',[]).append( run )
      elif dqFlag not in recoDQFlags and dqFlag != 'OK' :
        runInfo.setdefault('Untagged',[]).append( run )
      # Now count...
      flags = []
      if dqFlag != 'BAD':
        flags.append('')
        # OK in recoDQFlags means we take everything that is not BAD (reprocessing or new convention)
        if dqFlag in recoDQFlags or dqFlag == 'OK':
          flags.append('OK')
      else:
        flags.append('Bad')
      for f in flags:
        info['Runs'][f] += 1
        info['Files'][f] += cachedFiles
        info['Events'][f] += cachedEvents
        info['Lumi'][f] += cachedLumi

    # Set lumi in pb-1
    for f in info['Lumi']:
      info['Lumi'][f] /= 1000000.
    gLogger.info( "Runs per flag:" )
    for key in runsByDQFlag:
      gLogger.info( "%s : %d" %(key, runsByDQFlag[key]))
    for f in runInfo:
      runInfo[f].sort()
    return info, runInfo

  def __getLfnsMetadata( self, lfns ):
    from DIRAC.Core.Utilities.List                                         import breakListIntoChunks
    lfnDict = {}
    if len(lfns):
      gLogger.verbose("Getting metadata for %d files" % len(lfns))
      for lfnChunk in breakListIntoChunks( lfns, 5000):
        while True:
          res = self.bk.getFileMetadata(lfnChunk)
          if not res['OK']:
            gLogger.error( "Error getting files metadata, retrying...", res['Message'])
          else:
            break
        metadata = res['Value']
        for lfn in lfnChunk:
          lfnDict[lfn] = {}
          for m in ('EventStat', 'Luminosity', 'DQFlag', 'RunNumber'):
            lfnDict[lfn][m] = metadata[lfn][m]
    return lfnDict

  def _getStatsFromBK( self, prod, fileType, runList, allReplicas ):
    bkQueryDict = { "ProductionID": prod, "FileType": fileType }
    bkStr = str(bkQueryDict)
    bkQuery = BKQuery( bkQueryDict, visible = False )
    if allReplicas:
      bkQuery.setOption('ReplicaFlag', "All")
    cached = self.cachedInfo.get(bkStr, {})
    cachedTime = cached.get('Time', None)
    cachedLfns = cached.get('Lfns', {})
    if fileType in self.clearCache:
      cachedTime = datetime.datetime.utcnow() - datetime.timedelta( days = 8 )
      cachedTime = None
      cachedLfns = {}
      gLogger.verbose( "Cleared cache for production %s, file type %s" %( str(prod), fileType))
    # Update if needed the cached information on LFNs
    if cachedLfns:
      lfns = [lfn for lfn in cachedLfns if cachedLfns[lfn].get('DQFlag') not in ('OK', 'BAD')]
      if len(lfns):
        #  get the DQFlag of files that are not yet OK
        while True:
          res = self.bk.getFileMetadata(lfns)
          if not res['OK']:
            gLogger.error( "Error getting files metadata for cached files, bkQuery %s: %s" %(bkStr,res['Message']))
          else:
            metadata = res['Value']
            for lfn in lfns:
              cachedLfns[lfn]['DQFlag'] = metadata[lfn]['DQFlag']
            break

    # Now get the new files since last time...
    if cachedTime:
      bkQuery.setOption('StartDate', cachedTime.strftime( '%Y-%m-%d %H:%M:%S' ))
    gLogger.verbose("Getting files for BKQuery %s" % str(bkQuery))
    cachedTime = datetime.datetime.utcnow()
    lfns = [lfn for lfn in bkQuery.getLFNs(printOutput = False) if lfn not in cachedLfns]
    gLogger.verbose("Returned %d files" % len(lfns))
    cachedLfns.update( self.__getLfnsMetadata(lfns) )

    self.cachedInfo[bkStr] = { 'Time':cachedTime, 'Lfns':cachedLfns }

    # Now sum up all information for the files
    info = dict.fromkeys( ('Events','Runs','Files','Lumi'), {})
    for c in info:
      if c == 'Runs':
        for f in ('Bad', 'OK', ''):
          info[c][f] = []
      else:
        info[c] = dict.fromkeys( ('Bad', 'OK', ''), 0)

    for lfn in cachedLfns:
      lfnInfo = cachedLfns[lfn]
      run = lfnInfo['RunNumber']
      if run in runList and run in self.cachedInfo and self.cachedInfo[run]['DQFlag']!='BAD':
        dqFlag = cachedLfns[lfn]['DQFlag']
        flags = []
        if dqFlag != 'BAD':
          flags.append('')
        else:
          flags.append('Bad')
        if dqFlag == 'OK':
          flags.append('OK')
        for f in flags:
          if run not in info['Runs'][f]:
            info['Runs'][f].append(run)
          info['Files'][f] += 1
          info['Events'][f] += lfnInfo['EventStat']
          info['Lumi'][f] += lfnInfo['Luminosity']

    runInfo = {}
    if 'BAD' in info['Runs']:
      runInfo['BAD'] = info['Runs']['BAD']
      runInfo['BAD'].sort()
    else:
      runInfo['BAD'] = []
    if '' in info['Runs'] and 'OK' in info['Runs']:
      runInfo['Untagged'] = [run for run in info['Runs'][''] if run not in info['Runs']['OK']]
      runInfo['Untagged'].sort()
    else:
      runInfo['Untagged'] = []
    for f in info['Runs']:
      info['Runs'][f] = len(info['Runs'][f])
    for f in info['Lumi']:
      info['Lumi'][f] /= 1000000.
    return info, runInfo

  def getPreviousStats(self, processingPass):
    prevStats = self.cachedInfo.get('ProdStats',{}).get(processingPass)
    if prevStats:
      try:
        name = prevStats['ProdStats'][0][0].getName()
      except:
        prevStats = None
    return prevStats

  def setPreviousStats( self, processingPass, prevStats):
    self.cachedInfo.setdefault('ProdStats',{})[processingPass] = prevStats
    self.saveCache()

  def readCache( self ):
    if not os.path.exists( self.prodStatFile):
      gLogger.info( "Created cached file %s" % self.prodStatFile)
      self.cachedInfo = {}
      self.saveCache()
      return
    fileRead = False
    while not fileRead:
      try:
        with FileLock(self.prodStatFile):
          f = open( self.prodStatFile, 'r')
          cachedVersion = pickle.load(f)
          startTime = time.time()
          if cachedVersion == self.cacheVersion:
            self.cachedInfo = pickle.load(f)
            gLogger.info("Loaded cached information from %s in %.3f seconds" % (self.prodStatFile, time.time()-startTime))
          else:
            gLogger.info("Incompatible versions of cache, reset information (%s, expect %s)" %(cachedVersion,self.cacheVersion))
            self.cachedInfo = {}
          f.close()
          fileRead = True
      except FileLockException, error:
        gLogger.error( "Lock exception: %s while reading pickle file %s" %(error,self.prodStatFile))
      except:
        gLogger.error("Could not read cache file %s" % self.prodStatFile)
        self.cachedInfo = {}
        DIRAC.exit(1)

  def saveCache(self):
    fileSaved = False
    while not fileSaved:
      f = None
      startTime = time.time()
      try:
        with FileLock(self.prodStatFile) as lock:
          f = open(self.prodStatFile,'w')
          pickle.dump(self.cacheVersion, f)
          pickle.dump(self.cachedInfo, f)
          f.close()
          gLogger.verbose( "Successfully wrote pickle file %s in %.3f seconds" %(self.prodStatFile, time.time()-startTime))
          fileSaved = True
      except KeyboardInterrupt:
        gLogger.info( "<CTRL-C> hit while saving cache file, retry..." )
        lock.release()
        if f:
          f.close()
      except FileLockException, error:
        gLogger.error( "Lock exception: %s while writing pickle file %s" %(error,self.prodStatFile))
      except:
        gLogger.error( "Unable to write pickle file %s" %self.prodStatFile)


import errno

class FileLockException(Exception):
    pass

class FileLock(object):
    """ A file locking mechanism that has context-manager support so
        you can use it in a with statement. This should be relatively cross
        compatible as it doesn't rely on msvcrt or fcntl for the locking.
    """

    def __init__(self, file_name, timeout=10, delay=.05):
        """ Prepare the file locker. Specify the file to lock and optionally
            the maximum timeout and the delay between each attempt to lock.
        """
        self.is_locked = False
        self.lockfile = os.path.join(os.getcwd(), "%s.lock" % file_name)
        self.file_name = file_name
        self.timeout = timeout
        self.delay = delay


    def acquire(self):
        """ Acquire the lock, if possible. If the lock is in use, it check again
            every `wait` seconds. It does this until it either gets the lock or
            exceeds `timeout` number of seconds, in which case it throws
            an exception.
        """
        start_time = time.time()
        while True:
            try:
                self.fd = os.open(self.lockfile, os.O_CREAT|os.O_EXCL|os.O_RDWR)
                break;
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
                if (time.time() - start_time) >= self.timeout:
                    raise FileLockException("Timeout occured")
                time.sleep(self.delay)
        self.is_locked = True


    def release(self):
        """ Get rid of the lock by deleting the lockfile.
            When working in a `with` statement, this gets automatically
            called at the end.
        """
        if self.is_locked:
            os.close(self.fd)
            os.unlink(self.lockfile)
            self.is_locked = False


    def __enter__(self):
        """ Activated when used in the with statement.
            Should automatically acquire a lock to be used in the with block.
        """
        if not self.is_locked:
            self.acquire()
        return self


    def __exit__(self, type, value, traceback):
        """ Activated at the end of the with statement.
            It automatically releases the lock if it isn't locked.
        """
        if self.is_locked:
            self.release()


    def __del__(self):
        """ Make sure that the FileLock instance doesn't leave a lockfile
            lying around.
        """
        self.release()
