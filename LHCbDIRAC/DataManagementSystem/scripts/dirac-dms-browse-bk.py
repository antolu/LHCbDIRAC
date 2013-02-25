#!/usr/bin/env python
########################################################################

"""
   Browse a BK path. It also allows to get from a file the datasets that should be kept (with a date)
   and in this case creates a list of datasets to be deleted and datasets to be kept
"""

__RCSID__ = "$Id: dirac-dms-browse-bk.py 42387 2011-09-07 13:53:37Z phicharp $"

import os, sys, datetime

import DIRAC
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
from DIRAC.Core.Utilities.List                                         import uniqueElements, sortList

def printTree( tree, tabs = 0, depth = sys.maxint ):
  if type( tree ) == type( {} ):
    keys = tree.keys()
  elif type( tree ) == type( [] ):
    keys = tree
  elif type( tree ) == type( '' ):
    keys = [tree]
  keys = sortList( uniqueElements( keys ) )
  prStr = tabs * '   '
  tabs += 1
  newTabs = tabs
  if type( tree ) == type( {} ) and tabs < depth:
    for key in keys:
      print prStr + str( key )
      newTabs = printTree( tree[key], tabs = tabs, depth = depth )
  else:
    print prStr + str( keys )
  return newTabs

def makeDataset( configuration, condition, processingPass, eventTypes, separator = '|' ):
  if type( eventTypes ) == type( 0 ):
    eventTypes = [eventTypes]
  eventTypes = ','.join( uniqueElements( [str( evtType ) for evtType in eventTypes] ) )
  return separator.join( [configuration, condition, processingPass, eventTypes] )

def makeBKPath( configuration, condition, processingPass, eventTypes ):
  return makeDataset( configuration, condition, processingPass, eventTypes, separator = '/' ).replace( '//', '/' )

def makeTreeFromDatasets( datasets ):
  tree = {}
  for dataset in datasets:
    configuration, condition, processingPass, eventTypes = dataset.split( '|' )
    tree.setdefault( configuration, {} ).setdefault( condition, {} ).setdefault( processingPass, [] ).extend( [int( eventType ) for eventType in eventTypes.split( ',' )] )
  return tree

def isDatasetInTree( dataset, tree ):
  configuration, condition, processingPass, eventTypes = dataset.split( '|' )
  eventTypes = [int( eventType ) for eventType in eventTypes.split( ',' )]
  for conf in tree:
    if conf != configuration: continue
    for cond in tree[conf]:
      if cond != condition: continue
      for pPass in tree[conf][cond]:
        if pPass != processingPass: continue
        for eventType in tree[conf][cond][pPass]:
          if eventType in eventTypes:
            return True
  return False

def datasetsFromTree( tree ):
  datasets = []
  for conf in tree:
    for cond in tree[conf]:
      for pPass in tree[conf][cond]:
        for eventType in tree[conf][cond][pPass]:
          datasets.append( makeDataset( conf, cond, pPass, eventType ) )
  return sortList( datasets )

def bkPathsFromTree( tree ):
  paths = []
  for conf in tree:
    for cond in tree[conf]:
      for pPass in tree[conf][cond]:
        paths.append( makeBKPath( conf, cond, pPass, tree[conf][cond][pPass] ) )
  return sortList( paths )

def commonDatasetsInTrees( tree, cmpTree ):
  # Get paths in tree that are in cmpTree
  #print pathsFromTree(tree)
  return sortList( [dataset for dataset in datasetsFromTree( tree ) if isDatasetInTree( dataset, cmpTree )] )

def absentDatasetsInTree( tree, cmpTree ):
  return sortList( [dataset for dataset in datasetsFromTree( tree ) if not isDatasetInTree( dataset, cmpTree )] )

if __name__ == "__main__":
  import time
  startTime = time.time()
  dmScript = DMScript()
  dmScript.registerBKSwitches()

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] [<LFN>] [<LFN>...]' % Script.scriptName, ] ) )

  Script.registerSwitch( "", "ToKeep=", "   File containing the twiki source of the retention table" )
  Script.registerSwitch( "", "PrintBKPath", "   Print the BKPaths of datasets that can be removed" )
  Script.registerSwitch( "", "Verbose", "   Print the current BK tree and the trees from the retention table" )

  Script.parseCommandLine( ignoreErrors = True )
  #print "Start time: %.3f seconds" % (time.time() - startTime)

  toKeepFile = None
  printBKPath = False
  verbose = False
  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    opt = switch[0]
    val = switch[1]
    if opt == 'ToKeep':
      toKeepFile = val
    elif opt == "PrintBKPath":
      printBKPath = True
    elif opt == "Verbose":
      verbose = True

  bkQuery = dmScript.getBKQuery()
  configuration = bkQuery.getConfiguration()
  if configuration[0] != '/':
    configuration = '/' + configuration
  conf = configuration[1:].split( '/' )
  confName = conf[0]
  confVersion = conf[1]

  toKeep = {}
  toKeepTree = {}
  toRemoveTree = {}
  if toKeepFile:
    try:
      f = open( toKeepFile, 'r' )
      content = f.readlines()
      f.close()
    except:
      print "Failed to open", toKeepFile
      DIRAC.exit( 2 )
    toKeepTree = { configuration : {} }
    toRemoveTree = { configuration : {} }
    for line in content:
      l = line.split( '|' )
      if len( l ) > 1:
        now = datetime.datetime.now()
        try:
          # Protection in case the date field is not conformin to a convention
          limitDate = l[6].split( '/' )
          limitDate = datetime.datetime( int( limitDate[0] ), int( limitDate[1] ), int( limitDate[2] ) )
        except:
          limitDate = now
        if limitDate < now:
          tree = toRemoveTree
        else:
          tree = toKeepTree
        conditions = []
        for cond in [cond.replace( '2.5', '2,5' ) for cond in l[1].replace( '2,5', '2.5' ).replace( '!', '' ).strip().split( ',' )]:
          if 'Mag*' in cond:
            conditions.extend( [cond.replace( '*', 'Down' ), cond.replace( '*', 'Up' )] )
          else:
            conditions.append( cond )
        processingPass = l[2].replace( confVersion + '-', '' ).strip()
        #print processingPass
        if processingPass.find( '/' ) == -1:
          processingPass = '/'.join( processingPass.replace( ' ', '' ).split( '+' ) )
          #print processingPass[0:3]
          if processingPass[0:3] == 'Sim':
            processingPass = os.path.join( '/', processingPass[0:5], processingPass[5:] )
            #print processingPass
          if processingPass[0:3] == 'Reco':
            processingPass = os.path.join( '/', processingPass[0:6], processingPass[6:] )
        if processingPass[0] != '/':
          processingPass = '/' + processingPass
        eventTypes = [int( evt ) for evt in l[3].strip().split( ',' )]
        for condition in conditions:
          tree[configuration].setdefault( condition.strip(), {} ).setdefault( processingPass, [] ).extend( eventTypes )
  if toKeepTree:
    print "Datasets to ke kept:"
    depth = printTree( toKeepTree )
  else:
    depth = sys.maxint
  if verbose and toRemoveTree:
    print "Datasets that can be removed:"
    printTree( toRemoveTree )

  print "Browsing BK..."
  bkTree = bkQuery.browseBK()
  if ( not toKeepFile and not printBKPath ) or verbose:
    printTree( bkTree, depth = depth )

  if toKeepFile:
    # Get paths in toKeepTree that are not in bkTree
    datasets = absentDatasetsInTree( toKeepTree, bkTree )
    #print datasets
    print "The following datasets should have been kept but are not present:"
    printTree( makeTreeFromDatasets( datasets ) )

    # Get datasets in toRemoveTree that are in bkTree and should be removed
    datasets = absentDatasetsInTree( bkTree, toKeepTree )
    #print datasets
    print "The following datasets can be removed:"
    removeTree = makeTreeFromDatasets( sortList( datasets ) )
    printTree( removeTree )
    if printBKPath:
      print "BKPaths to be removed:"
      bkPaths = bkPathsFromTree( removeTree )
      for path in bkPaths:
        print path
  elif printBKPath:
      print "BKPaths present in %s:" % bkQuery.getPath()
      bkPaths = bkPathsFromTree( bkTree )
      for path in bkPaths:
        print path


