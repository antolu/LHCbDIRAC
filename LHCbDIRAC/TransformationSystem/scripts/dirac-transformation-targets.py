#!/usr/bin/env python
###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
"""
Gets all Assigned files in a transformation and reports by target SE
"""

__RCSID__ = "$Id$"

import sys, os
from DIRAC.Core.Base import Script

def __getTransformations( args ):
  if not len( args ):
    print "Specify transformation number..."
    Script.showHelp()
  else:
    ids = args[0].split( "," )
    transList = []
    for transID in ids:
      r = transID.split( ':' )
      if len( r ) > 1:
        for i in xrange( int( r[0] ), int( r[1] ) + 1 ):
          transList.append( i )
      else:
        transList.append( int( r[0] ) )
  return transList
def __getTask( transID, taskID ):
  res = transClient.getTransformationTasks( {'TransformationID':transID, "TaskID":taskID} )
  if not res['OK'] or not res['Value']:
    return None
  return res['Value'][0]

#====================================
if __name__ == "__main__":

  Script.parseCommandLine( ignoreErrors = True )
  transList = __getTransformations( Script.getPositionalArgs() )

  from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
  from DIRAC import gLogger, exit
  transClient = TransformationClient()

  for transID in transList:
    res = transClient.getTransformationFiles( { 'TransformationID' : transID, 'Status':'Assigned'} )
    if not res['OK']:
      gLogger.fatal( "Error getting transformation files for %d" % transID )
      continue
    targetStats = {}
    taskDict = {}
    for fileDict in res['Value']:
      taskID = fileDict['TaskID']
      taskDict[taskID] = taskDict.setdefault( taskID, 0 ) + 1
    for taskID in taskDict:
      task = __getTask( transID, taskID )
      targetSE = task.get( 'TargetSE', None )
      targetStats[targetSE][0] = targetStats.setdefault( targetSE, [ 0, 0 ] )[0] + taskDict[taskID]
      targetStats[targetSE][1] += 1

    gLogger.always( "Transformation %d: %d assigned files found" % ( transID, len( res['Value'] ) ) )
    for targetSE, ( nfiles, ntasks ) in targetStats.iteritems():
      gLogger.always( "\t%s: %d files in %d tasks" % ( targetSE, nfiles, ntasks ) )
