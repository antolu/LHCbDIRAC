#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-lhcb-production-check-files
# Author : Greig A Cowan
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.2 $"
import os, sys, popen2
import sys,string, pprint
import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "", "Status=", "Primary status" )
Script.registerSwitch( "", "MinorStatus=", "Secondary status" )
Script.registerSwitch( "", "ApplicationStatus=", "Application status" )
Script.registerSwitch( "", "Site=", "Execution site" )
Script.registerSwitch( "", "Owner=", "Owner (DIRAC nickname)" )
Script.registerSwitch( "", "ProductionID=", "Select jobs for specified job group" )
Script.registerSwitch( "", "Date=", "Date in YYYY-MM-DD format, if not specified default is today" )
Script.registerSwitch( "", "Verbose=", "For more detailed information about file and job states. Default False." )
#Script.initAsScript()
Script.addDefaultOptionValue( "LogLevel", "ALWAYS" )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Interfaces.API.Dirac                       import Dirac
from LHCbDIRAC.LHCbSystem.Client.DiracProduction      import DiracProduction
from LHCbDIRAC.LHCbSystem.Utilities.JobInfoFromXML    import JobInfoFromXML
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

args = Script.getPositionalArgs()

wmsStatus=None
minorStatus=None
appStatus=None
site=None
owner=None
prodID=None
date=None
verbose=False

def usage():
  print 'Usage: %s [Try -h,--help for more information]' %(Script.scriptName)
  DIRAC.exit(2)

def getWmsJobMetadata( prodID, wmsStatus, minorStatus, site, diracProd):
  '''Gets information about jobs from the WMS'''
  result_wms = diracProd.getProdJobMetadata( productionID=prodID,
                                             status=wmsStatus,
                                             minorStatus=minorStatus,
                                             site=site)

  if not result_wms['OK']:
    print 'ERROR %s' % result_wms['Message']
    DIRAC.exit( 2)
  else:
    # create list of jobIDs in this state belonging to this production
    jobIDs = result_wms['Value'].keys()

  return jobIDs

def getInputOutputLfns( jobIDs, dirac):
  '''Determine the input and output file LFNs for each job
  '''

  jobToInputOutputLfns = {}

  result_data = dirac.getJobInputData( jobIDs)
  if not result_data['OK']:
    print 'ERROR %s' % result_data['Message']
    DIRAC.exit( 2)
  else:
    idsToInputLfns = result_data['Value']

  for id in jobIDs:
    jobinfo = JobInfoFromXML( id)
    result = jobinfo.valid()
    if not result['OK']:
      print result['Message']
      continue

    inputLfnMap = {}
    for lfn in idsToInputLfns[ id]:
      inputLfnMap[ lfn[4:] ] = { 'ProdDbStatus' : 'Unknown',
                                 'UsedSE'       : 'Unknown',
                                 'BK': False, 'LFC' : False }


    jobToInputOutputLfns[ id ] = { 'Input' : inputLfnMap }

    outputLfnMap = {}
    result = jobinfo.getOutputLFN()
    if not result['OK']:
      print result['Message']
    else:
      lfns = result['Value']
      for lfn in lfns:
        outputLfnMap[ lfn ] = { 'BK' : False, 'LFC' : False}

    # Now add in the output data information
    jobToInputOutputLfns[ id ]['Output'] = outputLfnMap

  return jobToInputOutputLfns

def checkBkAndLfc( jobToInOutLfns, bk, dirac):
  '''Queries the LFC an BK to determine if files have been
  properly registered.
  '''
  errorList = {}

  for id, lfnMap in jobToInOutLfns.iteritems():
    for io in ['Input', 'Output']:
      # This gets the BK information
      lfns = lfnMap[ io ].keys()
      result_bk = bk.exists( lfns)
      if not result_bk['OK']:
        errorList[ id ] = result_bk['Message']
        exitCode = 2
      else:
        for lfn, status in result_bk['Value'].iteritems():
          if status:
            jobToInOutLfns[ id ][ io ][ lfn ]['BK'] = True

      #This gets the LFC information
      result = dirac.getMetadata( lfns, printOutput=False)
      if not result['OK']:
        errorList[ id ] = result_bk['Message']
        exitCode = 2
      else:
        for lfn, status in result['Value']['Successful'].iteritems():
          jobToInOutLfns[ id ][ io ][ lfn ]['LFC'] = True

  return (jobToInOutLfns, errorList)

def checkProdDbStatus( jobToInOutLfns, prodID, diracProd):
  '''Queries the production DB to check what the input files status has
  been marked as.
  '''

  for id, lfns in jobToInOutLfns.iteritems():
    result = diracProd.checkFilesStatus( lfns[ 'Input' ].keys(), prodID, printOutput=False)

    if not result['OK']:
      print 'ERROR %s' % result['Message']
      exitCode = 2
    else:
      for lfn, status in result['Value']['Successful'].iteritems():
        jobToInOutLfns[ id ][ 'Input' ][ lfn]['ProdDbStatus'] = status[ int(prodID)]['FileStatus']
        jobToInOutLfns[ id ][ 'Input' ][ lfn]['UsedSE'] = status[ int(prodID)]['UsedSE']

  return jobToInOutLfns

def compareStates( jobToInputOutputLfns, wmsStatus):
  '''This encodes the file checking logic. We need to cover the following cases:
  https://twiki.cern.ch/twiki/bin/view/LHCb/ProductionDB#dirac_lhcb_production_check_file
  '''

  summary = { 'Processed' : { 'Input' : {'OK' : [], 'NotOK' : [] },
                              'Output' : {'OK' : [], 'NotOK' : [] }
                              },
              'Assigned'  : { 'Input' : {'OK' : [], 'NotOK' : [] },
                              'Output' : {'OK' : [], 'NotOK' : [] }
                              },
              'Unused'    : { 'Input' : {'OK' : [], 'NotOK' : [] },
                              'Output' : {'OK' : [], 'NotOK' : [] }
                              },
              'Unknown'   : { 'Input' : {'OK' : [], 'NotOK' : [] },
                              'Output' : {'OK' : [], 'NotOK' : [] }
                              }
              }

  idToInputFileState = {}
  for id, io in jobToInputOutputLfns.iteritems():
    for lfn, status in io['Input'].iteritems():
      inputFileState = status['ProdDbStatus']

      if inputFileState in ['Processed', 'Assigned', 'Unused', 'Unknown']:
        if inputFileState != 'Unknown':
          idToInputFileState[ id ] = inputFileState

        if status['BK'] and status['LFC']:
          summary[ inputFileState ]['Input']['OK'].append( [id, lfn])
        else:
          summary[ inputFileState ]['Input']['NotOK'].append( [id, lfn])


    for lfn, status in io['Output'].iteritems():
      inputFileState = idToInputFileState[ id ]
      if status['BK'] and status['LFN']:
        summary[ inputFileState ]['Output']['OK'].append( [id, lfn])
      else:
        summary[ inputFileState ]['Output']['NotOK'].append( [id, lfn])

  return summary



def printSummary( summary, errorList):
  print '\nFile status summary for production %s. %s jobs are in the %s state.\n' % ( prodID, len(jobIDs), wmsStatus)

  for prodDbState, io in summary.iteritems():
    numInputsOK     = len( io['Input']['OK'])
    numInputsNotOK  = len( io['Input']['NotOK'])
    numOutputsOK    = len( io['Output']['OK'])
    numOutputsNotOK = len( io['Output']['NotOK'])

    print ' %s\t input files which are marked as %s \t with %s\t output files in the BK/LFC and %s\t output files not in BK/LFC' \
          % ( numInputsOK + numInputsNotOK, prodDbState, numOutputsOK, numOutputsNotOK)

  if errorList != {}:
    pprint.pprint( errorList)


def setProdDbStatus( summary):
  '''Set the appropriate state in the prodDB'''
  while True:
    choice = raw_input( 'Are you really sure you want to modify the status of the files in the production database? yes/no [no]: ')
    if choice.lower() in ( 'yes', 'y' ):
      choice2 = raw_input( 'Are you really really sure you want to modify the status of the files in the production database? yes/no [no]: ')
      if choice2.lower() in ( 'yes', 'y'):
        break
    else:
      print 'Modification aborted'
  return

if args:
  usage()

exitCode = 0

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower()=="status":
    wmsStatus=switch[1]
  elif switch[0].lower()=="minorstatus":
    minorStatus=switch[1]
  elif switch[0].lower()=="applicationstatus":
    appStatus=switch[1]
  elif switch[0].lower()=="site":
    site=switch[1]
  elif switch[0].lower()=="owner":
    owner=switch[1]
  elif switch[0].lower()=="productionid":
    prodID=switch[1]
  elif switch[0].lower()=="date":
    date=switch[1]
  elif switch[0].lower()=="verbose":
    verbose=switch[1]

selDate = date
if not date:
  selDate = 'Today'
conditions = {'Status':wmsStatus,'MinorStatus':minorStatus,'ApplicationStatus':appStatus,'Owner':owner,'ProductionID':prodID,'Date':selDate}

dirac = Dirac()
diracProd = DiracProduction()
bk = BookkeepingClient()

jobIDs = getWmsJobMetadata( prodID, wmsStatus, minorStatus, site, diracProd)

jobToInputOutputLfns = getInputOutputLfns( jobIDs, dirac)

jobToInputOutputLfns, errorList = checkBkAndLfc( jobToInputOutputLfns, bk, dirac)

jobToInputOutputLfns = checkProdDbStatus( jobToInputOutputLfns, prodID, diracProd)

summary = compareStates( jobToInputOutputLfns, wmsStatus)

if verbose:
  pprint.pprint( summary)

printSummary( summary, errorList)

DIRAC.exit( 0 )