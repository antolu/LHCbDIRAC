#!/usr/bin/env python
""" dirac-production-shifter
 
  Script for the production shifter. 

  Print a summary of the requests in the system ( and their productions ).
  The following switches and options are provided.
  
  - requestState  : request state(s) 
     [ Accepted, Active, BK Check, BK OK, Cancelled, Done, New, PPG OK, Rejected,
       Submitted, Tech OK ]
  - requestType   : request type(s)  
     [ Reconstruction, Simulation, Stripping, Stripping (Moore), Swimming, WGProduction ]
  - requestID     : request ID(s)
  - simCondition  : simulation condition(s) ( e.g. Beam4000GeV-VeloClosed-MagDown )
  - proPath       : proPath(s) ( e.g. Reco13 )
  - eventType     : eventType(s) ( e.g. 90000000 ) 
  - sort          : sort requests using the keywords [RequestID,RequestState,RequestType,
                    SimCondition,ProPath,EventType]
  
"""

import sys

from datetime import datetime

from DIRAC                      import exit as DIRACExit
from DIRAC.Core.Base            import Script
from DIRAC.Core.DISET.RPCClient import RPCClient

from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

__RCSID__  = "$Id$"

def doParse():
  """
  Function that contains all the switches definition and isolates the rest of the
  module from parseCommandLine.
  """
  
  # Switch description
  Script.registerSwitch( 'i:', 'requestID='    , 'ID of the request' )
  Script.registerSwitch( 'r:', 'requestState=' , 'request state(s)' )
  Script.registerSwitch( 't:', 'requestType='  , 'request type(s)' )
  Script.registerSwitch( 'm:', 'simCondition=' , 'simulation condition(s)' )
  Script.registerSwitch( 'p:', 'proPath='      , 'proPath(s)' )
  Script.registerSwitch( 'e:', 'eventType='    , 'eventType(s)' )
  Script.registerSwitch( 'z:', 'sort='         , 'sort requests' )
  Script.registerSwitch( 'n', 'silent'         , 'less verbose mode' )

  # Set script help message
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                          '\nArguments:',
                          '  requestID (string): csv ID(s) of the request, if used other switches are ignored',
                          '  requestState (string): csv states, being "Active" by default',
                          '  requestType (string): csv types, being "Stripping,Reconstruction" by default',
                          '  simCondition (string): csv conditions, being None by default',
                          '  proPath (string): csv paths, being None by default',
                          '  eventType (string): csv events, being None by default',
                          '  sortKey(string) : requests sort key [RequestID,RequestState,RequestType,\
                                               SimCondition,ProPath,EventType]',
                          '  groupMerge: group merge productions in one line',
                          '  omitMerge: omit merge productions on summary\n'] ) )
    
  # Get switches and options from command line
  Script.parseCommandLine()
  
  params = { 
             'RequestID'    : None, 
             'RequestState' : 'Active', 
             'RequestType'  : 'Stripping,Reconstruction', 
             'SimCondition' : None,
             'ProPath'      : None,
             'EventType'    : None
            }
  
  sortKey = 'RequestID'
  silent  = False
  
  for switch in Script.getUnprocessedSwitches():
    
    if switch[ 0 ].lower() in ( 'i', 'requestid' ):
      params[ 'RequestID' ] = switch[ 1 ]
    elif switch[ 0 ].lower() in ( 'r', 'requeststate' ):
      params[ 'RequestState' ] = switch[ 1 ]  
    elif switch[ 0 ].lower() in ( 't', 'requesttype' ):
      params[ 'RequestType' ] = switch[ 1 ]
    elif switch[ 0 ].lower() in ( 'm', 'simcondition' ):
      params[ 'SimCondition' ] = switch[ 1 ]
    elif switch[ 0 ].lower() in ( 'p', 'propath' ):
      params[ 'ProPath' ] = switch[ 1 ]
    elif switch[ 0 ].lower() in ( 'e', 'eventtype' ):
      params[ 'EventType' ] = switch[ 1 ]  
    elif switch[ 0 ].lower() in ( 'z', 'sort' ):
      sortKey = switch[ 1 ]
    elif switch[ 0 ].lower() in ( 'n', 'silent' ):
      silent = True
    
  return params, sortKey, silent
  
def getRequests( parsedInput, sortKey ):
  """
  Gets the requests from the database using the filters given by the user.  
  """
    
  reqClient = RPCClient( 'ProductionManagement/ProductionRequest' )
  
  for key, value in parsedInput.items():
    if value is None:
      del parsedInput[ key ]    
  
  if 'RequestID' in parsedInput:
    parsedInput = { 'RequestID' : parsedInput[ 'RequestID' ] }
      
  requests = reqClient.getProductionRequestList_v2( 0L, 'RequestID', 'DESC', 0L, 0L, parsedInput )
  if not requests[ 'OK' ]:
    print requests[ 'Message' ]
    return
    
  requests = requests[ 'Value' ][ 'Rows' ]
  
  sortedRequests = sorted( requests, key=lambda k:k[ sortKey ] )
  
  parsedRequests = []
  
  for request in sortedRequests:
    
    parsedRequests.append( { 
                             'requestID'    : request[ 'RequestID' ],
                             'requestState' : request[ 'RequestState' ],
                             'requestName'  : request[ 'RequestName' ],
                             'requestType'  : request[ 'RequestType' ],
                             'proPath'      : request[ 'ProPath' ],
                             'simCondition' : request[ 'SimCondition' ],
                             'eventType'    : request[ 'EventType' ]                                 
                             }
                          )
  return parsedRequests
  
def getTransformations( transClient, requestID, silent ):
  """
    Given a requestID, returns all its transformations.
  """
    
  transformations = transClient.getTransformations( { 'TransformationFamily' : requestID } )
  if not transformations[ 'OK' ]:
    print transformations[ 'Message' ]
    return
  
  transformations = transformations[ 'Value' ]
 
  parsedTransformations = {}

  for transformation in transformations:

    transformationID    = transformation[ 'TransformationID' ]
    
    try:
      transformationFiles, badFiles = getFiles( transClient, transformationID )
    except ValueError:
      print 'ERROR !!!!!!!!!!!!!!'
      print transformationID     

    if badFiles and not silent:
      tasks = getTasks( transClient, transformationID )
      
      for _bF in badFiles:
        _bF[ 'jobs' ] = getJobs( transClient, transformationID, _bF[ 'FileID' ], tasks )

    parsedTransformations[ transformationID ] = {
      
      'transformationStatus' : transformation[ 'Status' ],
      'transformationType'   : transformation[ 'Type' ],
      'transformationFiles'  : transformationFiles,
      'badFiles'             : badFiles
    }   
    
  return parsedTransformations
 
def getFiles( transClient, transformationID ):
  """
    Given a transformationID, returns the status of their files.
  """
  
  filesDict = { 
                'Total'     : 0,
                'Processed' : 0
              }
  
  files = transClient.getTransformationFilesSummaryWeb( { 'TransformationID' : transformationID }, 
                                              [], 0, 1000000 )
  
  if not files[ 'OK' ]:
    print files[ 'Message' ]
    return { 'Total' : -1 }
  
  files = files[ 'Value' ]
   
  filesDict[ 'Total' ] = files[ 'TotalRecords' ]
  
  if filesDict[ 'Total' ]:
    filesDict.update( files[ 'Extras' ] )

  # ApplicationCrash to be removed
  states = set( filesDict.keys() ).intersection( set( [ 'MaxReset', 'Problematic', 'ApplicationCrash' ] ) )
  states = list( states )

  badFiles = {}
  
  if states:
    
    _badFiles = transClient.getTransformationFiles( { 
                                                    'TransformationID' : transformationID,
                                                    'Status'           : states 
                                                    } )
    if _badFiles[ 'OK' ]:
      badFiles = _badFiles[ 'Value' ]
    else:
      print _badFiles[ 'Message' ]  

  return filesDict, badFiles 

def getTasks( transClient, transformationID ):
  """
    Given a transformation, get all tasks. Will be used to find which jobs belong
    to a file.
  """

  tasks = {}

  reqs      = transClient.getTransformationTasks( { 'TransformationID' : transformationID } )

  if not reqs['OK']:
    print '      .. some error happened .. production %s, tasks ' % ( transformationID )
    return tasks

  reqs = reqs['Value']

  for task in reqs:
  
    tasks[ task[ 'TaskID' ] ] = {
                                 'RunNumber'      : task[ 'RunNumber' ],
                                 'ExternalID'     : task[ 'ExternalID' ],
                                 'TargetSE'       : task[ 'TargetSE' ],
                                 'LastUpdateTime' : task[ 'LastUpdateTime' ],
                                 'CreationTime'   : task[ 'CreationTime' ],                  
                                 'ExternalStatus' : task[ 'ExternalStatus' ]
                                 }  

  return tasks

def getJobs( transClient, transformationID, fileID, tasks ):
  """
    Gets all jobs for a file in a transformation. In fact, it gets the tasks
    which are mapped to jobs later on.
  """
  
  jobs = []
  _jobs = transClient.getTableDistinctAttributeValues( 'TransformationFileTasks',
                                                      [ 'TransformationID', 'FileID', 'TaskID' ],
                                                      {
                                                        'TransformationID' : transformationID,
                                                        'FileID'           : fileID } )
  
  if not _jobs[ 'OK' ]:
    print _jobs[ 'Message' ]
    return []
    
  foundTasks = _jobs[ 'Value' ][ 'TaskID' ]
  for task in foundTasks:

    jobs.append( tasks[ task ] )  
  
  return jobs     
  
def printSelection( parsedInput, sortKey ):
  """
    Prints header with selection parameters used to filter requests, plus some 
    extra options to narrow summary.
  """
  
  if parsedInput[ 'RequestID' ] is not None:
    parsedInput = { 'RequestID' : parsedInput[ 'RequestID' ] }   
  
  print '\n'
  print '-' * 60
  print '                REQUESTS at %s' % datetime.now().replace( microsecond = 0 )
  print '-' * 60
  
  print '  Selection parameters:'
   
  for key, value in parsedInput.items():
    print '    %s : %s' % ( key.ljust( 25, ' ' ), value )
  
  print '  Display parameters:'
  print '    sort key                  : %s' % ( sortKey )
        
  print '-' * 60  
  print '\n'
  
  printNow()
  
def printResults( request ):  
  """
    Given a dictionary with requests, it prints the content on a human readable way.
    If mergeAction is given and different than None, it can omit all merge
    transformations from the summary or group all them together in one line if 
    the value is group.
  """
  
  #for request in requests.items():
 
  msgTuple = ( request[ 'requestID' ], request[ 'requestState' ], request[ 'requestType' ][:4], 
               request[ 'proPath' ], request[ 'simCondition' ], request[ 'eventType' ] )
  
  msgBuffer = []
  msgLock   = False  
    
  msgBuffer.append( '  o %d [%s][ %s/%s ][ %s/%s ]' % msgTuple )
  
  for transformationID, transformation in request[ 'transformations' ].items():
      
    filesMsg = ''
    
    filesDict = transformation[ 'transformationFiles' ]
                      
    if filesDict[ 'Total' ] == '-1':
      filesMsg += '..Internal error..'
    elif filesDict.has_key( 'MaxReset' ):
      filesMsg += '%s files in MaxReset' % filesDict[ 'MaxReset' ]  
    elif filesDict.has_key( 'Problematic' ):
      filesMsg += '%s files in Problematic' % filesDict[ 'Problematic' ]  
    elif filesDict.has_key( 'ApplicationCrash' ):
      filesMsg += '%s files in ApplicationCrash' % filesDict[ 'ApplicationCrash' ]  
             
    msgTuple = ( ( '%s [%s] %s' % ( transformationID, transformation[ 'transformationStatus' ], 
                 transformation[ 'transformationType' ] ) ).ljust( 35, ' ' ), filesMsg)
    if filesMsg:
      msgLock = True  
      msgBuffer.append( '      %s %s' % msgTuple )
    
    for _badFile in transformation[ 'badFiles' ]:
      
      _msg1 = ( _badFile['FileID'], _badFile[ 'LFN' ], _badFile[ 'ErrorCount' ], _badFile[ 'Status' ] )
      msgBuffer.append('      * file ( ID %s ) %s with ErrorCount %s is %s' % _msg1 )    
      
      if not 'job' in _badFile:
        continue
      for job in _badFile[ 'jobs' ]:
        
        jobID    = job[ 'ExternalID' ]
        status   = job[ 'ExternalStatus' ]
        se       = job[ 'TargetSE' ]
        creation = job[ 'CreationTime' ]
        updated  = job[ 'LastUpdateTime' ]
        
        _jobMsg = ( jobID, status, se, creation, updated )
        jobMsg = '           - Job ID %s with status %s at %s, created %s & updated %s' % _jobMsg     

        msgBuffer.append( jobMsg )

  if msgLock:
    for msg in msgBuffer:
      print msg

  printNow()

def printRequestsInfo( requests ):
  """
    Prints the number of requests 
  """
  
  print ' found %s requests \n' % len( requests )
  printNow()

def printNow():  
  """
    Flush stdout, otherwhise we have to wait until the end of the script to have
    if flushed.
  """
  
  sys.stdout.flush()                
                                        
if __name__ == "__main__":
  # Main function. Parses command line, get requests, their transformations and
  # prints summary.
  
  # Get input from command line
  _parsedInput, _sortKey, _silent = doParse() 
 
  # Print summary header
  printSelection( _parsedInput, _sortKey )

  # Get requests with given filters
  _requests = getRequests( _parsedInput, _sortKey )
  if _requests is None:
    DIRACExit( 2 )
  
  # Print small information 
  printRequestsInfo( _requests )
  
  # Initialized here to avoid multiple initializations due to the for-loop
  transformationClient = TransformationClient()
  
  # Print summary per request
  for _request in _requests:
    
    _requestID = _request[ 'requestID' ]
    
    _transformations = getTransformations( transformationClient, _requestID, _silent )
    _request[ 'transformations' ] = _transformations   
  
    #request = requests[ requestID ]
  
    printResults( _request )
  
  # And that's all folks.
  DIRACExit(0)
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
