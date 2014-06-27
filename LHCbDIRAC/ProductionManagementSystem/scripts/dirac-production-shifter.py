#!/usr/bin/env python
''' dirac-production-shifter 
 
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
  - groupMerge    : group all merge productions in one line
  - omitMerge     : omit all merge production
  - noFiles       : do not request file information
  
'''

import sys

from datetime                   import datetime

from DIRAC                      import exit as DIRACExit
from DIRAC.Core.Base            import Script
from DIRAC.Core.DISET.RPCClient import RPCClient

__RCSID__  = "$Id$"

def doParse():
  '''
  Function that contains all the switches definition and isolates the rest of the
  module from parseCommandLine.
  '''
  
  # Switch description
  Script.registerSwitch( 'i:', 'requestID='    , 'ID of the request' )
  Script.registerSwitch( 'r:', 'requestState=' , 'request state(s)' )
  Script.registerSwitch( 't:', 'requestType='  , 'request type(s)' )
  Script.registerSwitch( 'm:', 'simCondition=' , 'simulation condition(s)' )
  Script.registerSwitch( 'p:', 'proPath='      , 'proPath(s)' )
  Script.registerSwitch( 'e:', 'eventType='    , 'eventType(s)' )
  Script.registerSwitch( 'z:', 'sort='         , 'sort requests' )
  Script.registerSwitch( 'g',  'groupMerge'    , 'group merge productions' )
  Script.registerSwitch( 'x',  'omitMerge'     , 'omit all merge productions' )
  Script.registerSwitch( 'n',  'noFiles'       , 'do not show file information' )

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
                          '  omitMerge: omit merge productions on summary',
                          '  noFiles: do not report file status\n'] ) )
    
  # Get switches and options from command line
  Script.parseCommandLine()
  
  params = { 'RequestID'    : None,
             'RequestState' : 'Active', 
             'RequestType'  : 'Stripping,Reconstruction', 
             'SimCondition' : None,
             'ProPath'      : None,
             'EventType'    : None}
  
  mergeAction, noFiles, sortKey = None, False, 'RequestID'
  
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
    elif switch[ 0 ].lower() in ( 'g', 'groupmerge' ):
      mergeAction = 'group'
    elif switch[ 0 ].lower() in ( 'x', 'omitmerge' ):
      mergeAction = 'omit'
    elif switch[ 0 ].lower() in ( 'n', 'nofiles' ):
      noFiles = True
    
  return params, mergeAction, noFiles, sortKey
  
def getRequests( parsedInput, sortKey ):
  '''
  Gets the requests from the database using the filters given by the user.  
  '''
    
  reqClient = RPCClient( 'ProductionManagement/ProductionRequest' )
  
  for key, value in parsedInput.items():
    if value is None:
      del parsedInput[ key ]    
  
  if 'RequestID' in parsedInput:
    parsedInput = { 'RequestID' : parsedInput[ 'RequestID' ] }
      
  requests = reqClient.getProductionRequestList( 0L, 'RequestID', 'DESC', 0L, 0L, parsedInput )
  if not requests[ 'OK' ]:
    print requests[ 'Message' ]
    return
    
  requests = requests[ 'Value' ][ 'Rows' ]
  
  sortedRequests = sorted( requests, key=lambda k:k[ sortKey ] )
  
  parsedRequests = []
  
  for request in sortedRequests:
    
    parsedRequests.append( { 'requestID'    : request[ 'RequestID' ],
                             'requestState' : request[ 'RequestState' ],
                             'requestName'  : request[ 'RequestName' ],
                             'requestType'  : request[ 'RequestType' ],
                             'proPath'      : request[ 'ProPath' ],
                             'simCondition' : request[ 'SimCondition' ],
                             'eventType'    : request[ 'EventType' ] } )
  return parsedRequests
  
def getTransformations( transClient, requestID, noFiles ):
  '''
    Given a requestID, returns all its transformations.
  '''
    
  transformations = transClient.getTransformations( { 'TransformationFamily' : requestID } )
  if not transformations[ 'OK' ]:
    print transformations[ 'Message' ]
    return
  
  transformations = transformations[ 'Value' ]
 
  parsedTransformations = {}

  for transformation in transformations:

    transformationID    = transformation[ 'TransformationID' ]
    
    if not noFiles:
      transformationFiles = getFiles( transClient, transformationID )
    else:
      transformationFiles = {}  

    parsedTransformations[ transformationID ] = {'transformationStatus' : transformation[ 'Status' ],
                                                 'transformationType'   : transformation[ 'Type' ],
                                                 'transformationFiles'  : transformationFiles}

  return parsedTransformations
  
def getFiles( transClient, transformationID ):
  '''
    Given a transformationID, returns the status of their files.
  '''
  
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

  return filesDict  
  
def printSelection( parsedInput, mergeAction, noFiles, sortKey ):
  '''
    Prints header with selection parameters used to filter requests, plus some 
    extra options to narrow summary.
  '''
  
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
  print '    display merge transformations: %s' % ( ( 1 and mergeAction ) or 'all' )
  print '    get files information        : %s' % ( not noFiles )
  print '    sort key                     : %s' % ( sortKey )
        
  print '-' * 60  
  print '\n'
  
  printNow()
  
def printResults( request, mergeAction ):  
  '''
    Given a dictionary with requests, it prints the content on a human readable way.
    If mergeAction is given and different than None, it can omit all merge
    transformations from the summary or group all them together in one line if 
    the value is group.
  '''
 
  msgTuple = ( request[ 'requestID' ], request[ 'requestState' ], request[ 'requestType' ][:4], 
               request[ 'proPath' ], request[ 'simCondition' ], request[ 'eventType' ] )
    
  print '  o %d [%s][ %s/%s ][ %s/%s ]' % msgTuple
    
  groupedMerge = [ 0, 0, 0 ]
  
  for transformationID, transformation in request[ 'transformations' ].items():
      
    filesMsg = ''
    
    filesDict = transformation[ 'transformationFiles' ]
      
    # Using switch noFiles
    if filesDict == {}:
      continue
      
    if mergeAction == 'omit' and transformation[ 'transformationType' ] == 'Merge':
      continue
      
    if mergeAction == 'group' and transformation[ 'transformationType' ] == 'Merge':
        
      groupedMerge[ 0 ] += 1  
        
      if filesDict[ 'Total' ]:
          
        groupedMerge[ 1 ] += filesDict[ 'Processed' ]
        groupedMerge[ 2 ] += filesDict[ 'Total' ]
        
      continue
      
    if filesDict[ 'Total' ] == '-1':
      filesMsg = '..Internal error..'
    elif filesDict[ 'Total' ] == 0:
      filesMsg = '..No files at all..'
    else:      
      try:
        processed = ( float( filesDict[ 'Processed' ] ) / float( filesDict[ 'Total' ] ) ) * 100
        filesMsg = '%.2f %% ( %d total files )' % ( processed, filesDict['Total'] )
      except KeyError:
        print "No files processed"

             
    msgTuple = ( ( '%s [%s] %s' % ( transformationID, transformation[ 'transformationStatus' ], 
                 transformation[ 'transformationType' ] ) ).ljust( 40, ' ' ), filesMsg)
      
    print '      %s %s' % msgTuple

  if mergeAction == 'group' and groupedMerge[ 0 ]:
    
    if groupedMerge[ 2 ]:
      processed = ( float( groupedMerge[ 1 ] ) / float( groupedMerge[ 2 ] ) ) * 100
      filesMsg = '%.2f %% ( %d total files )' % ( processed, groupedMerge[ 2 ] ) 
    else:
      filesMsg = '..No files at all..'
  
    print '      %s %s' % (( '%s merge prod(s) grouped' % groupedMerge[0] ).ljust( 40, ' ' ), filesMsg )

  printNow()

def printRequestsInfo( requests ):
  '''
    Prints the number of requests 
  '''
  
  print ' found %s requests \n' % len( requests )
  printNow()

def printNow():  
  '''
    Flush stdout, otherwhise we have to wait until the end of the script to have
    if flushed.
  '''
  
  sys.stdout.flush()
                                        
if __name__ == "__main__":
  # Main function. Parses command line, get requests, their transformations and
  # prints summary.

  # Get input from command line
  _parsedInput, _mergeAction, _noFiles, _sortKey = doParse() 
 
  # Print summary header
  printSelection( _parsedInput, _mergeAction, _noFiles, _sortKey )

  # Get requests with given filters
  _requests = getRequests( _parsedInput, _sortKey )
  if _requests is None:
    DIRACExit( 2 )
  
  # Print small information 
  printRequestsInfo( _requests )
  
  # Initialized here to avoid multiple initializations due to the for-loop
  transformationClient = RPCClient( 'Transformation/TransformationManager' )
  
  # Print summary per request
  for _request in _requests:
    
    _requestID = _request[ 'requestID' ]
    
    _transformations = getTransformations( transformationClient, _requestID, _noFiles )
    _request[ 'transformations' ] = _transformations   
  
    #request = requests[ requestID ]
  
    printResults( _request, _mergeAction )
  
  # And that's all folks.
  DIRACExit(0)
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  
