#!/bin/env python
""" Show request given its name, a jobID or a transformation and a task """
__RCSID__ = "$Id: $"

from DIRAC.Core.Base import Script
Script.registerSwitch( '', 'Job=', '   = JobID' )
Script.registerSwitch( '', 'Transformation=', '   = transID' )
Script.registerSwitch( '', 'Tasks=', '   = list of taskIDs' )
Script.registerSwitch( '', 'Full', '   Print more information' )
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     ' %s [option|cfgfile] [requestName|requestID]' % Script.scriptName,
                                     'Arguments:',
                                     ' requestName: a request name' ] ) )
# # execution
if __name__ == "__main__":

  from DIRAC.Core.Base.Script import parseCommandLine
  parseCommandLine()

  import DIRAC

  job = None
  requestName = ""
  transID = None
  tasks = None
  requests = None
  full = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Job':
      try:
        job = int( switch[1] )
      except:
        print "Invalid jobID", switch[1]
    elif switch[0] == 'Transformation':
      try:
        transID = int( switch[1] )
      except:
        print 'Invalid transID', switch[1]
    elif switch[0] == 'Tasks':
      try:
        taskIDs = [int( task ) for task in switch[1].split( ',' )]
      except:
        print 'Invalid tasks', switch[1]
    elif switch[0] == 'Full':
      full = True

  if transID:
    if not taskIDs:
      Script.showHelp()
      DIRAC.exit( 2 )
    requests = ['%08d_%08d' % ( transID, task ) for task in taskIDs]

  elif not job:
    args = Script.getPositionalArgs()

    if len( args ) == 1:
      requests = args[0].split( ',' )
  else:
    from DIRAC.Interfaces.API.Dirac                              import Dirac
    dirac = Dirac()
    res = dirac.attributes( job )
    if not res['OK']:
      print "Error getting job parameters", res['Message']
    else:
      jobName = res['Value'].get( 'JobName' )
      if not jobName:
        print 'Job %d not found' % job
      else:
        requests = [jobName + '_job_%d' % job]

  if not requests:
    Script.showHelp()
    DIRAC.exit( 2 )

  for requestName in requests:
    if requestName:
      from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
      reqClient = ReqClient()

      try:
        requestName = reqClient.getRequestName( int( requestName ) )
        if requestName['OK']:
          requestName = requestName['Value']
      except ValueError:
        pass

      request = reqClient.peekRequest( requestName )
      if not request["OK"]:
        DIRAC.gLogger.error( request["Message"] )
        DIRAC.exit( -1 )

      request = request["Value"]
      if not request:
        DIRAC.gLogger.info( "no such request" )
        DIRAC.exit( 0 )

      DIRAC.gLogger.always( "Request name='%s' ID=%s Status='%s' %s" % ( request.RequestName,
                                                                       request.RequestID,
                                                                       request.Status,
                                                                       "error=%s" % request.Error if request.Error else "" ) )
      for i, op in enumerate( request ):
        prStr = ''
        if full:
          if op.SourceSE:
            prStr += 'SourceSE: %s' % op.SourceSE
          if op.TargetSE:
            prStr += ' - ' if prStr else '' + 'TargetSE: %s' % op.TargetSE
        DIRAC.gLogger.always( "  [%s] Operation Type='%s' ID=%s Order=%s Status='%s' %s" % ( i, op.Type, op.OperationID,
                                                                                             op.Order, op.Status,
                                                                                             "error=%s" % op.Error if op.Error else "" ) )
        if prStr:
          DIRAC.gLogger.always( "      %s" % prStr )
        for j, f in enumerate( op ):
          DIRAC.gLogger.always( "    [%02d] ID=%s LFN='%s' Status='%s' %s" % ( j + 1, f.FileID, f.LFN, f.Status,
                                                                               "error=%s" % f.Error if f.Error else "" ) )



