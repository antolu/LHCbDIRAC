#!/usr/bin/env python

################################################################################
#
# DISCLAIMER: this is an old prototype. If used, needs to be rewritten properly!
#
################################################################################

bccolors = {

             'purple': '\033[95m',
             'green' : '\033[92m',
             'blue'  : '\033[94m',
             'yellow': '\033[93m',
             'red'   : '\033[91m',
             'end'   : '\033[0m'

           }

print bccolors[ 'purple' ]
print '#' * 126
print 'ERROR FINDER'.rjust( 63, ' ' )
print '#' * 126
print bccolors[ 'end' ]

module      = None
compoType   = None
logsSize    = 100
maxHours    = 24
printLogs   = False
lastRelease = False

SEPARATOR = '.'

import os
from datetime import datetime
import DIRAC
from DIRAC           import gLogger
from DIRAC.Core.Base import Script

Script.registerSwitch( "g", "logs"         ,"      print logs" )
Script.registerSwitch( "l:", "logsize="    ,"      size of logs going to be analized" )
Script.registerSwitch( "m:", "module="     ,"      module to be analized" )
Script.registerSwitch( "t:", "type="       ,"      type ( Service or Agent )" )
Script.registerSwitch( "u:", "hours="      ,"      max number of hours in the past of the logs" )
Script.registerSwitch( ""  , "lastRelease" ,"      analyse logs since last release" )

Script.setUsageMessage( '\n'.join( [ '\nUsage:',
                                     '  %s [option|cfgfile] <logsize> <module>' % Script.scriptName,
                                     '\nArguments:',
                                     '  logs: print logs\n',
                                     '  logsize (integer): size of logs going to be analized\n',
                                     '  module (string)  : module to be analized\n',
                                     '  type (string) : type of components to be analized\n',
                                     '  hours (intefer) : max number of hours of logs to be considered\n',
                                     '  lastRelease: analyse only since last release'
                                   ]
                                  ) )
Script.parseCommandLine()

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "l" or switch[0].lower() == "logsize":
    logsSize = int( switch[ 1 ] )
  elif switch[0].lower() == "m" or switch[0].lower() == "module":
    module = switch[ 1 ]
  elif switch[0].lower() == "t" or switch[0].lower() == "type":
    compoType = switch[ 1 ]
  elif switch[0].lower() == "u" or switch[0].lower() == "hours":
    maxHours = int( switch[ 1 ] )
  elif switch[0].lower() == "g" or switch[0].lower() == "logs":
    printLogs = True
  elif switch[0].lower() == "lastrelease":
    lastRelease = True
    t = os.path.getmtime( '/opt/dirac/pro/DIRAC/__init__.py' )
    diff = datetime.now() - datetime.fromtimestamp(t)
    maxHours = diff.days * 24 + diff.seconds / 3600.

import sys

from datetime import datetime, timedelta

from DIRAC import siteName, version
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals
from LHCbDIRAC import version as lhcbdiracversion

setup      = CSGlobals.getSetup()
vo         = CSGlobals.getVO()
extensions = CSGlobals.getInstalledExtensions()
siteName   = siteName()
dirac      = version
lhcbdirac  = lhcbdiracversion

print '  Setup'.ljust( 22, ' ' )                + ( ': %s' % setup )
print '  VO'.ljust( 22, ' ' )                   + ( ': %s' % vo )
print '  Installed extensions'.ljust( 22, ' ' ) + ( ': %s' % ( ', '.join( extensions ) ) )
print '  SiteName'.ljust( 22, ' ' )             + ( ': %s' % siteName )
print '  DIRAC'.ljust( 22, ' ' )                + ( ': %s' % dirac )
print '  LHCbDIRAC'.ljust( 22, ' ' )            + ( ': %s' % lhcbdirac )
print '  Logs size'.ljust( 22, ' ' )            + ( ': %s' % logsSize )
print '  Module restriction'.ljust( 22, ' ' )   + ( ': %s' % module )
print '  Max. Hours'.ljust( 22, ' ' )           + ( ': %s' % maxHours )
print '  Last Release'.ljust( 22, ' ' )         + ( ': %s' % lastRelease )
print ''

from LHCbDIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller

extensions2 = [ ext.replace( 'DIRAC', '' ) for ext in extensions ]
status = gComponentInstaller.getOverallStatus( extensions2 )

if not status[ 'OK' ]:
  print 'Something went wrong: /n %s' % status[ 'Message' ]
  sys.exit( 1 )

status = status[ 'Value' ]

def checkProperties( properties ):

  printFlag = False

  if properties[ 'Setup' ] == False:
    printFlag = True
  elif properties[ 'PID' ] == 0:
    printFlag = True
  elif properties[ 'RunitStatus' ] != 'Run':
    printFlag = True
  elif properties[ 'Timeup' ] == 0:
    printFlag = True
  elif properties[ 'Installed' ] == False:
    printFlag = True

  _msg = ''

  if not printFlag:
    return _msg

  for k, v in properties.items():
    _msg += ( '        %s' % k ).ljust( 20, ' ' ) + ( ' : %s' % v ) + '\n'

  return _msg

def checkLogs( systemName, element ):

  logs = gComponentInstaller.getLogTail( systemName, element, length = logsSize )

  if not logs[ 'OK' ]:
    print logs[ 'Message' ]

  logs         = logs[ 'Value' ][ '%s_%s' % ( systemName, element ) ]

  splittedlogs = logs.split( '\n' )
  lowerlogs    = logs.lower()

  ERROR_KEYWORDS = [ "ERROR", "EXCEPTION", "'Message'" ]

  LERROR_KEYWORDS = [ kw.lower() for kw in ERROR_KEYWORDS ]

  _msg = ''

  for error_keyword in LERROR_KEYWORDS:

    if error_keyword in lowerlogs:

      errorbuffer = parseLogs( splittedlogs, LERROR_KEYWORDS )
      if errorbuffer == []:
        _msg =  'Errorbuffer is empty, errors are not fresh ( older than %s hours )' % maxHours
      else:
        _msg =  '\n'.join( errorbuffer )

      return _msg

  return _msg

def parseLogs( loglines, error_keywords ):

  lowerloglines = [ l.lower() for l in loglines ] #logs.lower().split( '\n' )
  errorbuffer   = []

  timeWindow = datetime.utcnow() - timedelta( hours = maxHours )

  for _l in xrange( 0, len( loglines ) ):

    for error_keyword in error_keywords:

      if error_keyword in lowerloglines[ _l ]:
        try:
          t = datetime.strptime( loglines[ _l ].rsplit( ' UTC' )[ 0 ], '%Y-%m-%d %H:%M:%S' )
        except:
          errorbuffer.append( loglines[ _l ] )
          continue

        if t < timeWindow:
          continue

        errorbuffer.append( loglines[ _l ] )

  return errorbuffer

errorsDict = {}

# Copied from gComponentInstaller
def printOverallStatus( rDict ):
  """
  Print in nice format the return dictionary from getOverallStatus
  """
  try:
    print bccolors[ 'purple' ]
    print '-' * 126
    print "...System" + SEPARATOR*21 + 'Name' + SEPARATOR*23 + 'Type' + SEPARATOR*7 + 'Setup....Installed.......Runit....Uptime....PID.....ERRORS'
    print '-' * 126
    print bccolors[ 'end' ]

    for compType in rDict:

      if compoType is not None and not compType.startswith( compoType ):
        continue

      for system in rDict[compType]:

        if module is not None and system != module:
          continue

        for component in rDict[compType][system]:

          setup, installed, runitstatus, timeup, pid, errorlogs = False, False, False, False, False, '?'
          setup       = rDict[compType][system][component]['Setup']
          installed   = rDict[compType][system][component]['Installed']
          runitstatus = rDict[compType][system][component]['RunitStatus'] != 'Unknown'
          timeup      = rDict[compType][system][component]['Timeup'] != 0
          pid         = rDict[compType][system][component]['PID'] != 0

          _output = ''


          if not( setup and installed and runitstatus and timeup and pid ):
            _output += bccolors['yellow'] + system.ljust( 28, SEPARATOR ) + bccolors[ 'end' ] + component.ljust( 28, SEPARATOR ) + compType.lower()[:-1].ljust( 11, SEPARATOR )
          else:
            errorLogs = checkLogs( system, component )
            if errorLogs == '':
              errorlogs = 'no'
              color = bccolors[ 'green' ]
            else:
              errorlogs = 'yes'
              color = bccolors[ 'red' ]

              if not errorsDict.has_key( system ):
                errorsDict[ system ] = {}
              if not errorsDict[ system ].has_key( compType ):
                errorsDict[ system ][ compType ] = {}
              errorsDict[ system ][ compType ][ component ] = errorLogs

            _output += color + system.ljust( 28, SEPARATOR ) + bccolors[ 'end' ] + component.ljust( 28, SEPARATOR ) + compType.lower()[:-1].ljust( 11, SEPARATOR )

          if setup:
            _output += 'SetUp'.rjust( 10, SEPARATOR )
          else:
            _output += bccolors[ 'yellow' ] + 'NotSetup'.rjust( 10, SEPARATOR ) + bccolors[ 'end' ]
          if installed:
            _output += 'Installed'.rjust( 15, SEPARATOR )
          else:
            _output += bccolors[ 'yellow' ] + 'NotInstalled'.rjust( 15, SEPARATOR ) + bccolors[ 'end' ]
          if runitstatus:
            _output += rDict[compType][system][component]['RunitStatus'].rjust( 8, SEPARATOR )
          else:
            _output += bccolors[ 'yellow' ] + rDict[compType][system][component]['RunitStatus'].rjust( 8, SEPARATOR ) + bccolors[ 'end' ]
          if timeup:
            _output += str( rDict[compType][system][component]['Timeup'] ).rjust( 8, SEPARATOR )
          else:
            _output += bccolors[ 'yellow' ] + str( rDict[compType][system][component]['Timeup'] ).rjust( 8, SEPARATOR ) + bccolors[ 'end' ]
          if pid:
            _output += str( rDict[compType][system][component]['PID'] ).rjust( 8, SEPARATOR )
          else:
            _output += bccolors[ 'yellow' ] + str( rDict[compType][system][component]['PID'] ).rjust( 8, SEPARATOR ) + bccolors[ 'end' ]
          if errorlogs == 'yes':
            _output += bccolors[ 'red' ] + errorlogs.rjust( 8, SEPARATOR ) + bccolors[ 'end' ]
          else:
            _output += errorlogs.rjust( 8, SEPARATOR )
          print _output

  except Exception:
    pass

def printErrorLogs( rDict ):

  print bccolors[ 'purple' ]
  print '-' * 126
  print 'ERROR LOGS'.rjust( 63 )
  print '-' * 126
  print bccolors[ 'end' ]

  for system in errorsDict:

    if module is not None and system != module:
      continue
    print bccolors[ 'blue' ] + '.'*20 + ' ' + system + ' ' + '.'*20 + bccolors[ 'end' ]

    for compType in errorsDict[ system ]:

      if compoType is not None and not compType.startswith( compoType ):
        continue

      print '\n' + (' '*2) + compType

      for component, logs in errorsDict[ system ][ compType ].items():
        print '\n' + (' '*4) + bccolors[ 'red' ] + component
        print logs + bccolors[ 'end' ]

printOverallStatus( status )
if printLogs:
  printErrorLogs( status )

sys.exit( 0 )
