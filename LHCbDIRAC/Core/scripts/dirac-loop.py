#!/usr/bin/env python
"""
  Execute a (list of) commands taking arguments from a file.
  Usage:
      dirac-loop [File] Command [Command2...] [option|cfgfile]

  Arguments:
     File: File containing the strings to be appended to the command (default /dev/stdin)
           If the arguments are BK paths, the script reduces the paths merging event and file types (use --NoMerge to disable)
     Command: Command to execute (separate with spaces)
              If command terminates with ", a " is appended after the argument
              If the command contains the string @arg@, it is replaced by the argument rather than the argument appended
"""

def reduce( arguments ):
  if noMerge:
    return arguments
  others = []
  placeHolder = '@@'
  for item in [-1, -2]:
    reducedDict = {}
    for path in arguments:
      path = path.strip()
      if path.startswith( '/LHCb' ) or path.startswith( '/MC' ):
        parsed = path.split( '/' )
        val = parsed[item]
        parsed[item] = placeHolder
        holderPath = '/'.join( parsed )
        reducedDict.setdefault( holderPath, set() ).add( val )
      else:
        others.append( path )
    arguments = [holderPath.replace( placeHolder, ','.join( sorted( values ) ) ) for holderPath, values in reducedDict.items()]
  # Now sort out the conditions
  conditions = {}
  finalArgs = [arg for arg in arguments if not arg.startswith( '/LHCb' )]
  for path in [arg for arg in arguments if arg.startswith( '/LHCb' )]:
    parsed = path.split( '/' )
    cond = parsed[3]
    parsed[3] = ''
    conditions.setdefault( '/'.join( parsed ), [] ).append( ( path, cond ) )
  for newPath, condTuple in conditions.items():
    if len( condTuple ) != 2:
      finalArgs += [path for ( path, _c ) in condTuple]
    else:
      if len( set( ['-'.join( cond.split( '-' )[0:2] )] ) ) == 1:
        finalArgs.append( newPath )
      else:
        finalArgs += [path for ( path, _c ) in condTuple]

  return sorted( others + finalArgs )

if __name__ == '__main__':

  import sys, os
  from DIRAC import gLogger, exit
  from DIRAC.Core.Base import Script

  Script.registerSwitch( '', 'NoMerge', 'If set, do not merge arguments if BK paths' )
  Script.registerSwitch( '', 'Items=', 'Alternative way of passing list of arguments' )
  Script.setUsageMessage( '\n'.join( [ __doc__ ] ) )
  Script.parseCommandLine( ignoreErrors = True )
  args = Script.getPositionalArgs()

  noMerge = False
  arguments = []
  for switch, val in Script.getUnprocessedSwitches():
    if switch == 'NoMerge':
      noMerge = True
    elif switch == 'Items':
      arguments = val.split( ',' )

  if len( args ) < 1:
    Script.showHelp()
    exit( 0 )

  if not arguments:
    if os.path.exists( args[0] ):
      file = args.pop( 0 )
    else:
      file = '/dev/stdin'
    f = open( file, 'r' )
    arguments = f.read().split( '\n' )[:-1]
    f.close()

  commands = args

  arguments = [arg.strip().split()[0] for arg in arguments]

  for arg in reduce( arguments ):
    if arg:
      for command in commands:
        if '@arg@' in command:
          c = command.replace( '@arg@', arg, 1 )
        elif command[-1] in ( '"', "'" ):
          c = command + arg + command[-1]
        else:
          c = command + ' ' + arg
        gLogger.always( '======= %s =========' % c )
        pipe = os.popen( c )
        gLogger.always( pipe.read() )
        pipe.close()

