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
              If the command itself is dirac-loop, then the first occurrence only of @arg@ is replaced, which allows recursive calls
"""


def reduce(arguments):
  """
  If the arguments look like BK paths (start with /LHCb or /MC), try to reduce the list of BK paths
  by merging event types or file types into a list.
  """
  if noMerge:
    return arguments
  others = []
  placeHolder = '@@'
  for item in [-1, -2]:
    reducedDict = {}
    for path in arguments:
      path = path.strip()
      if path.startswith('/LHCb') or path.startswith('/MC'):
        parsed = path.split('/')
        val = parsed[item]
        parsed[item] = placeHolder
        holderPath = '/'.join(parsed)
        reducedDict.setdefault(holderPath, set()).add(val)
      else:
        others.append(path)
    arguments = [holderPath.replace(placeHolder, ','.join(sorted(values)))
                 for holderPath, values in reducedDict.items()]
  # Now sort out the conditions
  conditions = {}
  finalArgs = [arg for arg in arguments if not arg.startswith('/LHCb')]
  for path in [arg for arg in arguments if arg.startswith('/LHCb')]:
    parsed = path.split('/')
    cond = parsed[3]
    parsed[3] = ''
    conditions.setdefault('/'.join(parsed), []).append((path, cond))
  for newPath, condTuple in conditions.iteritems():
    if len(condTuple) != 2:
      finalArgs += [path for (path, _c) in condTuple]
    else:
      if len(set(['-'.join(cond.split('-')[0:2])])) == 1:
        finalArgs.append(newPath)
      else:
        finalArgs += [path for (path, _c) in condTuple]

  return sorted(others + finalArgs)


if __name__ == '__main__':

  import sys
  import os
  import subprocess
  from DIRAC import gLogger, exit
  from DIRAC.Core.Base import Script

  Script.registerSwitch('', 'NoMerge', 'If set, do not merge arguments if BK paths')
  Script.registerSwitch('', 'Items=', 'Alternative way of passing list of arguments')
  Script.registerSwitch('', 'NoParse', 'Consider the full lines are arguments and not only the first word')
  Script.registerSwitch('', 'Terse', 'Do not print the command executed')
  Script.setUsageMessage('\n'.join([__doc__]))
  Script.parseCommandLine(ignoreErrors=True)
  args = Script.getPositionalArgs()

  noMerge = False
  noParse = False
  terse = False
  arguments = []
  for switch, val in Script.getUnprocessedSwitches():
    if switch == 'NoMerge':
      noMerge = True
    elif switch == 'NoParse':
      noParse = True
    elif switch == 'Items':
      arguments = val.split(',')
    elif switch == 'Terse':
      terse = True

  if len(args) < 1:
    Script.showHelp()
    exit(0)

  if not arguments:
    if os.path.exists(args[0]):
      file = args.pop(0)
    else:
      file = '/dev/stdin'
    with open(file, 'r') as fd:
      arguments = fd.read().split('\n')[:-1]

  commands = args

  argList = []
  for arg in arguments:
    if not arg.strip():
      continue
    # If the argument is between quotes, take what is between the quotes
    if arg[0] == "'":
      arg = arg.split("'")[1]
    elif arg[0] == '"':
      arg = arg.split('"')[1]
    elif noParse:
      # Consider the whole line is the argument
      pass
    else:
      # Take the firt word
      arg = arg.strip().split()[0]
    # Escape any space left
    argList.append(arg.replace(' ', '\ '))

  for arg in reduce(argList):
    if arg:
      for command in commands:
        if '@arg@' in command:
          c = command.replace('@arg@', arg, 1) if 'dirac-loop' in command else \
              command.replace('@arg@', arg)
        elif command[-1] in ('"', "'"):
          c = command + arg.replace('\ ', ' ') + command[-1]
        else:
          c = command + ' ' + arg
        if not terse:
          gLogger.notice('======= %s =========' % c)
        try:
          output = subprocess.check_output([x for x in c.split() if x], stderr=subprocess.STDOUT)
          gLogger.notice(output[:-1] if terse else output)
        except subprocess.CalledProcessError as e:
          gLogger.exception("Error calling command", lException=e)
