#!/usr/bin/env python
""" getting the produced environment saved in the file
"""

# environmentLbLogin
environment = {}
fp = open( 'environmentLbLogin', 'r' )
for line in fp:
  if line[0] == '#':
    continue
  try:
    var = line.split( '=' )[0].strip()
    value = line.split( '=' )[1].strip()
    if '{' in value:
      value = value + '\n}'
    environment[var] = value
  except IndexError:
    continue


# bashrc
environmentBashrc = {}
fp = open( 'bashrc', 'r' )
for line in fp:
  if line[0] == '#':
    continue
  try:
    var = line.split( '=' )[0].strip().replace('export', '')
    value = line.split( '=' )[1].strip().replace('export', '')
    environmentBashrc[var] = value
  except IndexError:
    continue

# committing
environment.update(environmentBashrc)
fp = open( 'environmentLbLoginbashrc', 'w' )
for key, value in environment.iteritems():
  line = "export %s=%s\n" %(key, value)
  fp.write(line)
fp.close()
