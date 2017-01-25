#!/usr/bin/env python
""" getting the produced environment saved in the file
"""

# environmentLbLogin
environment = {}
fp = open( 'environmentLbLogin', 'r' )
for line in fp:
  if line[0] == '#' or 'SSH' in line:
    continue
  try:
    var = line.split( '=' )[0].strip()
    value = line.split( '=' )[1].strip()
    if '{' in value:
      value = value + '\n}'
    environment[var] = value
  except IndexError:
    pass


# bashrc
environmentBashrc = {}
functsBahsrc = {}
diracFunct = {}
fp = open( 'bashrc', 'r' )
for line in fp:
  if line[0] == '#' or 'DIRACPLAT' in line:
    continue
  if '(' in line or '[' in line:
    try:
      var = line.split( 'export' )[0]
      value = line.split( 'export' )[1].strip()
      if 'DIRAC=' in value:
        diracFunct[var] = value
      else:
        functsBahsrc[var] = value
    except IndexError:
      pass
  else:
    try:
      var = line.replace('export', '').split( '=' )[0].strip()
      value = line.replace('export', '').split( '=' )[1].strip()
      environmentBashrc[var] = value
    except IndexError:
      pass

# Combining
for key, value in environmentBashrc.iteritems():
  if key in environment:
    environment[key] = value + ':' + environment[key]
  else:
    environment[key] = value

# committing
fp = open( 'environmentLbLoginbashrc', 'w' )
for key, value in diracFunct.iteritems():
  line = "%s export %s" %(key, value)
  fp.write(line)
# first exports (there are only variables, with one function that will be ignored)
for key, value in environment.iteritems():
  if '(' not in key:
    line = "export %s=%s\n" %(key, value)
    fp.write(line)
# then functs
for key, value in functsBahsrc.iteritems():
  line = "%s export %s\n" %(key, value)
  fp.write(line)
fp.close()
