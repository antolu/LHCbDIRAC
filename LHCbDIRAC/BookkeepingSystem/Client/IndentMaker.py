########################################################################
# $Id: IndentMaker.py 69359 2013-08-08 13:57:13Z phicharp $
########################################################################
"""
Its ident the debug message
"""

__RCSID__ = "$Id$"
TAB = "\t"
SPACE = " "
DEFAULT = "___"


#############################################################################
def _createIndentedString(string, indent):
  """create string"""
  string = string.strip('\n')
  tokens = string.split("\n")
  newstr = ""
  for token in tokens[0:-1]:
    newstr += indent + token + "\n"
  newstr += indent + tokens[-1]
  return newstr

#############################################################################
def prepend(string, indent=DEFAULT):
  """ add string"""
  return _createIndentedString(string, indent)

#############################################################################
def append(value, suffix):
  """append...."""
  lines = value.split('\n')
  maxLine = 0
  for line in lines:
    length = len(line)
    if length > maxLine:
      maxLine = length
  string = ''
  formats = '%-' + str(maxLine) + string
  for line in lines:
    string += formats % line
    string += ' ' + suffix + ' \n'
  return string.strip('\n')

#############################################################################

