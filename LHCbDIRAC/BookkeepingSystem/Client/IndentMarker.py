########################################################################
# $Id: IndentMarker.py,v 1.1 2008/06/06 11:04:02 zmathe Exp $
########################################################################

"""
"""

__RCSID__ = "$Id: IndentMarker.py,v 1.1 2008/06/06 11:04:02 zmathe Exp $"
TAB     = "\t"
SPACE   = " "
DEFAULT   = "___"


#############################################################################
def _createIndentedString(str, indent):
  str = str.strip('\n')
  tokens = str.split("\n")
  newstr = ""
  for token in tokens[0:-1]:
     newstr += indent + token + "\n"
  newstr += indent + tokens[-1]
  return newstr

#############################################################################
def prepend(str, indent = DEFAULT):
  return _createIndentedString(str, indent)

#############################################################################
def append(value, suffix):
  lines = value.split('\n')
  maxLine = 0
  for line in lines:
    length = len(line)
    if length > maxLine:
      maxLine = length
  s = ''
  format = '%-' + str(maxLine) + 's'  
  for line in lines:
    s += format % line
    s += ' ' + suffix + ' \n'
  return s.strip('\n')  

#############################################################################