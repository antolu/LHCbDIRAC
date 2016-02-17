########################################################################
# $Id: FileParam.py 54098 2012-07-02 16:43:53Z zmathe $
########################################################################

"""
declare a file parameter
"""

__RCSID__ = "$Id$"

class FileParam:

  """
  FileParam class
  """
  #############################################################################
  def __init__(self):
    """initialize the class members"""
    self.name_ = ""
    self.value_ = ""

  #############################################################################
  def setParamName(self, name):
    """sets the file parameter"""
    self.name_ = name

  #############################################################################
  def getParamName(self):
    """returns the file parameter"""
    return self.name_

  #############################################################################
  def setParamValue(self, value):
    """sets the value of the parameter"""
    self.value_ = value

  #############################################################################
  def getParamValue(self):
    """returns the value of the parameter"""
    return self.value_

  #############################################################################
  def __repr__(self):
    """formats the output of print"""
    result = '\nFileParam: \n'
    result += self.name_ + ' ' + self.value_ + '\n'
    return result

  #############################################################################
  def writeToXML(self):
    """creates an xml string"""
    return '    <Parameter  Name="'+ self.getParamName() +'"     Value="'+self.getParamValue()+'"/>\n'

  #############################################################################