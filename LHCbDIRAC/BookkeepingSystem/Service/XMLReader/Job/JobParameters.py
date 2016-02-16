########################################################################
# $Id: JobParameters.py 54098 2012-07-02 16:43:53Z zmathe $
########################################################################

"""
stores the job parameters
"""

__RCSID__ = "$Id: JobParameters.py 54098 2012-07-02 16:43:53Z zmathe $"

class JobParameters:
  """
  JobParameters class
  """

  #############################################################################
  def __init__(self):
    """initialize the class members"""
    self.name_ = ""
    self.value_ = ""
    self.type_ = ""

  #############################################################################
  def setName(self, name):
    """sets the name"""
    self.name_ = name

  #############################################################################
  def getName(self):
    """retuns the name"""
    return self.name_

  #############################################################################
  def setValue(self, value):
    """sets the value"""
    self.value_ = value

  #############################################################################
  def getValue(self):
    """returns the value"""
    return self.value_

  #############################################################################
  def setType(self, ptype):
    """sets the type"""
    self.type_ = ptype

  #############################################################################
  def getType(self):
    """returns the type"""
    return self.type_

  #############################################################################
  def __repr__(self):
    """formats the output of the print"""
    result = self.name_ + '  ' + self.value_ + '  ' + self.type_ + '\n'
    return result

  #############################################################################
  def writeToXML(self):
    """creates an xml string"""
    result = '  <TypedParameter Name="' + str(self.getName()) + \
                     '" Value="' + str(self.getValue()) + '" Type="' + str(self.getType()) + '"/>\n'
    return result
  #############################################################################

