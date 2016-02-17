########################################################################
# $Id: QualityParameters.py 54098 2012-07-02 16:43:53Z zmathe $
########################################################################

"""
stores the data quality informations
"""

__RCSID__ = "$Id$"

class QualityParameters:
  """
  QualityParameters class
  """
  #############################################################################
  def __init__(self):
    """initialize the class members"""
    self.name_ = ""
    self.value_ = ""

  #############################################################################
  def setName(self, name):
    """sets the parameter name"""
    self.name_ = name

  #############################################################################
  def getName(self):
    """retunrs the name"""
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
  def __repr__(self):
    """formats the output of the print command"""
    result = self.name_ + " " + self.value_ + "\n"
    return result

  #############################################################################
  def writeToXML(self):
    """creates an xml string"""
    result = '  <Parameter Name="' + str(self.getName()) + \
                     '" Value="' + str(self.getValue()) + '"/>\n'

    return result


  #############################################################################

