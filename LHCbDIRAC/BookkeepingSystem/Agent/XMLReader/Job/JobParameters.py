########################################################################
# $Id: JobParameters.py,v 1.3 2008/07/01 08:58:30 zmathe Exp $
########################################################################

"""

"""

__RCSID__ = "$Id: JobParameters.py,v 1.3 2008/07/01 08:58:30 zmathe Exp $"

class JobParameters:


  #############################################################################  
  def __init__(self):
    self.name_ = ""
    self.value_ = ""
    self.type_ = ""

  #############################################################################  
  def setName(self, name):
    self.name_ = name
      
  #############################################################################  
  def getName(self):
    return self.name_

  #############################################################################  
  def setValue(self, value):
    self.value_ = value

  #############################################################################  
  def getValue(self):
    return self.value_

  #############################################################################  
  def setType(self, type):
    self.type_ = type

  #############################################################################  
  def getType(self):
    return self.type_
  
  #############################################################################  
  def __repr__(self):
    result = self.name_ + '  ' + self.value_ + '  ' + self.type_ + '\n'
    return result
  
  def writeToXML(self):

    result = '  <TypedParameter Name="' + str(self.getName()) + \
                     '" Value="'+str(self.getValue())+'" Type="'+str(self.getType())+'"/>\n'
    return result
  #############################################################################  
