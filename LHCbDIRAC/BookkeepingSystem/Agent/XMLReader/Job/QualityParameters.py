########################################################################
# $Id: QualityParameters.py 18161 2009-11-11 12:07:09Z acasajus $
########################################################################

"""

"""

__RCSID__ = "$Id: QualityParameters.py 18161 2009-11-11 12:07:09Z acasajus $"

class QualityParameters:
  
  #############################################################################
  def __init__(self):
    self.name_ = ""
    self.value_ = ""
    
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
  def __repr__(self):
    result = self.name_ + " " + self.value_ + "\n"
  
  #############################################################################
  def writeToXML(self):
     result = '  <Parameter Name="' + str(self.getName()) + \
                     '" Value="'+str(self.getValue())+'"/>\n'

     return result
   
  
  #############################################################################
