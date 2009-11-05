########################################################################
# $Id$
########################################################################

"""

"""

__RCSID__ = "$Id$"

class JobConfiguration:
    

  #############################################################################  
  def __init__(self):
    self.configName_ = ""#None
    self.configVersion_ = ""# None
    self.date_ = "" #None
    self.time_ = ""#None

  #############################################################################  
  def setConfigName(self, name):
    self.configName_ = name

  #############################################################################  
  def getConfigName(self):
    return self.configName_
  
  #############################################################################  
  def setConfigVersion(self, version):
    self.configVersion_ = version
   
  #############################################################################  
  def getConfigVersion(self):
    return self.configVersion_
 
  #############################################################################  
  def setDate(self, date):
    self.date_ = date
      
  #############################################################################  
  def getDate(self):
    return self.date_
  
  #############################################################################  
  def setTime(self, time):
    self.time_ = time
      
  #############################################################################  
  def getTime(self):
    return self.time_
  
  #############################################################################  
  def __repr__(self):
    result = 'JobConfiguration: \n' 
    result += 'ConfigName:'+self.configName_ + '\n' 
    result += 'ConfigVersion:'+self.configVersion_ +'\n'
    result += 'Date and Time:'+self.date_ + ' ' + self.time_
    return result

  def writeToXML(self):
    result = '<Job ConfigName="'+self.getConfigName() + \
          '" ConfigVersion="'+self.getConfigVersion()+ \
          '" Date="'+ self.getDate() + \
          '" Time="'+ self.getTime()+'">\n'
    return result
  
  #############################################################################  