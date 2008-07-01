########################################################################
# $Id: ReplicaParam.py,v 1.2 2008/07/01 10:54:26 zmathe Exp $
########################################################################

"""

"""

from DIRAC                                           import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: ReplicaParam.py,v 1.2 2008/07/01 10:54:26 zmathe Exp $"

class ReplicaParam:
    
    

  #############################################################################  
  def __init__(self):
    self.file_ = ""
    self.name_ = ""
    self.location_ = ""
    self.se_ = ""
    self.action_ = ""
      
  #############################################################################    
  def setFile(self, file):
    self.file_ = file

  #############################################################################    
  def getFile(self):
    return self.file_
  
  #############################################################################  
  def setName(self, name):
    self.name_ = name
      
  #############################################################################  
  def getName(self):
    return self.name_
  
  #############################################################################  
  def setLocation(self, location):
    self.location_ = location

  #############################################################################  
  def getLocation(self):
    return self.location_

  #############################################################################  
  def setSE(self, se):
    self.se_ = se

  #############################################################################  
  def getSE(self):
    return self.se_

  #############################################################################  
  def setAction(self, action):
    self.action_ = action
  
  #############################################################################  
  def getAction(self):
    return self.action_
  
  #############################################################################  
  def __repr__(self):
    result = "\n Replica:\n"
    result += self.file_ + " " + self.name_ + " " + self.location_ + " "
    result += self.se_ + " " + self.action_
      
    return result
  
  #############################################################################  
  def writeToXML(self, flag = True):
    # job replica param
    gLogger.info("replica param",str(flag))
    if flag == True:
      result = '     <Replica Name="'+self.getName()+'" Location="'+self.getLocation()+'"/>\n'
    
    else:
      result = ''
  
    return result
  
  #############################################################################  