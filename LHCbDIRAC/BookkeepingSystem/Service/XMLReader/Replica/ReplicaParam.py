"""stores the replica information"""

from DIRAC                                           import gLogger

__RCSID__ = "$Id$"

class ReplicaParam:
  """
  ReplicaParam class
  """


  #############################################################################
  def __init__(self):
    """initialize the member of the class"""
    self.file_ = ""
    self.name_ = ""
    self.location_ = ""
    self.se_ = ""
    self.action_ = ""

  #############################################################################
  def setFile(self, fileName):
    """sets the file name"""
    self.file_ = fileName

  #############################################################################
  def getFile(self):
    """returns the file name"""
    return self.file_

  #############################################################################
  def setName(self, name):
    """sets the name"""
    self.name_ = name

  #############################################################################
  def getName(self):
    """returns the name"""
    return self.name_

  #############################################################################
  def setLocation(self, location):
    """sets the location of the replica"""
    self.location_ = location

  #############################################################################
  def getLocation(self):
    """returns the location"""
    return self.location_

  #############################################################################
  def setSE(self, se):
    """sets the Storage Element"""
    self.se_ = se

  #############################################################################
  def getSE(self):
    """returns the storage element"""
    return self.se_

  #############################################################################
  def setAction(self, action):
    """sets the action (add/delete)"""
    self.action_ = action

  #############################################################################
  def getAction(self):
    """returns the action"""
    return self.action_

  #############################################################################
  def __repr__(self):
    """formats the output of print"""
    result = "\n Replica:\n"
    result += self.file_ + " " + self.name_ + " " + self.location_ + " "
    result += self.se_ + " " + self.action_

    return result

  #############################################################################
  def writeToXML(self, flag=True):
    """creates an XML string"""
    # job replica param
    gLogger.info("replica param", str(flag))
    if flag == True:
      result = '     <Replica Name="' + self.getName() + '" Location="' + self.getLocation() + '"/>\n'

    else:
      result = '<Replica File="' + self.getFile() + '"\n'
      result += '      Name="' + self.getName() + '"\n'
      result += '      Location="' + self.getLocation() + '"\n'
      result += '      SE="' + self.getSE() + '"/> \n'

    return result

  #############################################################################

