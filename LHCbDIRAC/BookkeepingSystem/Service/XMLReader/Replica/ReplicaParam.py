"""stores the replica information"""
########################################################################
# $Id$
########################################################################

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
  def writeToXML(self, flag=True):
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

