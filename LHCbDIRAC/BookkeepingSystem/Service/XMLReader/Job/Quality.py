###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
"""
reads the data quality
"""

__RCSID__ = "$Id$"

class Quality:
  """
  Quality class
  """
  #############################################################################
  def __init__(self):
    """initialize the class members"""
    self.group_ = ""
    self.flag_ = ""
    self.qualityID_ = -1
    self.params_ = []

  #############################################################################
  def setGroup(self, name):
    """sets the group"""
    self.group_ = name

  #############################################################################
  def getGroup(self):
    """returns the group"""
    return self.group_

  #############################################################################
  def setFlag(self, flag):
    """sets the data quality flag"""
    self.flag_ = flag

  #############################################################################
  def getFlag(self):
    """returns the data quality flag"""
    return self.flag_

  #############################################################################
  def addParam(self, param):
    """adds a param"""
    self.params_ += [param]

  #############################################################################
  def getParams(self):
    """returns the params"""
    return self.params_

  #############################################################################
  def setQualityID(self, qualityid):
    """sets the quality identifier"""
    self.qualityID_ = qualityid

  #############################################################################
  def getQualityID(self):
    """returns the quality identifier"""
    return self.qualityID_

  #############################################################################
  def __repr__(self):
    """formats the output of the print"""
    result = "Quality: "
    result += self.group_ + " " + self.flag_ + "\n"

    for param in self.params_:
      result += str(param)

    result += "\n"
    return result

  #############################################################################
  def writeToXML(self):
    """creates an XML string"""
    result = '<Quality Group="' + self.getGroup() + '" Flag="' + self.getFlag() + '"/>\n'
    return result


  #############################################################################

