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
stores the job parameters
"""

__RCSID__ = "$Id$"

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

