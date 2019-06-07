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

