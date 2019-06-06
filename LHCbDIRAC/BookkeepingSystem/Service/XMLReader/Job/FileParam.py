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
declare a file parameter
"""

__RCSID__ = "$Id$"

class FileParam:

  """
  FileParam class
  """
  #############################################################################
  def __init__(self):
    """initialize the class members"""
    self.name_ = ""
    self.value_ = ""

  #############################################################################
  def setParamName(self, name):
    """sets the file parameter"""
    self.name_ = name

  #############################################################################
  def getParamName(self):
    """returns the file parameter"""
    return self.name_

  #############################################################################
  def setParamValue(self, value):
    """sets the value of the parameter"""
    self.value_ = value

  #############################################################################
  def getParamValue(self):
    """returns the value of the parameter"""
    return self.value_

  #############################################################################
  def __repr__(self):
    """formats the output of print"""
    result = '\nFileParam: \n'
    result += self.name_ + ' ' + self.value_ + '\n'
    return result

  #############################################################################
  def writeToXML(self):
    """creates an xml string"""
    return '    <Parameter  Name="'+ self.getParamName() +'"     Value="'+self.getParamValue()+'"/>\n'

  #############################################################################