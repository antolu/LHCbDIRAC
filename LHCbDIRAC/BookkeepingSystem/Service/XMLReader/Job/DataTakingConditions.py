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
stores the data taking conditions

"""

from DIRAC                                                                  import gLogger

__RCSID__ = "$Id$"


class DataTakingConditions:
  """
  DataTakingConditions class
  """
  #############################################################################
  def __init__(self):
    """initialize the class memeber"""
    self.parameters_ = {}

  #############################################################################
  def addParam(self, name, value):
    """adds parameter"""
    self.parameters_[name] = value

  #############################################################################
  def getParams(self):
    """returns the parameters (data taking conditions)"""
    return self.parameters_

  #############################################################################
  def writeToXML(self):
    """creates an xml string"""
    gLogger.info("Write DataTaking conditions to XML!!")
    result = '<DataTakingConditions>\n'
    for name, value in self.getParams().items():
      result += '    <Parameter Name="' + name + '"   Value="' + value + '"/>\n'
    result += '</DataTakingConditions>\n'

    return result

