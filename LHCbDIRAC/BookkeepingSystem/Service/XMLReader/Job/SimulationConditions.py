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
stores the simulation condition
"""

from DIRAC                                                                  import gLogger

__RCSID__ = "$Id$"


class SimulationConditions:
  """
  SimulationConditions class
  """
  #############################################################################
  def __init__(self):
    """initialize the class member"""
    self.parameters_ = {}

  #############################################################################
  def addParam(self, name, value):
    """adds a parameter into the dictionary"""
    self.parameters_[name] = value

  #############################################################################
  def getParams(self):
    """returns the parameters"""
    return self.parameters_

  #############################################################################
  def writeToXML(self):
    """creates the xml string"""
    gLogger.info("Write Simulation conditions to XML!!")
    result = '<SimulationCondition>\n'
    for name, value in self.getParams().items():
      result += '    <Parameter Name="' + name + '"   Value="' + value + '"/>\n'
    result += '</SimulationCondition>\n'

    return result

