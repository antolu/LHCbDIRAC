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
stores the replica readed from an xml
"""

from DIRAC import gLogger

__RCSID__ = "$Id$"


class Replica:
  """
  Replica class
  """

  #############################################################################
  def __init__(self):
    """initialize the class members"""
    self.params_ = []
    self.fileName_ = ""

  #############################################################################
  def addParam(self, param):
    """sets the parameters"""
    self.params_ += [param]

  #############################################################################
  def getaprams(self):
    """returns the list of parameters"""
    return self.params_

  #############################################################################
  def getFileName(self):
    """returns the file name"""
    return self.fileName_

  #############################################################################
  def setFileName(self, name):
    """sets the file name"""
    self.fileName_ = name

  #############################################################################
  def __repr__(self):
    """It idents the print output"""
    result = "\nReplica: "
    result += self.fileName_ + "\n"
    for param in self.params_:
      result += str(param)

    return result

  #############################################################################
  def writeToXML(self):
    """writs an XML file"""
    gLogger.debug("Replica XML writing!!!")
    result = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE Replicas SYSTEM "book.dtd">
<Replicas>
"""
    for param in self.getaprams():
      result += param.writeToXML(False)

    result += '</Replicas>'
    return result
  #############################################################################
