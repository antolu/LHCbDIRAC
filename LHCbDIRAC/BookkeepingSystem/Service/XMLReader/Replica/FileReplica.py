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
stores the file parameters
"""

from LHCbDIRAC.BookkeepingSystem.Service.XMLReader.Replica.Replica import Replica
from DIRAC import gLogger

__RCSID__ = "$Id$"


class FileReplica(Replica):
  """
  FileReplica class
  """

  def writeToXML(self):
    """creates an xml string"""
    gLogger.debug("Job Replica XML writing!!!")
    result = ''
    for param in self.getaprams():
      result += param.writeToXML()

    return result
