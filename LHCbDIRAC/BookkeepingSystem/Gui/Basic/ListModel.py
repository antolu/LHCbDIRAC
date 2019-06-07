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
# pylint: skip-file

"""
List model used for all table widgets
"""

from PyQt4.QtCore import Qt, QVariant, QAbstractListModel, QModelIndex
from DIRAC import gLogger
__RCSID__ = "$Id$"

class ListModel(QAbstractListModel):
  """List Model class"""
  #############################################################################
  def __init__(self, datain=None, parent=None, *args):
    """initialize the class members"""
    QAbstractListModel.__init__(self, parent, *args)
    self.__listdata = datain


  #############################################################################
  def rowCount(self, parent=QModelIndex()):
    """counts the number of rows"""
    gLogger.debug(parent)
    return len(self.__listdata)

  #############################################################################
  def data(self, index, role):
    """returns the data"""
    if index.isValid() and role == Qt.DisplayRole:
      return QVariant(self.__listdata[index.row()])
    else:
      return QVariant()

  #############################################################################
  def setAllData(self, newdata):
    """ replace all data with new data """
    self.__listdata = newdata
    self.reset()

  #############################################################################
  def getAllData(self):
    """returns the data"""
    return self.__listdata

  #############################################################################
  def setData(self, data):
    """sets the data"""
    self.__listdata = data
    self.reset()
