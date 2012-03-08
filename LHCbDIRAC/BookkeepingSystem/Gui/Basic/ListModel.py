########################################################################
# $Id: $
########################################################################
from PyQt4.QtGui                                                          import *
from PyQt4.QtCore                                                         import *

import types

__RCSID__ = "$Id: $"

class ListModel(QAbstractListModel):
  #############################################################################
  def __init__(self, datain = None, parent=None, *args):
      QAbstractTableModel.__init__(self, parent, *args)
      self.__listdata = datain


  #############################################################################
  def rowCount(self, parent=QModelIndex()):
    return len(self.__listdata)

  #############################################################################
  def data(self, index, role):
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
    return self.__listdata

  #############################################################################
  def setData(self, data):
    self.__listdata = data
    self.reset()