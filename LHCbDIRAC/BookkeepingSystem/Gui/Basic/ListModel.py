# pylint: skip-file

"""
List model used for all table widgets
"""
########################################################################
# $Id: $
########################################################################
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
