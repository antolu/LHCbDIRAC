"""
Table model used most of the widget
"""
########################################################################
# $Id$
########################################################################

from PyQt4.QtCore                import Qt, SIGNAL, QAbstractTableModel, QVariant

import operator, datetime

__RCSID__ = "$Id$"

#############################################################################
class TableModel(QAbstractTableModel):
  """
  TableModel class
  """
  #############################################################################
  def __init__(self, datain, headerdata, parent=None, *args):
    QAbstractTableModel.__init__(self, parent, *args)
    self.arraydata = datain
    self.headerdata = headerdata

  #############################################################################
  def rowCount(self, parent):
    """number of rows"""
    return len(self.arraydata)

  #############################################################################
  def columnCount(self, parent):
    """number of collumns"""
    return len(self.arraydata[0])

  #############################################################################
  def data(self, index, role):
    """retuns an element of the table"""
    if not index.isValid():
      return QVariant()
    elif role != Qt.DisplayRole:
      return QVariant()

    data = self.arraydata[index.row()][index.column()]
    if type(data) == datetime.datetime:
      return QVariant(str(data))

    return QVariant(data)

  #############################################################################

  def headerData(self, col, orientation, role):
    """returns the header data"""
    if orientation == Qt.Horizontal and role == Qt.DisplayRole:
      return QVariant(self.headerdata[col])
    elif orientation == Qt.Vertical and role == Qt.DisplayRole:
      return QVariant(col + 1)
    return QVariant()

  #############################################################################
  def sort(self, ncol, order):
    """Sort table by given column number.
    """
    self.emit(SIGNAL("layoutAboutToBeChanged()"))
    self.arraydata = sorted(self.arraydata, key=operator.itemgetter(ncol))
    if order == Qt.DescendingOrder:
      self.arraydata.reverse()
    self.emit(SIGNAL("layoutChanged()"))
