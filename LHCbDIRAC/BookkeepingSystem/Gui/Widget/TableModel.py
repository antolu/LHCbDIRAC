# pylint: skip-file

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

    result = None
    data = None
    if not index.isValid():
      result =  QVariant()
    elif role != Qt.DisplayRole:
      result =  QVariant()
    else:
      data = self.arraydata[index.row()][index.column()]
      if type(data) == datetime.datetime:
        result =  QVariant(str(data))
      else:
        result = QVariant(data)

    return result

  #############################################################################

  def headerData(self, col, orientation, role):
    """returns the header data"""
    try:
      if orientation == Qt.Horizontal and role == Qt.DisplayRole:
        return QVariant(self.headerdata[col])
      elif orientation == Qt.Vertical and role == Qt.DisplayRole:
        return QVariant(col + 1)
    except Exception, ex:
      print 'ERRRHHHH', ex
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
