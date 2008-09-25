########################################################################
# $Id: TableModel.py,v 1.1 2008/09/25 15:50:31 zmathe Exp $
########################################################################

from PyQt4.QtGui                 import *
from PyQt4.QtCore                import *

import operator  

__RCSID__ = "$Id: TableModel.py,v 1.1 2008/09/25 15:50:31 zmathe Exp $"

#############################################################################  
class TableModel(QAbstractTableModel): 
  
  #############################################################################  
  def __init__(self, datain, headerdata, parent=None, *args): 
    QAbstractTableModel.__init__(self, parent, *args) 
    self.arraydata = datain
    self.headerdata = headerdata
 
  #############################################################################  
  def rowCount(self, parent): 
    return len(self.arraydata) 
  
  #############################################################################  
  def columnCount(self, parent): 
    return len(self.arraydata[0]) 
 
  #############################################################################  
  def data(self, index, role): 
    if not index.isValid(): 
        return QVariant() 
    elif role != Qt.DisplayRole: 
        return QVariant() 
    return QVariant(self.arraydata[index.row()][index.column()]) 
  
  #############################################################################  
  def headerData(self, col, orientation, role):
    if orientation == Qt.Horizontal and role == Qt.DisplayRole:
        return QVariant(self.headerdata[col])
    return QVariant()
  
  #############################################################################  
  def sort(self, Ncol, order):
    """Sort table by given column number.
    """
    self.emit(SIGNAL("layoutAboutToBeChanged()"))
    self.arraydata = sorted(self.arraydata, key=operator.itemgetter(Ncol))        
    if order == Qt.DescendingOrder:
        self.arraydata.reverse()
    self.emit(SIGNAL("layoutChanged()"))