########################################################################
# $Id: TableModel.py,v 1.2 2008/10/10 15:18:29 zmathe Exp $
########################################################################

from PyQt4.QtGui                 import *
from PyQt4.QtCore                import *

import operator, datetime  

__RCSID__ = "$Id: TableModel.py,v 1.2 2008/10/10 15:18:29 zmathe Exp $"

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
    
    data = self.arraydata[index.row()][index.column()]
    if type(data)== datetime.datetime:
      return QVariant(str(data)) 
    
    return QVariant(data) 
  
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