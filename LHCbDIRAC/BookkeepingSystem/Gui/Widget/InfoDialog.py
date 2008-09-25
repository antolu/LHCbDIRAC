########################################################################
# $Id: InfoDialog.py,v 1.1 2008/09/25 15:50:31 zmathe Exp $
########################################################################

from PyQt4.QtGui                                import *
from PyQt4.QtCore                               import *
from DIRAC.BookkeepingSystem.Gui.Widget.InfoDialog_ui           import Ui_Dialog
from DIRAC.BookkeepingSystem.Gui.Widget.TableModel              import TableModel
from DIRAC.BookkeepingSystem.Gui.Controler.ControlerInfoDialog  import ControlerInfoDialog

__RCSID__ = "$Id: InfoDialog.py,v 1.1 2008/09/25 15:50:31 zmathe Exp $"

#############################################################################  
class InfoDialog(QDialog, Ui_Dialog):
  
  #############################################################################  
  def __init__(self, parent = None):
    QDialog.__init__(self, parent)
    self.setupUi(self)
    self.__controler = ControlerInfoDialog(self, parent.getControler())
    self.connect(self.pushButton, SIGNAL("clicked()"), self.__controler.close)

  #############################################################################  
  def getControler(self):
    return self.__controler
  
  #############################################################################  
  def showData(self, data):
    noheader = ['name','expandable','level','fullpath']
    tabledata =[]
    header = ['Name','Value']

    for item in data.keys():
      if item not in noheader:
        if data[item] == None:
          d = ''
        else:
          d= data[item]
        tabledata += [ [item, d] ] 

    if len(tabledata) > 0:
      self.filltable(header, tabledata)
      return True
  
  #############################################################################  
  def filltable(self, header, tabledata):
      
    # set the table model
    tm = TableModel(tabledata, header, self) 
    self.tableView.setModel(tm)
  
    # set the minimum size
    self.setMinimumSize(400, 300)
  
    # hide grid
    self.tableView.setShowGrid(False)
  
    # set the font
    font = QFont("Courier New", 8)
    self.tableView.setFont(font)
    
    self.tableView.setSortingEnabled(True)
  
    # hide vertical header
    vh = self.tableView.verticalHeader()
    vh.setVisible(False)
  
    # set horizontal header properties
    hh = self.tableView.horizontalHeader()
    hh.setStretchLastSection(True)
  
    # set column width to fit contents
    self.tableView.resizeColumnsToContents()
  
    # set row height
    nrows = len(tabledata)
    for row in xrange(nrows):
        self.tableView.setRowHeight(row, 18)
  
    # enable sorting
    # this doesn't work
    #tv.setSortingEnabled(True)
  
  
  #############################################################################  