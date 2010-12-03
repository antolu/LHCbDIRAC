########################################################################
# $Id: HistoryDialog.py 18175 2009-11-11 14:02:19Z zmathe $
########################################################################

from PyQt4.QtGui                                import *
from PyQt4.QtCore                               import *

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.HistoryDialog_ui           import Ui_HistoryDialog
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.TableModel                 import TableModel
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerHistoryDialog  import ControlerHistoryDialog

import DIRAC,os

__RCSID__ = "$Id: HistoryDialog.py 18175 2009-11-11 14:02:19Z zmathe $"

#############################################################################  
class HistoryDialog(QDialog, Ui_HistoryDialog):
  
  #############################################################################  
  def __init__(self, parent = None):
    QDialog.__init__(self, parent)
    self.setupUi(self)
    self.__controler = ControlerHistoryDialog(self, parent.getControler())
    self.connect(self.nextButton, SIGNAL("clicked()"), self.__controler.next)
    self.connect(self.backButton, SIGNAL("clicked()"), self.__controler.back)
    self.connect(self.closeButton, SIGNAL("clicked()"), self.__controler.close)   
    
    self.__model = {}
    self.__tableModel = None
 
  #############################################################################  
  def getControler(self):
    return self.__controler
  
  #############################################################################  
  def setModel(self, model):
    self.__model = model
  
  def updateModel(self, model):
    self.__model.update(model)
    
  #############################################################################  
  def getModel(self):
    return self.__model
  
  #############################################################################
  def showError(self, message):
    QMessageBox.critical(self, "ERROR", message,QMessageBox.Ok)
  
  #############################################################################
  def getFilesTableView(self):
    return self.filesTableView
  
  #############################################################################
  def getJobTableView(self):
    return self.jobTableView
  
  #############################################################################
  def setTableModel(self, tableViewObject, tableModel):
    tableViewObject.setModel(tableModel) 
  
  #############################################################################
  def setNextButtonState(self, enable = True): 
    self.nextButton.setEnabled(enable)
  
  #############################################################################
  def setBackButtonSatate(self,enable = True): 
    self.backButton.setEnabled(enable)
    
  #############################################################################  
  def filltable(self, header, tabledata, tableViewObject):
      
    # set the table model
    tm = TableModel(tabledata, header, self) 
    
    tableViewObject.setModel(tm)
    tableViewObject.setSelectionBehavior(QAbstractItemView.SelectRows)
    tableViewObject.setSelectionMode(QAbstractItemView.SingleSelection) 
  
    tableViewObject.setAlternatingRowColors(True)

    sm = tableViewObject.selectionModel()
    self.connect(sm, SIGNAL("selectionChanged(QItemSelection, QItemSelection)"), self.__controler.selection)
    
    # set the minimum size
    self.setMinimumSize(400, 300)
  
    # hide grid
    tableViewObject.setShowGrid(True)
  
    # set the font
    #font = QFont("Courier New", 12)
    #tableViewObject.setFont(font)
  
    # hide vertical header
    vh = tableViewObject.verticalHeader()
    vh.setVisible(True)
  
    # set horizontal header properties
    hh = tableViewObject.horizontalHeader()
    hh.setStretchLastSection(True)
  
    # set column width to fit contents
    tableViewObject.resizeColumnsToContents()
    tableViewObject.setSortingEnabled(True)
  
    # set row height
    #nrows = len(tabledata)
    #for row in xrange(nrows):
     #   tableViewObject.setRowHeight(row, 18)
    # enable sorting
    # this doesn't work
    #tv.setSortingEnabled(True)
    return tm
  
  #############################################################################
  def clearTable(self):
    #tableViewObject().clear()
    self.__model = {}
  
  #############################################################################  
  def showError(self, message):
    QMessageBox.critical(self, "ERROR", message,QMessageBox.Ok)
  