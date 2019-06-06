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
History widget
"""

from PyQt4.QtGui                                import QDialog, QMessageBox, QAbstractItemView
from PyQt4.QtCore                               import SIGNAL

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Ui_HistoryDialog           import Ui_HistoryDialog
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.TableModel                 import TableModel
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerHistoryDialog  import ControlerHistoryDialog


__RCSID__ = "$Id$"

#############################################################################
class HistoryDialog(QDialog, Ui_HistoryDialog):
  """
  HistoryDialog class
  """
  #############################################################################
  def __init__(self, parent=None):
    """initialize the widget"""
    QDialog.__init__(self, parent)
    Ui_HistoryDialog.__init__(self)
    self.setupUi(self)
    self.__controler = ControlerHistoryDialog(self, parent.getControler())
    self.connect(self.nextButton, SIGNAL("clicked()"), self.__controler.next)
    self.connect(self.backButton, SIGNAL("clicked()"), self.__controler.back)
    self.connect(self.closeButton, SIGNAL("clicked()"), self.__controler.close)

    self.__model = {}
    self.__tableModel = None

  #############################################################################
  def getControler(self):
    """returns the controller"""
    return self.__controler

  #############################################################################
  def setModel(self, model):
    """sets the model"""
    self.__model = model

  def updateModel(self, model):
    """changes the model"""
    self.__model.update(model)

  #############################################################################
  def getModel(self):
    """returns the model"""
    return self.__model

  #############################################################################
  def showError(self, message):
    """shows the error"""
    QMessageBox.critical(self, "ERROR", message, QMessageBox.Ok)

  #############################################################################
  def getFilesTableView(self):
    """returns the table widget"""
    return self.filesTableView

  #############################################################################
  def getJobTableView(self):
    """returns the job table view"""
    return self.jobTableView

  #############################################################################
  @staticmethod
  def setTableModel(tableViewObject, tableModel):
    """sets the model of the table"""
    tableViewObject.setModel(tableModel)

  #############################################################################
  def setNextButtonState(self, enable=True):
    """enables the next button"""
    self.nextButton.setEnabled(enable)

  #############################################################################
  def setBackButtonSatate(self, enable=True):
    """enables the back button"""
    self.backButton.setEnabled(enable)

  #############################################################################
  def filltable(self, header, tabledata, tableViewObject):
    """fills the table"""
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
    """clear the table"""
    #tableViewObject().clear()
    self.__model = {}

