# pylint: skip-file

# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'MainWidget.ui'
#
# Created: Fri Sep  7 13:28:34 2012
#      by: PyQt4 UI code generator 4.7
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_MainWidget(object):
  def setupUi(self, MainWidget):
    MainWidget.setObjectName("MainWidget")
    MainWidget.setWindowModality(QtCore.Qt.NonModal)
    MainWidget.setEnabled(True)
    MainWidget.resize(400, 500)
    MainWidget.setCursor(QtCore.Qt.ArrowCursor)
    self.centralwidget = QtGui.QWidget(MainWidget)
    self.centralwidget.setObjectName("centralwidget")
    self.vboxlayout = QtGui.QVBoxLayout(self.centralwidget)
    self.vboxlayout.setObjectName("vboxlayout")
    self.tree = TreeWidget(self.centralwidget)
    self.tree.setEnabled(True)
    self.tree.setObjectName("tree")
    self.vboxlayout.addWidget(self.tree)
    MainWidget.setCentralWidget(self.centralwidget)
    self.menubar = QtGui.QMenuBar(MainWidget)
    self.menubar.setGeometry(QtCore.QRect(0, 0, 400, 20))
    self.menubar.setObjectName("menubar")
    self.menuFile = QtGui.QMenu(self.menubar)
    self.menuFile.setObjectName("menuFile")
    self.menuSetings = QtGui.QMenu(self.menubar)
    self.menuSetings.setObjectName("menuSetings")
    MainWidget.setMenuBar(self.menubar)
    self.statusbar = QtGui.QStatusBar(MainWidget)
    self.statusbar.setObjectName("statusbar")
    MainWidget.setStatusBar(self.statusbar)
    self.actionExit = QtGui.QAction(MainWidget)
    self.actionExit.setObjectName("actionExit")
    self.actionFile_dialog_paging_size = QtGui.QAction(MainWidget)
    self.actionFile_dialog_paging_size.setObjectName("actionFile_dialog_paging_size")
    self.actionDataQuality = QtGui.QAction(MainWidget)
    self.actionDataQuality.setObjectName("actionDataQuality")
    self.menuFile.addAction(self.actionExit)
    self.menuSetings.addAction(self.actionDataQuality)
    self.menubar.addAction(self.menuFile.menuAction())
    self.menubar.addAction(self.menuSetings.menuAction())

    self.retranslateUi(MainWidget)
    QtCore.QMetaObject.connectSlotsByName(MainWidget)

  def retranslateUi(self, MainWidget):
    MainWidget.setWindowTitle(QtGui.QApplication.translate("MainWidget", "Feicim - LHCb Bookkeeping browser", None, QtGui.QApplication.UnicodeUTF8))
    self.tree.headerItem().setText(0, QtGui.QApplication.translate("MainWidget", "BookkeepingTree", None, QtGui.QApplication.UnicodeUTF8))
    self.tree.headerItem().setText(1, QtGui.QApplication.translate("MainWidget", "Description", None, QtGui.QApplication.UnicodeUTF8))
    self.menuFile.setTitle(QtGui.QApplication.translate("MainWidget", "File", None, QtGui.QApplication.UnicodeUTF8))
    self.menuSetings.setTitle(QtGui.QApplication.translate("MainWidget", "Settings", None, QtGui.QApplication.UnicodeUTF8))
    self.actionExit.setText(QtGui.QApplication.translate("MainWidget", "Exit", None, QtGui.QApplication.UnicodeUTF8))
    self.actionFile_dialog_paging_size.setText(QtGui.QApplication.translate("MainWidget", "File dialog page size", None, QtGui.QApplication.UnicodeUTF8))
    self.actionDataQuality.setText(QtGui.QApplication.translate("MainWidget", "DataQuality", None, QtGui.QApplication.UnicodeUTF8))

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.TreeWidget import TreeWidget
