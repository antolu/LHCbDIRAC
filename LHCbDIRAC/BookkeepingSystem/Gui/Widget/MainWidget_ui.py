# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'MainWidget.ui'
#
# Created: Tue Jun 23 09:31:38 2009
#      by: PyQt4 UI code generator 4.3.3
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.TreeWidget  import TreeWidget

class Ui_MainWidget(object):
    def setupUi(self, MainWidget):
        MainWidget.setObjectName("MainWidget")
        MainWidget.setWindowModality(QtCore.Qt.ApplicationModal)
        MainWidget.setEnabled(True)
        MainWidget.resize(QtCore.QSize(QtCore.QRect(0,0,400,500).size()).expandedTo(MainWidget.minimumSizeHint()))
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
        self.menubar.setGeometry(QtCore.QRect(0,0,400,25))
        self.menubar.setObjectName("menubar")

        self.menuFile = QtGui.QMenu(self.menubar)
        self.menuFile.setObjectName("menuFile")
        MainWidget.setMenuBar(self.menubar)

        self.statusbar = QtGui.QStatusBar(MainWidget)
        self.statusbar.setObjectName("statusbar")
        MainWidget.setStatusBar(self.statusbar)

        self.actionExit = QtGui.QAction(MainWidget)
        self.actionExit.setObjectName("actionExit")

        self.actionFile_dialog_paging_size = QtGui.QAction(MainWidget)
        self.actionFile_dialog_paging_size.setObjectName("actionFile_dialog_paging_size")
        self.menuFile.addAction(self.actionExit)
        self.menubar.addAction(self.menuFile.menuAction())

        self.retranslateUi(MainWidget)
        QtCore.QMetaObject.connectSlotsByName(MainWidget)

    def retranslateUi(self, MainWidget):
        MainWidget.setWindowTitle(QtGui.QApplication.translate("MainWidget", "Feicim - LHCb Bookkeeping browser", None, QtGui.QApplication.UnicodeUTF8))
        #self.tree.headerItem().setText(0,QtGui.QApplication.translate("MainWidget", "Jaj", None, QtGui.QApplication.UnicodeUTF8))
        #self.tree.headerItem().setText(1,QtGui.QApplication.translate("MainWidget", "New Column", None, QtGui.QApplication.UnicodeUTF8))
        self.menuFile.setTitle(QtGui.QApplication.translate("MainWidget", "File", None, QtGui.QApplication.UnicodeUTF8))
        self.actionExit.setText(QtGui.QApplication.translate("MainWidget", "Exit", None, QtGui.QApplication.UnicodeUTF8))
        self.actionFile_dialog_paging_size.setText(QtGui.QApplication.translate("MainWidget", "File dialog page size", None, QtGui.QApplication.UnicodeUTF8))

