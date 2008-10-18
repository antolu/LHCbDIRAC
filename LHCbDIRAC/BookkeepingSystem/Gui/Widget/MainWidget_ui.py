# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'MainWidget.ui'
#
# Created: Fri Oct 10 17:38:19 2008
#      by: PyQt4 UI code generator 4.3.3
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

from DIRAC.BookkeepingSystem.Gui.Widget.TreeWidget import TreeWidget

class Ui_MainWidget(object):
    def setupUi(self, MainWidget):
        MainWidget.setObjectName("MainWidget")
        MainWidget.setWindowModality(QtCore.Qt.ApplicationModal)
        MainWidget.resize(QtCore.QSize(QtCore.QRect(0,0,444,841).size()).expandedTo(MainWidget.minimumSizeHint()))

        self.centralwidget = QtGui.QWidget(MainWidget)
        self.centralwidget.setObjectName("centralwidget")

        self.tree = TreeWidget(self.centralwidget)
        #self.tree.setGeometry(QtCore.QRect(20,20,256,192))
        self.tree.setObjectName("tree")
        MainWidget.setCentralWidget(self.centralwidget)

        self.menubar = QtGui.QMenuBar(MainWidget)
        self.menubar.setGeometry(QtCore.QRect(0,0,444,25))
        self.menubar.setObjectName("menubar")

        self.menuFile = QtGui.QMenu(self.menubar)
        self.menuFile.setObjectName("menuFile")
        MainWidget.setMenuBar(self.menubar)

        self.statusbar = QtGui.QStatusBar(MainWidget)
        self.statusbar.setObjectName("statusbar")
        MainWidget.setStatusBar(self.statusbar)

        self.actionExit = QtGui.QAction(MainWidget)
        self.actionExit.setObjectName("actionExit")
        self.menuFile.addAction(self.actionExit)
        self.menubar.addAction(self.menuFile.menuAction())

        self.retranslateUi(MainWidget)
        QtCore.QMetaObject.connectSlotsByName(MainWidget)

    def retranslateUi(self, MainWidget):
        MainWidget.setWindowTitle(QtGui.QApplication.translate("MainWidget", "Bookkeeping ", None, QtGui.QApplication.UnicodeUTF8))
        self.tree.headerItem().setText(0,QtGui.QApplication.translate("MainWidget", "Path", None, QtGui.QApplication.UnicodeUTF8))
        self.menuFile.setTitle(QtGui.QApplication.translate("MainWidget", "File", None, QtGui.QApplication.UnicodeUTF8))
        self.actionExit.setText(QtGui.QApplication.translate("MainWidget", "Exit", None, QtGui.QApplication.UnicodeUTF8))

