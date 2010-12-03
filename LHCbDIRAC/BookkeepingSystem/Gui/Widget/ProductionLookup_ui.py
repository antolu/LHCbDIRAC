# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'ProductionLookup.ui'
#
# Created: Thu Mar 25 12:03:41 2010
#      by: PyQt4 UI code generator 4.3.3
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_Production(object):
    def setupUi(self, Production):
        Production.setObjectName("Production")
        Production.resize(QtCore.QSize(QtCore.QRect(0,0,270,230).size()).expandedTo(Production.minimumSizeHint()))

        self.gridlayout = QtGui.QGridLayout(Production)
        self.gridlayout.setObjectName("gridlayout")

        self.lineEdit = QtGui.QLineEdit(Production)
        self.lineEdit.setObjectName("lineEdit")
        self.gridlayout.addWidget(self.lineEdit,0,0,1,3)

        self.listView = QtGui.QListView(Production)
        self.listView.setObjectName("listView")
        self.gridlayout.addWidget(self.listView,1,0,1,3)

        self.allButton = QtGui.QPushButton(Production)
        self.allButton.setObjectName("allButton")
        self.gridlayout.addWidget(self.allButton,2,0,1,1)

        self.pushButton = QtGui.QPushButton(Production)
        self.pushButton.setObjectName("pushButton")
        self.gridlayout.addWidget(self.pushButton,2,1,1,1)

        self.pushButton_2 = QtGui.QPushButton(Production)
        self.pushButton_2.setObjectName("pushButton_2")
        self.gridlayout.addWidget(self.pushButton_2,2,2,1,1)

        self.retranslateUi(Production)
        QtCore.QMetaObject.connectSlotsByName(Production)

    def retranslateUi(self, Production):
        Production.setWindowTitle(QtGui.QApplication.translate("Production", "Dialog", None, QtGui.QApplication.UnicodeUTF8))
        self.allButton.setText(QtGui.QApplication.translate("Production", "All", None, QtGui.QApplication.UnicodeUTF8))
        self.pushButton.setText(QtGui.QApplication.translate("Production", "OK", None, QtGui.QApplication.UnicodeUTF8))
        self.pushButton_2.setText(QtGui.QApplication.translate("Production", "Cancel", None, QtGui.QApplication.UnicodeUTF8))

