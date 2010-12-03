# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'InfoDialog.ui'
#
# Created: Thu Mar 25 12:02:35 2010
#      by: PyQt4 UI code generator 4.3.3
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_Dialog(object):
    def setupUi(self, Dialog):
        Dialog.setObjectName("Dialog")
        Dialog.setWindowModality(QtCore.Qt.WindowModal)
        Dialog.resize(QtCore.QSize(QtCore.QRect(0,0,664,365).size()).expandedTo(Dialog.minimumSizeHint()))

        self.hboxlayout = QtGui.QHBoxLayout(Dialog)
        self.hboxlayout.setObjectName("hboxlayout")

        self.tableView = QtGui.QTableView(Dialog)
        self.tableView.setObjectName("tableView")
        self.hboxlayout.addWidget(self.tableView)

        self.groupBox = QtGui.QGroupBox(Dialog)
        self.groupBox.setObjectName("groupBox")

        self.gridlayout = QtGui.QGridLayout(self.groupBox)
        self.gridlayout.setObjectName("gridlayout")

        self.pushButton = QtGui.QPushButton(self.groupBox)
        self.pushButton.setObjectName("pushButton")
        self.gridlayout.addWidget(self.pushButton,0,0,1,1)
        self.hboxlayout.addWidget(self.groupBox)

        self.retranslateUi(Dialog)
        QtCore.QMetaObject.connectSlotsByName(Dialog)

    def retranslateUi(self, Dialog):
        Dialog.setWindowTitle(QtGui.QApplication.translate("Dialog", "Feicim Info dialog", None, QtGui.QApplication.UnicodeUTF8))
        self.pushButton.setText(QtGui.QApplication.translate("Dialog", "Close", None, QtGui.QApplication.UnicodeUTF8))

