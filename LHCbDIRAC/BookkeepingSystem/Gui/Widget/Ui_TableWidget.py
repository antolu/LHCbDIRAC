# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'TableWidget.ui'
#
# Created: Thu Sep  1 17:42:06 2011
#      by: PyQt4 UI code generator 4.7
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_TableWidget(object):
    def setupUi(self, TableWidget):
        TableWidget.setObjectName("TableWidget")
        TableWidget.resize(764, 694)
        self.tableWidget = QtGui.QTableWidget(TableWidget)
        self.tableWidget.setGeometry(QtCore.QRect(10, 10, 741, 671))
        self.tableWidget.setObjectName("tableWidget")
        self.tableWidget.setColumnCount(0)
        self.tableWidget.setRowCount(0)

        self.retranslateUi(TableWidget)
        QtCore.QMetaObject.connectSlotsByName(TableWidget)

    def retranslateUi(self, TableWidget):
        TableWidget.setWindowTitle(QtGui.QApplication.translate("TableWidget", "TableWidget", None, QtGui.QApplication.UnicodeUTF8))

