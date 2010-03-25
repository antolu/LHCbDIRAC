# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'TableWidget.ui'
#
# Created: Thu Mar 25 12:04:19 2010
#      by: PyQt4 UI code generator 4.3.3
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_TableWidget(object):
    def setupUi(self, TableWidget):
        TableWidget.setObjectName("TableWidget")
        TableWidget.resize(QtCore.QSize(QtCore.QRect(0,0,764,694).size()).expandedTo(TableWidget.minimumSizeHint()))

        self.tableWidget = QtGui.QTableWidget(TableWidget)
        self.tableWidget.setGeometry(QtCore.QRect(10,10,741,671))
        self.tableWidget.setObjectName("tableWidget")

        self.retranslateUi(TableWidget)
        QtCore.QMetaObject.connectSlotsByName(TableWidget)

    def retranslateUi(self, TableWidget):
        TableWidget.setWindowTitle(QtGui.QApplication.translate("TableWidget", "TableWidget", None, QtGui.QApplication.UnicodeUTF8))
        self.tableWidget.clear()
        self.tableWidget.setColumnCount(0)
        self.tableWidget.setRowCount(0)

