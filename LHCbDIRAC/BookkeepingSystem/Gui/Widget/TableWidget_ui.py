########################################################################
# $Id: TableWidget_ui.py,v 1.1 2008/09/25 15:50:31 zmathe Exp $
########################################################################

# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'TableWidget.ui'
#
# Created: Mon Aug  4 18:47:28 2008
#      by: PyQt4 UI code generator 4.2
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

__RCSID__ = "$Id: TableWidget_ui.py,v 1.1 2008/09/25 15:50:31 zmathe Exp $"

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

