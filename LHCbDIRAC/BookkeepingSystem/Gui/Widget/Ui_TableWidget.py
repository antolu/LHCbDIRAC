# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file '../qt_resources/TableWidget.ui'
#
# Created: Fri Jun 29 16:04:12 2012
#      by: PyQt4 UI code generator snapshot-4.9-86ab82ddf2a6
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

try:
  _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
  _fromUtf8 = lambda s: s

class Ui_TableWidget(object):
  def setupUi(self, TableWidget):
    TableWidget.setObjectName(_fromUtf8("TableWidget"))
    TableWidget.resize(764, 694)
    self.tableWidget = QtGui.QTableWidget(TableWidget)
    self.tableWidget.setGeometry(QtCore.QRect(10, 10, 741, 671))
    self.tableWidget.setObjectName(_fromUtf8("tableWidget"))
    self.tableWidget.setColumnCount(0)
    self.tableWidget.setRowCount(0)

    self.retranslateUi(TableWidget)
    QtCore.QMetaObject.connectSlotsByName(TableWidget)

  def retranslateUi(self, TableWidget):
    TableWidget.setWindowTitle(QtGui.QApplication.translate("TableWidget", "TableWidget", None, QtGui.QApplication.UnicodeUTF8))

