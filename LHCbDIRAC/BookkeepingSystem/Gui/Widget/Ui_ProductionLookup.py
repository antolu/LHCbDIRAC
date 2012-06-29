# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file '../qt_resources/ProductionLookup.ui'
#
# Created: Fri Jun 29 16:03:45 2012
#      by: PyQt4 UI code generator snapshot-4.9-86ab82ddf2a6
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

try:
  _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
  _fromUtf8 = lambda s: s

class Ui_ProductionLookup(object):
  def setupUi(self, ProductionLookup):
    ProductionLookup.setObjectName(_fromUtf8("ProductionLookup"))
    ProductionLookup.resize(321, 230)
    self.gridlayout = QtGui.QGridLayout(ProductionLookup)
    self.gridlayout.setObjectName(_fromUtf8("gridlayout"))
    self.lineEdit = QtGui.QLineEdit(ProductionLookup)
    self.lineEdit.setObjectName(_fromUtf8("lineEdit"))
    self.gridlayout.addWidget(self.lineEdit, 0, 0, 1, 3)
    self.listView = QtGui.QListView(ProductionLookup)
    self.listView.setObjectName(_fromUtf8("listView"))
    self.gridlayout.addWidget(self.listView, 1, 0, 1, 3)
    self.allButton = QtGui.QPushButton(ProductionLookup)
    icon = QtGui.QIcon()
    icon.addPixmap(QtGui.QPixmap(_fromUtf8(":/icons/images/files5.png")), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.allButton.setIcon(icon)
    self.allButton.setObjectName(_fromUtf8("allButton"))
    self.gridlayout.addWidget(self.allButton, 2, 0, 1, 1)
    self.pushButton = QtGui.QPushButton(ProductionLookup)
    icon1 = QtGui.QIcon()
    icon1.addPixmap(QtGui.QPixmap(_fromUtf8(":/icons/images/ok.png")), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.pushButton.setIcon(icon1)
    self.pushButton.setObjectName(_fromUtf8("pushButton"))
    self.gridlayout.addWidget(self.pushButton, 2, 1, 1, 1)
    self.pushButton_2 = QtGui.QPushButton(ProductionLookup)
    icon2 = QtGui.QIcon()
    icon2.addPixmap(QtGui.QPixmap(_fromUtf8(":/icons/images/stop.png")), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.pushButton_2.setIcon(icon2)
    self.pushButton_2.setObjectName(_fromUtf8("pushButton_2"))
    self.gridlayout.addWidget(self.pushButton_2, 2, 2, 1, 1)

    self.retranslateUi(ProductionLookup)
    QtCore.QMetaObject.connectSlotsByName(ProductionLookup)

  def retranslateUi(self, ProductionLookup):
    ProductionLookup.setWindowTitle(QtGui.QApplication.translate("ProductionLookup", "Dialog", None, QtGui.QApplication.UnicodeUTF8))
    self.allButton.setText(QtGui.QApplication.translate("ProductionLookup", "All", None, QtGui.QApplication.UnicodeUTF8))
    self.pushButton.setText(QtGui.QApplication.translate("ProductionLookup", "OK", None, QtGui.QApplication.UnicodeUTF8))
    self.pushButton_2.setText(QtGui.QApplication.translate("ProductionLookup", "Cancel", None, QtGui.QApplication.UnicodeUTF8))

import Resources_rc
