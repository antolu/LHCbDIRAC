# pylint: skip-file

# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'ProductionLookup.ui'
#
# Created: Fri Sep  7 13:27:55 2012
#      by: PyQt4 UI code generator 4.7
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_ProductionLookup(object):
  def setupUi(self, ProductionLookup):
    ProductionLookup.setObjectName("ProductionLookup")
    ProductionLookup.resize(321, 230)
    self.gridlayout = QtGui.QGridLayout(ProductionLookup)
    self.gridlayout.setObjectName("gridlayout")
    self.lineEdit = QtGui.QLineEdit(ProductionLookup)
    self.lineEdit.setObjectName("lineEdit")
    self.gridlayout.addWidget(self.lineEdit, 0, 0, 1, 3)
    self.listView = QtGui.QListView(ProductionLookup)
    self.listView.setObjectName("listView")
    self.gridlayout.addWidget(self.listView, 1, 0, 1, 3)
    self.allButton = QtGui.QPushButton(ProductionLookup)
    icon = QtGui.QIcon()
    icon.addPixmap(QtGui.QPixmap(":/icons/images/files5.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.allButton.setIcon(icon)
    self.allButton.setObjectName("allButton")
    self.gridlayout.addWidget(self.allButton, 2, 0, 1, 1)
    self.pushButton = QtGui.QPushButton(ProductionLookup)
    icon1 = QtGui.QIcon()
    icon1.addPixmap(QtGui.QPixmap(":/icons/images/ok.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.pushButton.setIcon(icon1)
    self.pushButton.setObjectName("pushButton")
    self.gridlayout.addWidget(self.pushButton, 2, 1, 1, 1)
    self.pushButton_2 = QtGui.QPushButton(ProductionLookup)
    icon2 = QtGui.QIcon()
    icon2.addPixmap(QtGui.QPixmap(":/icons/images/stop.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.pushButton_2.setIcon(icon2)
    self.pushButton_2.setObjectName("pushButton_2")
    self.gridlayout.addWidget(self.pushButton_2, 2, 2, 1, 1)

    self.retranslateUi(ProductionLookup)
    QtCore.QMetaObject.connectSlotsByName(ProductionLookup)

  def retranslateUi(self, ProductionLookup):
    ProductionLookup.setWindowTitle(QtGui.QApplication.translate("ProductionLookup", "Dialog", None, QtGui.QApplication.UnicodeUTF8))
    self.allButton.setText(QtGui.QApplication.translate("ProductionLookup", "All", None, QtGui.QApplication.UnicodeUTF8))
    self.pushButton.setText(QtGui.QApplication.translate("ProductionLookup", "OK", None, QtGui.QApplication.UnicodeUTF8))
    self.pushButton_2.setText(QtGui.QApplication.translate("ProductionLookup", "Cancel", None, QtGui.QApplication.UnicodeUTF8))

import Resources_rc
