# pylint: skip-file

# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'InfoDialog.ui'
#
# Created: Fri Sep  7 13:28:49 2012
#      by: PyQt4 UI code generator 4.7
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_InfoDialog(object):
  def setupUi(self, InfoDialog):
    InfoDialog.setObjectName("InfoDialog")
    InfoDialog.setWindowModality(QtCore.Qt.WindowModal)
    InfoDialog.resize(664, 365)
    self.hboxlayout = QtGui.QHBoxLayout(InfoDialog)
    self.hboxlayout.setObjectName("hboxlayout")
    self.tableView = QtGui.QTableView(InfoDialog)
    self.tableView.setObjectName("tableView")
    self.hboxlayout.addWidget(self.tableView)
    self.groupBox = QtGui.QGroupBox(InfoDialog)
    self.groupBox.setObjectName("groupBox")
    self.gridlayout = QtGui.QGridLayout(self.groupBox)
    self.gridlayout.setObjectName("gridlayout")
    self.pushButton = QtGui.QPushButton(self.groupBox)
    icon = QtGui.QIcon()
    icon.addPixmap(QtGui.QPixmap(":/icons/images/close.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.pushButton.setIcon(icon)
    self.pushButton.setObjectName("pushButton")
    self.gridlayout.addWidget(self.pushButton, 0, 0, 1, 1)
    self.hboxlayout.addWidget(self.groupBox)

    self.retranslateUi(InfoDialog)
    QtCore.QMetaObject.connectSlotsByName(InfoDialog)

  def retranslateUi(self, InfoDialog):
    InfoDialog.setWindowTitle(QtGui.QApplication.translate("InfoDialog", "Feicim Info dialog", None, QtGui.QApplication.UnicodeUTF8))
    self.pushButton.setText(QtGui.QApplication.translate("InfoDialog", "Close", None, QtGui.QApplication.UnicodeUTF8))

import Resources_rc
