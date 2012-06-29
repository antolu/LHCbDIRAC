# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file '../qt_resources/InfoDialog.ui'
#
# Created: Fri Jun 29 16:02:33 2012
#      by: PyQt4 UI code generator snapshot-4.9-86ab82ddf2a6
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

try:
  _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
  _fromUtf8 = lambda s: s

class Ui_InfoDialog(object):
  def setupUi(self, InfoDialog):
    InfoDialog.setObjectName(_fromUtf8("InfoDialog"))
    InfoDialog.setWindowModality(QtCore.Qt.WindowModal)
    InfoDialog.resize(664, 365)
    self.hboxlayout = QtGui.QHBoxLayout(InfoDialog)
    self.hboxlayout.setObjectName(_fromUtf8("hboxlayout"))
    self.tableView = QtGui.QTableView(InfoDialog)
    self.tableView.setObjectName(_fromUtf8("tableView"))
    self.hboxlayout.addWidget(self.tableView)
    self.groupBox = QtGui.QGroupBox(InfoDialog)
    self.groupBox.setTitle(_fromUtf8(""))
    self.groupBox.setObjectName(_fromUtf8("groupBox"))
    self.gridlayout = QtGui.QGridLayout(self.groupBox)
    self.gridlayout.setObjectName(_fromUtf8("gridlayout"))
    self.pushButton = QtGui.QPushButton(self.groupBox)
    icon = QtGui.QIcon()
    icon.addPixmap(QtGui.QPixmap(_fromUtf8(":/icons/images/close.png")), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.pushButton.setIcon(icon)
    self.pushButton.setObjectName(_fromUtf8("pushButton"))
    self.gridlayout.addWidget(self.pushButton, 0, 0, 1, 1)
    self.hboxlayout.addWidget(self.groupBox)

    self.retranslateUi(InfoDialog)
    QtCore.QMetaObject.connectSlotsByName(InfoDialog)

  def retranslateUi(self, InfoDialog):
    InfoDialog.setWindowTitle(QtGui.QApplication.translate("InfoDialog", "Feicim Info dialog", None, QtGui.QApplication.UnicodeUTF8))
    self.pushButton.setText(QtGui.QApplication.translate("InfoDialog", "Close", None, QtGui.QApplication.UnicodeUTF8))

import Resources_rc
