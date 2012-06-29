# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file '../qt_resources/DataQualityDialog.ui'
#
# Created: Fri Jun 29 16:00:24 2012
#      by: PyQt4 UI code generator snapshot-4.9-86ab82ddf2a6
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

try:
  _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
  _fromUtf8 = lambda s: s

class Ui_DataQualityDialog(object):
  def setupUi(self, DataQualityDialog):
    DataQualityDialog.setObjectName(_fromUtf8("DataQualityDialog"))
    DataQualityDialog.setWindowModality(QtCore.Qt.NonModal)
    DataQualityDialog.resize(204, 208)
    self.gridLayout = QtGui.QGridLayout(DataQualityDialog)
    self.gridLayout.setObjectName(_fromUtf8("gridLayout"))
    self.OkButton = QtGui.QPushButton(DataQualityDialog)
    icon = QtGui.QIcon()
    icon.addPixmap(QtGui.QPixmap(_fromUtf8(":/icons/images/ok.png")), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.OkButton.setIcon(icon)
    self.OkButton.setObjectName(_fromUtf8("OkButton"))
    self.gridLayout.addWidget(self.OkButton, 1, 0, 1, 1)
    self.groupBox = QtGui.QGroupBox(DataQualityDialog)
    self.groupBox.setObjectName(_fromUtf8("groupBox"))
    self.dataQualityLayout = QtGui.QGridLayout(self.groupBox)
    self.dataQualityLayout.setObjectName(_fromUtf8("dataQualityLayout"))
    self.gridLayout.addWidget(self.groupBox, 0, 0, 1, 1)

    self.retranslateUi(DataQualityDialog)
    QtCore.QMetaObject.connectSlotsByName(DataQualityDialog)

  def retranslateUi(self, DataQualityDialog):
    DataQualityDialog.setWindowTitle(QtGui.QApplication.translate("DataQualityDialog", "Data quality settings dialog", None, QtGui.QApplication.UnicodeUTF8))
    self.OkButton.setText(QtGui.QApplication.translate("DataQualityDialog", "OK", None, QtGui.QApplication.UnicodeUTF8))
    self.groupBox.setTitle(QtGui.QApplication.translate("DataQualityDialog", "Flags", None, QtGui.QApplication.UnicodeUTF8))

import Resources_rc
