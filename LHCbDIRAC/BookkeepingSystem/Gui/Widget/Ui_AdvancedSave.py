# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file '../qt_resources/AdvancedSave.ui'
#
# Created: Fri Jun 29 15:59:22 2012
#      by: PyQt4 UI code generator snapshot-4.9-86ab82ddf2a6
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

try:
  _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
  _fromUtf8 = lambda s: s

class Ui_AdvancedSave(object):
  def setupUi(self, AdvancedSave):
    AdvancedSave.setObjectName(_fromUtf8("AdvancedSave"))
    AdvancedSave.resize(376, 186)
    self.gridlayout = QtGui.QGridLayout(AdvancedSave)
    self.gridlayout.setObjectName(_fromUtf8("gridlayout"))
    self.label = QtGui.QLabel(AdvancedSave)
    self.label.setObjectName(_fromUtf8("label"))
    self.gridlayout.addWidget(self.label, 0, 0, 1, 1)
    self.lineEdit = QtGui.QLineEdit(AdvancedSave)
    self.lineEdit.setObjectName(_fromUtf8("lineEdit"))
    self.gridlayout.addWidget(self.lineEdit, 0, 1, 1, 1)
    self.groupBox = QtGui.QGroupBox(AdvancedSave)
    self.groupBox.setObjectName(_fromUtf8("groupBox"))
    self.gridlayout1 = QtGui.QGridLayout(self.groupBox)
    self.gridlayout1.setObjectName(_fromUtf8("gridlayout1"))
    self.pfnButton = QtGui.QRadioButton(self.groupBox)
    self.pfnButton.setObjectName(_fromUtf8("pfnButton"))
    self.gridlayout1.addWidget(self.pfnButton, 0, 0, 1, 1)
    self.comboBox = QtGui.QComboBox(self.groupBox)
    self.comboBox.setObjectName(_fromUtf8("comboBox"))
    self.gridlayout1.addWidget(self.comboBox, 0, 1, 2, 1)
    self.lfnButton = QtGui.QRadioButton(self.groupBox)
    self.lfnButton.setObjectName(_fromUtf8("lfnButton"))
    self.gridlayout1.addWidget(self.lfnButton, 1, 0, 1, 1)
    self.gridlayout.addWidget(self.groupBox, 1, 0, 1, 2)
    self.saveButton = QtGui.QPushButton(AdvancedSave)
    icon = QtGui.QIcon()
    icon.addPixmap(QtGui.QPixmap(_fromUtf8(":/icons/images/save.png")), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.saveButton.setIcon(icon)
    self.saveButton.setObjectName(_fromUtf8("saveButton"))
    self.gridlayout.addWidget(self.saveButton, 2, 1, 1, 1)

    self.retranslateUi(AdvancedSave)
    QtCore.QMetaObject.connectSlotsByName(AdvancedSave)

  def retranslateUi(self, AdvancedSave):
    AdvancedSave.setWindowTitle(QtGui.QApplication.translate("AdvancedSave", "Dialog", None, QtGui.QApplication.UnicodeUTF8))
    self.label.setText(QtGui.QApplication.translate("AdvancedSave", "FileName", None, QtGui.QApplication.UnicodeUTF8))
    self.groupBox.setTitle(QtGui.QApplication.translate("AdvancedSave", "GroupBox", None, QtGui.QApplication.UnicodeUTF8))
    self.pfnButton.setText(QtGui.QApplication.translate("AdvancedSave", "PFN(s)", None, QtGui.QApplication.UnicodeUTF8))
    self.lfnButton.setText(QtGui.QApplication.translate("AdvancedSave", "LFN(s)", None, QtGui.QApplication.UnicodeUTF8))
    self.saveButton.setText(QtGui.QApplication.translate("AdvancedSave", "Save", None, QtGui.QApplication.UnicodeUTF8))

import Resources_rc
