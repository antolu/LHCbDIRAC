###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
# pylint: skip-file

# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'AdvancedSave.ui'
#
# Created: Fri Sep  7 13:26:15 2012
#      by: PyQt4 UI code generator 4.7
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_AdvancedSave(object):
  def setupUi(self, AdvancedSave):
    AdvancedSave.setObjectName("AdvancedSave")
    AdvancedSave.resize(376, 186)
    self.gridlayout = QtGui.QGridLayout(AdvancedSave)
    self.gridlayout.setObjectName("gridlayout")
    self.label = QtGui.QLabel(AdvancedSave)
    self.label.setObjectName("label")
    self.gridlayout.addWidget(self.label, 0, 0, 1, 1)
    self.lineEdit = QtGui.QLineEdit(AdvancedSave)
    self.lineEdit.setObjectName("lineEdit")
    self.gridlayout.addWidget(self.lineEdit, 0, 1, 1, 1)
    self.groupBox = QtGui.QGroupBox(AdvancedSave)
    self.groupBox.setObjectName("groupBox")
    self.gridlayout1 = QtGui.QGridLayout(self.groupBox)
    self.gridlayout1.setObjectName("gridlayout1")
    self.pfnButton = QtGui.QRadioButton(self.groupBox)
    self.pfnButton.setObjectName("pfnButton")
    self.gridlayout1.addWidget(self.pfnButton, 0, 0, 1, 1)
    self.comboBox = QtGui.QComboBox(self.groupBox)
    self.comboBox.setObjectName("comboBox")
    self.gridlayout1.addWidget(self.comboBox, 0, 1, 2, 1)
    self.lfnButton = QtGui.QRadioButton(self.groupBox)
    self.lfnButton.setObjectName("lfnButton")
    self.gridlayout1.addWidget(self.lfnButton, 1, 0, 1, 1)
    self.gridlayout.addWidget(self.groupBox, 1, 0, 1, 2)
    self.saveButton = QtGui.QPushButton(AdvancedSave)
    icon = QtGui.QIcon()
    icon.addPixmap(QtGui.QPixmap(":/icons/images/save.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.saveButton.setIcon(icon)
    self.saveButton.setObjectName("saveButton")
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
