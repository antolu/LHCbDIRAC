# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file '../qt_resources/DataQualityDialog.ui'
#
# Created: Fri Sep  2 16:33:02 2011
#      by: PyQt4 UI code generator 4.7
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_DataQualityDialog(object):
    def setupUi(self, DataQualityDialog):
        DataQualityDialog.setObjectName("DataQualityDialog")
        DataQualityDialog.setWindowModality(QtCore.Qt.NonModal)
        DataQualityDialog.resize(204, 203)
        self.gridLayout = QtGui.QGridLayout(DataQualityDialog)
        self.gridLayout.setObjectName("gridLayout")
        self.OkButton = QtGui.QPushButton(DataQualityDialog)
        icon = QtGui.QIcon()
        icon.addPixmap(QtGui.QPixmap(":/icons/images/ok.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        self.OkButton.setIcon(icon)
        self.OkButton.setObjectName("OkButton")
        self.gridLayout.addWidget(self.OkButton, 1, 0, 1, 1)
        self.groupBox = QtGui.QGroupBox(DataQualityDialog)
        self.groupBox.setObjectName("groupBox")
        self.gridLayout_2 = QtGui.QGridLayout(self.groupBox)
        self.gridLayout_2.setObjectName("gridLayout_2")
        self.checkBox = QtGui.QCheckBox(self.groupBox)
        self.checkBox.setObjectName("checkBox")
        self.gridLayout_2.addWidget(self.checkBox, 1, 0, 1, 1)
        self.checkBox_2 = QtGui.QCheckBox(self.groupBox)
        self.checkBox_2.setObjectName("checkBox_2")
        self.gridLayout_2.addWidget(self.checkBox_2, 2, 0, 1, 1)
        self.checkBox_3 = QtGui.QCheckBox(self.groupBox)
        self.checkBox_3.setObjectName("checkBox_3")
        self.gridLayout_2.addWidget(self.checkBox_3, 3, 0, 1, 1)
        self.checkBox_4 = QtGui.QCheckBox(self.groupBox)
        self.checkBox_4.setObjectName("checkBox_4")
        self.gridLayout_2.addWidget(self.checkBox_4, 4, 0, 1, 1)
        self.gridLayout.addWidget(self.groupBox, 0, 0, 1, 1)

        self.retranslateUi(DataQualityDialog)
        QtCore.QMetaObject.connectSlotsByName(DataQualityDialog)

    def retranslateUi(self, DataQualityDialog):
        DataQualityDialog.setWindowTitle(QtGui.QApplication.translate("DataQualityDialog", "Data quality settings dialog", None, QtGui.QApplication.UnicodeUTF8))
        self.OkButton.setText(QtGui.QApplication.translate("DataQualityDialog", "OK", None, QtGui.QApplication.UnicodeUTF8))
        self.groupBox.setTitle(QtGui.QApplication.translate("DataQualityDialog", "Flags", None, QtGui.QApplication.UnicodeUTF8))
        self.checkBox.setText(QtGui.QApplication.translate("DataQualityDialog", "OK", None, QtGui.QApplication.UnicodeUTF8))
        self.checkBox_2.setText(QtGui.QApplication.translate("DataQualityDialog", "MAYBE", None, QtGui.QApplication.UnicodeUTF8))
        self.checkBox_3.setText(QtGui.QApplication.translate("DataQualityDialog", "BAD", None, QtGui.QApplication.UnicodeUTF8))
        self.checkBox_4.setText(QtGui.QApplication.translate("DataQualityDialog", "EXPRESS_OK", None, QtGui.QApplication.UnicodeUTF8))

import Resources_rc
