# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'AdvancedSave.ui'
#
# Created: Tue Mar 31 15:45:11 2009
#      by: PyQt4 UI code generator 4.3.3
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_Dialog(object):
    def setupUi(self, Dialog):
        Dialog.setObjectName("Dialog")
        Dialog.resize(QtCore.QSize(QtCore.QRect(0,0,376,186).size()).expandedTo(Dialog.minimumSizeHint()))

        self.gridlayout = QtGui.QGridLayout(Dialog)
        self.gridlayout.setObjectName("gridlayout")

        self.label = QtGui.QLabel(Dialog)
        self.label.setObjectName("label")
        self.gridlayout.addWidget(self.label,0,0,1,1)

        self.lineEdit = QtGui.QLineEdit(Dialog)
        self.lineEdit.setObjectName("lineEdit")
        self.gridlayout.addWidget(self.lineEdit,0,1,1,1)

        self.groupBox = QtGui.QGroupBox(Dialog)
        self.groupBox.setObjectName("groupBox")

        self.gridlayout1 = QtGui.QGridLayout(self.groupBox)
        self.gridlayout1.setObjectName("gridlayout1")

        self.pfnButton = QtGui.QRadioButton(self.groupBox)
        self.pfnButton.setObjectName("pfnButton")
        self.gridlayout1.addWidget(self.pfnButton,0,0,1,1)

        self.comboBox = QtGui.QComboBox(self.groupBox)
        self.comboBox.setObjectName("comboBox")
        self.gridlayout1.addWidget(self.comboBox,0,1,2,1)

        self.lfnButton = QtGui.QRadioButton(self.groupBox)
        self.lfnButton.setObjectName("lfnButton")
        self.gridlayout1.addWidget(self.lfnButton,1,0,1,1)
        self.gridlayout.addWidget(self.groupBox,1,0,1,2)

        self.saveButton = QtGui.QPushButton(Dialog)
        self.saveButton.setObjectName("saveButton")
        self.gridlayout.addWidget(self.saveButton,2,1,1,1)

        self.retranslateUi(Dialog)
        QtCore.QMetaObject.connectSlotsByName(Dialog)

    def retranslateUi(self, Dialog):
        Dialog.setWindowTitle(QtGui.QApplication.translate("Dialog", "Dialog", None, QtGui.QApplication.UnicodeUTF8))
        self.label.setText(QtGui.QApplication.translate("Dialog", "FileName", None, QtGui.QApplication.UnicodeUTF8))
        self.groupBox.setTitle(QtGui.QApplication.translate("Dialog", "GroupBox", None, QtGui.QApplication.UnicodeUTF8))
        self.pfnButton.setText(QtGui.QApplication.translate("Dialog", "PFN(s)", None, QtGui.QApplication.UnicodeUTF8))
        self.lfnButton.setText(QtGui.QApplication.translate("Dialog", "LFN(s)", None, QtGui.QApplication.UnicodeUTF8))
        self.saveButton.setText(QtGui.QApplication.translate("Dialog", "Save", None, QtGui.QApplication.UnicodeUTF8))

