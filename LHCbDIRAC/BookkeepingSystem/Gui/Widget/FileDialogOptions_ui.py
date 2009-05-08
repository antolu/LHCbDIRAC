# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'workspace/DIRAC3/DIRAC/BookkeepingSystem/Gui/Widget/FileDialogOptions.ui'
#
# Created: Thu May  7 15:11:07 2009
#      by: PyQt4 UI code generator 4.3.3
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_FileDialogOptions(object):
    def setupUi(self, FileDialogOptions):
        FileDialogOptions.setObjectName("FileDialogOptions")
        FileDialogOptions.resize(QtCore.QSize(QtCore.QRect(0,0,193,115).size()).expandedTo(FileDialogOptions.minimumSizeHint()))

        self.gridlayout = QtGui.QGridLayout(FileDialogOptions)
        self.gridlayout.setObjectName("gridlayout")

        self.label = QtGui.QLabel(FileDialogOptions)
        self.label.setObjectName("label")
        self.gridlayout.addWidget(self.label,0,0,1,1)

        self.pageSize = QtGui.QLineEdit(FileDialogOptions)
        self.pageSize.setObjectName("pageSize")
        self.gridlayout.addWidget(self.pageSize,0,1,1,1)

        self.cancelButton = QtGui.QPushButton(FileDialogOptions)
        self.cancelButton.setObjectName("cancelButton")
        self.gridlayout.addWidget(self.cancelButton,1,0,1,1)

        self.okButton = QtGui.QPushButton(FileDialogOptions)
        self.okButton.setObjectName("okButton")
        self.gridlayout.addWidget(self.okButton,1,1,1,1)

        self.retranslateUi(FileDialogOptions)
        QtCore.QMetaObject.connectSlotsByName(FileDialogOptions)

    def retranslateUi(self, FileDialogOptions):
        FileDialogOptions.setWindowTitle(QtGui.QApplication.translate("FileDialogOptions", "Dialog", None, QtGui.QApplication.UnicodeUTF8))
        self.label.setText(QtGui.QApplication.translate("FileDialogOptions", "Page size", None, QtGui.QApplication.UnicodeUTF8))
        self.pageSize.setText(QtGui.QApplication.translate("FileDialogOptions", "ALL", None, QtGui.QApplication.UnicodeUTF8))
        self.cancelButton.setText(QtGui.QApplication.translate("FileDialogOptions", "Cancel", None, QtGui.QApplication.UnicodeUTF8))
        self.okButton.setText(QtGui.QApplication.translate("FileDialogOptions", "OK", None, QtGui.QApplication.UnicodeUTF8))

