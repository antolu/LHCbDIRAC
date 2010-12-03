# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'DataQualityDialog.ui'
#
# Created: Wed Jun  9 12:45:24 2010
#      by: PyQt4 UI code generator 4.3.3
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_DataQualityDialog(object):
    def setupUi(self, DataQualityDialog):
        DataQualityDialog.setObjectName("DataQualityDialog")
        DataQualityDialog.setWindowModality(QtCore.Qt.NonModal)
        DataQualityDialog.resize(QtCore.QSize(QtCore.QRect(0,0,204,200).size()).expandedTo(DataQualityDialog.minimumSizeHint()))

        self.gridlayout = QtGui.QGridLayout(DataQualityDialog)
        self.gridlayout.setObjectName("gridlayout")

        self.OkButton = QtGui.QPushButton(DataQualityDialog)
        self.OkButton.setObjectName("OkButton")
        self.gridlayout.addWidget(self.OkButton,1,0,1,1)

        self.groupBox = QtGui.QGroupBox(DataQualityDialog)
        self.groupBox.setObjectName("groupBox")

        self.gridlayout1 = QtGui.QGridLayout(self.groupBox)
        self.gridlayout1.setObjectName("gridlayout1")

        
        self.gridlayout.addWidget(self.groupBox,0,0,1,1)

        self.retranslateUi(DataQualityDialog)
        QtCore.QMetaObject.connectSlotsByName(DataQualityDialog)

    def retranslateUi(self, DataQualityDialog):
        DataQualityDialog.setWindowTitle(QtGui.QApplication.translate("DataQualityDialog", "Data quality settings dialog", None, QtGui.QApplication.UnicodeUTF8))
        #self.OkButton.setText(QtGui.QApplication.translate("DataQualityDialog", "OK", None, QtGui.QApplication.UnicodeUTF8))
        self.groupBox.setTitle(QtGui.QApplication.translate("DataQualityDialog", "Flags", None, QtGui.QApplication.UnicodeUTF8))
        #self.checkBox.setText(QtGui.QApplication.translate("DataQualityDialog", "OK", None, QtGui.QApplication.UnicodeUTF8))
        #self.checkBox_2.setText(QtGui.QApplication.translate("DataQualityDialog", "MAYBE", None, QtGui.QApplication.UnicodeUTF8))
        #self.checkBox_3.setText(QtGui.QApplication.translate("DataQualityDialog", "BAD", None, QtGui.QApplication.UnicodeUTF8))
        #self.checkBox_4.setText(QtGui.QApplication.translate("DataQualityDialog", "EXPRESS_OK", None, QtGui.QApplication.UnicodeUTF8))

