# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'AddBookmarks.ui'
#
# Created: Thu Mar 25 12:00:47 2010
#      by: PyQt4 UI code generator 4.3.3
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_AddBookmarks(object):
    def setupUi(self, AddBookmarks):
        AddBookmarks.setObjectName("AddBookmarks")
        AddBookmarks.resize(QtCore.QSize(QtCore.QRect(0,0,344,156).size()).expandedTo(AddBookmarks.minimumSizeHint()))

        self.gridlayout = QtGui.QGridLayout(AddBookmarks)
        self.gridlayout.setObjectName("gridlayout")

        self.pathlineEdit = QtGui.QLineEdit(AddBookmarks)
        self.pathlineEdit.setObjectName("pathlineEdit")
        self.gridlayout.addWidget(self.pathlineEdit,4,0,1,3)

        self.okButton = QtGui.QPushButton(AddBookmarks)
        self.okButton.setObjectName("okButton")
        self.gridlayout.addWidget(self.okButton,5,1,1,1)

        self.cancelButton = QtGui.QPushButton(AddBookmarks)
        self.cancelButton.setObjectName("cancelButton")
        self.gridlayout.addWidget(self.cancelButton,5,2,1,1)

        spacerItem = QtGui.QSpacerItem(40,20,QtGui.QSizePolicy.Expanding,QtGui.QSizePolicy.Minimum)
        self.gridlayout.addItem(spacerItem,5,0,1,1)

        self.label_2 = QtGui.QLabel(AddBookmarks)
        self.label_2.setObjectName("label_2")
        self.gridlayout.addWidget(self.label_2,2,0,1,1)

        self.titlelineEdit = QtGui.QLineEdit(AddBookmarks)
        self.titlelineEdit.setObjectName("titlelineEdit")
        self.gridlayout.addWidget(self.titlelineEdit,1,0,1,3)

        self.label = QtGui.QLabel(AddBookmarks)
        self.label.setObjectName("label")
        self.gridlayout.addWidget(self.label,0,0,1,1)

        self.retranslateUi(AddBookmarks)
        QtCore.QMetaObject.connectSlotsByName(AddBookmarks)

    def retranslateUi(self, AddBookmarks):
        AddBookmarks.setWindowTitle(QtGui.QApplication.translate("AddBookmarks", "Dialog", None, QtGui.QApplication.UnicodeUTF8))
        self.okButton.setText(QtGui.QApplication.translate("AddBookmarks", "OK", None, QtGui.QApplication.UnicodeUTF8))
        self.cancelButton.setText(QtGui.QApplication.translate("AddBookmarks", "Cancel", None, QtGui.QApplication.UnicodeUTF8))
        self.label_2.setText(QtGui.QApplication.translate("AddBookmarks", "Path:", None, QtGui.QApplication.UnicodeUTF8))
        self.label.setText(QtGui.QApplication.translate("AddBookmarks", "Title:", None, QtGui.QApplication.UnicodeUTF8))

