########################################################################
# $Id: FileDialog_ui.py,v 1.1 2008/09/25 15:50:31 zmathe Exp $
########################################################################

# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'workspace/BookkeepingSystem/Bookkeeping/Widget/FileDialog.ui'
#
# Created: Wed Sep 17 15:51:32 2008
#      by: PyQt4 UI code generator 4.3.3
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

__RCSID__ = "$Id: FileDialog_ui.py,v 1.1 2008/09/25 15:50:31 zmathe Exp $"

class Ui_FileDialog(object):
    def setupUi(self, FileDialog):
        FileDialog.setObjectName("FileDialog")
        FileDialog.setWindowModality(QtCore.Qt.NonModal)
        FileDialog.resize(QtCore.QSize(QtCore.QRect(0,0,1144,653).size()).expandedTo(FileDialog.minimumSizeHint()))
        FileDialog.setModal(False)

        self.closeButton = QtGui.QPushButton(FileDialog)
        self.closeButton.setGeometry(QtCore.QRect(990,580,91,31))
        self.closeButton.setObjectName("closeButton")

        self.tableView = QtGui.QTableView(FileDialog)
        self.tableView.setGeometry(QtCore.QRect(20,20,831,591))
        self.tableView.setObjectName("tableView")

        self.saveButton = QtGui.QPushButton(FileDialog)
        self.saveButton.setGeometry(QtCore.QRect(870,580,91,31))
        self.saveButton.setObjectName("saveButton")

        self.groupBox = QtGui.QGroupBox(FileDialog)
        self.groupBox.setGeometry(QtCore.QRect(860,20,281,181))
        self.groupBox.setObjectName("groupBox")

        self.gridLayout_2 = QtGui.QWidget(self.groupBox)
        self.gridLayout_2.setGeometry(QtCore.QRect(10,30,261,131))
        self.gridLayout_2.setObjectName("gridLayout_2")

        self.gridlayout = QtGui.QGridLayout(self.gridLayout_2)
        self.gridlayout.setObjectName("gridlayout")

        self.hboxlayout = QtGui.QHBoxLayout()
        self.hboxlayout.setObjectName("hboxlayout")

        self.label_2 = QtGui.QLabel(self.gridLayout_2)
        self.label_2.setObjectName("label_2")
        self.hboxlayout.addWidget(self.label_2)
        self.gridlayout.addLayout(self.hboxlayout,1,0,1,1)

        self.label = QtGui.QLabel(self.gridLayout_2)
        self.label.setObjectName("label")
        self.gridlayout.addWidget(self.label,0,0,1,1)

        self.lineEdit = QtGui.QLineEdit(self.gridLayout_2)
        self.lineEdit.setObjectName("lineEdit")
        self.gridlayout.addWidget(self.lineEdit,0,1,1,1)

        self.lineEdit_2 = QtGui.QLineEdit(self.gridLayout_2)
        self.lineEdit_2.setObjectName("lineEdit_2")
        self.gridlayout.addWidget(self.lineEdit_2,1,1,1,1)

        self.label_5 = QtGui.QLabel(self.gridLayout_2)
        self.label_5.setObjectName("label_5")
        self.gridlayout.addWidget(self.label_5,2,0,1,1)

        self.lineEdit_5 = QtGui.QLineEdit(self.gridLayout_2)
        self.lineEdit_5.setObjectName("lineEdit_5")
        self.gridlayout.addWidget(self.lineEdit_5,2,1,1,1)

        self.groupBox_2 = QtGui.QGroupBox(FileDialog)
        self.groupBox_2.setGeometry(QtCore.QRect(860,230,281,231))
        self.groupBox_2.setObjectName("groupBox_2")

        self.gridLayout = QtGui.QWidget(self.groupBox_2)
        self.gridLayout.setGeometry(QtCore.QRect(10,40,261,131))
        self.gridLayout.setObjectName("gridLayout")

        self.gridlayout1 = QtGui.QGridLayout(self.gridLayout)
        self.gridlayout1.setObjectName("gridlayout1")

        self.hboxlayout1 = QtGui.QHBoxLayout()
        self.hboxlayout1.setObjectName("hboxlayout1")

        self.label_4 = QtGui.QLabel(self.gridLayout)
        self.label_4.setObjectName("label_4")
        self.hboxlayout1.addWidget(self.label_4)
        self.gridlayout1.addLayout(self.hboxlayout1,1,0,1,1)

        self.lineEdit_3 = QtGui.QLineEdit(self.gridLayout)
        self.lineEdit_3.setObjectName("lineEdit_3")
        self.gridlayout1.addWidget(self.lineEdit_3,0,1,1,1)

        self.lineEdit_4 = QtGui.QLineEdit(self.gridLayout)
        self.lineEdit_4.setObjectName("lineEdit_4")
        self.gridlayout1.addWidget(self.lineEdit_4,1,1,1,1)

        self.label_6 = QtGui.QLabel(self.gridLayout)
        self.label_6.setObjectName("label_6")
        self.gridlayout1.addWidget(self.label_6,0,0,1,1)

        self.lineEdit_6 = QtGui.QLineEdit(self.gridLayout)
        self.lineEdit_6.setObjectName("lineEdit_6")
        self.gridlayout1.addWidget(self.lineEdit_6,2,1,1,1)

        self.label_3 = QtGui.QLabel(self.gridLayout)
        self.label_3.setObjectName("label_3")
        self.gridlayout1.addWidget(self.label_3,2,0,1,1)
        
        self.retranslateUi(FileDialog)
        QtCore.QMetaObject.connectSlotsByName(FileDialog)

    def retranslateUi(self, FileDialog):
        FileDialog.setWindowTitle(QtGui.QApplication.translate("FileDialog", "FileDialog", None, QtGui.QApplication.UnicodeUTF8))
        self.closeButton.setText(QtGui.QApplication.translate("FileDialog", "Close", None, QtGui.QApplication.UnicodeUTF8))
        self.saveButton.setText(QtGui.QApplication.translate("FileDialog", "Save Files...", None, QtGui.QApplication.UnicodeUTF8))
        self.groupBox.setTitle(QtGui.QApplication.translate("FileDialog", "Statistics", None, QtGui.QApplication.UnicodeUTF8))
        self.label_2.setText(QtGui.QApplication.translate("FileDialog", "Number Of Events", None, QtGui.QApplication.UnicodeUTF8))
        self.label.setText(QtGui.QApplication.translate("FileDialog", "Number Of Files:", None, QtGui.QApplication.UnicodeUTF8))
        self.label_5.setText(QtGui.QApplication.translate("FileDialog", "Files size", None, QtGui.QApplication.UnicodeUTF8))
        self.groupBox_2.setTitle(QtGui.QApplication.translate("FileDialog", "Selected", None, QtGui.QApplication.UnicodeUTF8))
        self.label_4.setText(QtGui.QApplication.translate("FileDialog", "Number Of Events", None, QtGui.QApplication.UnicodeUTF8))
        self.label_6.setText(QtGui.QApplication.translate("FileDialog", "Number Of Files:", None, QtGui.QApplication.UnicodeUTF8))
        self.label_3.setText(QtGui.QApplication.translate("FileDialog", "Files size", None, QtGui.QApplication.UnicodeUTF8))

