# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'FileDialog.ui'
#
# Created: Tue Mar 31 14:14:57 2009
#      by: PyQt4 UI code generator 4.3.3
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_FileDialog(object):
    def setupUi(self, FileDialog):
        FileDialog.setObjectName("FileDialog")
        FileDialog.setWindowModality(QtCore.Qt.WindowModal)
        FileDialog.resize(QtCore.QSize(QtCore.QRect(0,0,1234,737).size()).expandedTo(FileDialog.minimumSizeHint()))
        FileDialog.setModal(False)

        self.gridlayout = QtGui.QGridLayout(FileDialog)
        self.gridlayout.setObjectName("gridlayout")

        self.tableView = QtGui.QTableView(FileDialog)
        self.tableView.setObjectName("tableView")
        self.gridlayout.addWidget(self.tableView,0,0,3,1)

        self.groupBox_3 = QtGui.QGroupBox(FileDialog)
        self.groupBox_3.setObjectName("groupBox_3")

        self.gridlayout1 = QtGui.QGridLayout(self.groupBox_3)
        self.gridlayout1.setObjectName("gridlayout1")

        self.gridlayout2 = QtGui.QGridLayout()
        self.gridlayout2.setObjectName("gridlayout2")

        self.configname = QtGui.QLineEdit(self.groupBox_3)
        self.configname.setReadOnly(True)
        self.configname.setObjectName("configname")
        self.gridlayout2.addWidget(self.configname,0,1,1,1)

        self.configversion = QtGui.QLineEdit(self.groupBox_3)
        self.configversion.setReadOnly(True)
        self.configversion.setObjectName("configversion")
        self.gridlayout2.addWidget(self.configversion,1,1,1,1)

        self.simulation = QtGui.QLineEdit(self.groupBox_3)
        self.simulation.setReadOnly(True)
        self.simulation.setObjectName("simulation")
        self.gridlayout2.addWidget(self.simulation,2,1,1,1)

        self.processing = QtGui.QLineEdit(self.groupBox_3)
        self.processing.setReadOnly(True)
        self.processing.setObjectName("processing")
        self.gridlayout2.addWidget(self.processing,3,1,1,1)

        self.eventtype = QtGui.QLineEdit(self.groupBox_3)
        self.eventtype.setReadOnly(True)
        self.eventtype.setObjectName("eventtype")
        self.gridlayout2.addWidget(self.eventtype,4,1,1,1)

        self.filetype = QtGui.QLineEdit(self.groupBox_3)
        self.filetype.setReadOnly(True)
        self.filetype.setObjectName("filetype")
        self.gridlayout2.addWidget(self.filetype,5,1,1,1)

        self.production = QtGui.QLineEdit(self.groupBox_3)
        self.production.setReadOnly(True)
        self.production.setObjectName("production")
        self.gridlayout2.addWidget(self.production,6,1,1,1)

        self.progrnameandversion = QtGui.QLineEdit(self.groupBox_3)
        self.progrnameandversion.setReadOnly(True)
        self.progrnameandversion.setObjectName("progrnameandversion")
        self.gridlayout2.addWidget(self.progrnameandversion,7,1,1,1)

        self.label_7 = QtGui.QLabel(self.groupBox_3)
        self.label_7.setObjectName("label_7")
        self.gridlayout2.addWidget(self.label_7,0,0,1,1)

        self.label_8 = QtGui.QLabel(self.groupBox_3)
        self.label_8.setObjectName("label_8")
        self.gridlayout2.addWidget(self.label_8,1,0,1,1)

        self.label_9 = QtGui.QLabel(self.groupBox_3)
        self.label_9.setObjectName("label_9")
        self.gridlayout2.addWidget(self.label_9,2,0,1,1)

        self.label_10 = QtGui.QLabel(self.groupBox_3)
        self.label_10.setObjectName("label_10")
        self.gridlayout2.addWidget(self.label_10,3,0,1,1)

        self.label_11 = QtGui.QLabel(self.groupBox_3)
        self.label_11.setObjectName("label_11")
        self.gridlayout2.addWidget(self.label_11,4,0,1,1)

        self.label_12 = QtGui.QLabel(self.groupBox_3)
        self.label_12.setObjectName("label_12")
        self.gridlayout2.addWidget(self.label_12,5,0,1,1)

        self.label_13 = QtGui.QLabel(self.groupBox_3)
        self.label_13.setObjectName("label_13")
        self.gridlayout2.addWidget(self.label_13,6,0,1,1)

        self.label_14 = QtGui.QLabel(self.groupBox_3)
        self.label_14.setObjectName("label_14")
        self.gridlayout2.addWidget(self.label_14,7,0,1,1)
        self.gridlayout1.addLayout(self.gridlayout2,0,0,1,1)
        self.gridlayout.addWidget(self.groupBox_3,0,1,1,3)

        self.groupBox = QtGui.QGroupBox(FileDialog)
        self.groupBox.setObjectName("groupBox")

        self.gridlayout3 = QtGui.QGridLayout(self.groupBox)
        self.gridlayout3.setObjectName("gridlayout3")

        self.gridlayout4 = QtGui.QGridLayout()
        self.gridlayout4.setObjectName("gridlayout4")

        self.hboxlayout = QtGui.QHBoxLayout()
        self.hboxlayout.setObjectName("hboxlayout")

        self.label_2 = QtGui.QLabel(self.groupBox)
        self.label_2.setObjectName("label_2")
        self.hboxlayout.addWidget(self.label_2)
        self.gridlayout4.addLayout(self.hboxlayout,1,0,1,1)

        self.label = QtGui.QLabel(self.groupBox)
        self.label.setObjectName("label")
        self.gridlayout4.addWidget(self.label,0,0,1,1)

        self.lineEdit = QtGui.QLineEdit(self.groupBox)
        self.lineEdit.setReadOnly(True)
        self.lineEdit.setObjectName("lineEdit")
        self.gridlayout4.addWidget(self.lineEdit,0,1,1,1)

        self.lineEdit_2 = QtGui.QLineEdit(self.groupBox)
        self.lineEdit_2.setReadOnly(True)
        self.lineEdit_2.setObjectName("lineEdit_2")
        self.gridlayout4.addWidget(self.lineEdit_2,1,1,1,1)

        self.label_5 = QtGui.QLabel(self.groupBox)
        self.label_5.setObjectName("label_5")
        self.gridlayout4.addWidget(self.label_5,2,0,1,1)

        self.lineEdit_5 = QtGui.QLineEdit(self.groupBox)
        self.lineEdit_5.setReadOnly(True)
        self.lineEdit_5.setObjectName("lineEdit_5")
        self.gridlayout4.addWidget(self.lineEdit_5,2,1,1,1)
        self.gridlayout3.addLayout(self.gridlayout4,0,0,1,1)
        self.gridlayout.addWidget(self.groupBox,1,1,1,3)

        self.groupBox_2 = QtGui.QGroupBox(FileDialog)
        self.groupBox_2.setObjectName("groupBox_2")

        self.gridlayout5 = QtGui.QGridLayout(self.groupBox_2)
        self.gridlayout5.setObjectName("gridlayout5")

        self.gridlayout6 = QtGui.QGridLayout()
        self.gridlayout6.setObjectName("gridlayout6")

        self.hboxlayout1 = QtGui.QHBoxLayout()
        self.hboxlayout1.setObjectName("hboxlayout1")

        self.label_4 = QtGui.QLabel(self.groupBox_2)
        self.label_4.setObjectName("label_4")
        self.hboxlayout1.addWidget(self.label_4)
        self.gridlayout6.addLayout(self.hboxlayout1,1,0,1,1)

        self.lineEdit_3 = QtGui.QLineEdit(self.groupBox_2)
        self.lineEdit_3.setReadOnly(True)
        self.lineEdit_3.setObjectName("lineEdit_3")
        self.gridlayout6.addWidget(self.lineEdit_3,0,1,1,1)

        self.lineEdit_4 = QtGui.QLineEdit(self.groupBox_2)
        self.lineEdit_4.setReadOnly(True)
        self.lineEdit_4.setObjectName("lineEdit_4")
        self.gridlayout6.addWidget(self.lineEdit_4,1,1,1,1)

        self.label_6 = QtGui.QLabel(self.groupBox_2)
        self.label_6.setObjectName("label_6")
        self.gridlayout6.addWidget(self.label_6,0,0,1,1)

        self.lineEdit_6 = QtGui.QLineEdit(self.groupBox_2)
        self.lineEdit_6.setReadOnly(True)
        self.lineEdit_6.setObjectName("lineEdit_6")
        self.gridlayout6.addWidget(self.lineEdit_6,2,1,1,1)

        self.label_3 = QtGui.QLabel(self.groupBox_2)
        self.label_3.setObjectName("label_3")
        self.gridlayout6.addWidget(self.label_3,2,0,1,1)
        self.gridlayout5.addLayout(self.gridlayout6,0,0,1,1)
        self.gridlayout.addWidget(self.groupBox_2,2,1,1,3)

        self.advancedSave = QtGui.QPushButton(FileDialog)
        self.advancedSave.setObjectName("advancedSave")
        self.gridlayout.addWidget(self.advancedSave,3,1,1,1)

        self.saveButton = QtGui.QPushButton(FileDialog)
        self.saveButton.setObjectName("saveButton")
        self.gridlayout.addWidget(self.saveButton,3,2,1,1)

        self.closeButton = QtGui.QPushButton(FileDialog)
        self.closeButton.setObjectName("closeButton")
        self.gridlayout.addWidget(self.closeButton,3,3,1,1)

        self.retranslateUi(FileDialog)
        QtCore.QMetaObject.connectSlotsByName(FileDialog)

    def retranslateUi(self, FileDialog):
        FileDialog.setWindowTitle(QtGui.QApplication.translate("FileDialog", "Feicim FileDialog", None, QtGui.QApplication.UnicodeUTF8))
        self.label_7.setText(QtGui.QApplication.translate("FileDialog", "Configuration Name", None, QtGui.QApplication.UnicodeUTF8))
        self.label_8.setText(QtGui.QApplication.translate("FileDialog", "Configuration Version", None, QtGui.QApplication.UnicodeUTF8))
        self.label_9.setText(QtGui.QApplication.translate("FileDialog", "Simulation Conditions", None, QtGui.QApplication.UnicodeUTF8))
        self.label_10.setText(QtGui.QApplication.translate("FileDialog", "Processing pass", None, QtGui.QApplication.UnicodeUTF8))
        self.label_11.setText(QtGui.QApplication.translate("FileDialog", "Event Type", None, QtGui.QApplication.UnicodeUTF8))
        self.label_12.setText(QtGui.QApplication.translate("FileDialog", "File Type", None, QtGui.QApplication.UnicodeUTF8))
        self.label_13.setText(QtGui.QApplication.translate("FileDialog", "Production", None, QtGui.QApplication.UnicodeUTF8))
        self.label_14.setText(QtGui.QApplication.translate("FileDialog", "Program Name and version", None, QtGui.QApplication.UnicodeUTF8))
        self.groupBox.setTitle(QtGui.QApplication.translate("FileDialog", "Statistics", None, QtGui.QApplication.UnicodeUTF8))
        self.label_2.setText(QtGui.QApplication.translate("FileDialog", "Number Of Events", None, QtGui.QApplication.UnicodeUTF8))
        self.label.setText(QtGui.QApplication.translate("FileDialog", "Number Of Files:", None, QtGui.QApplication.UnicodeUTF8))
        self.label_5.setText(QtGui.QApplication.translate("FileDialog", "Files size", None, QtGui.QApplication.UnicodeUTF8))
        self.groupBox_2.setTitle(QtGui.QApplication.translate("FileDialog", "Selected", None, QtGui.QApplication.UnicodeUTF8))
        self.label_4.setText(QtGui.QApplication.translate("FileDialog", "Number Of Events", None, QtGui.QApplication.UnicodeUTF8))
        self.label_6.setText(QtGui.QApplication.translate("FileDialog", "Number Of Files:", None, QtGui.QApplication.UnicodeUTF8))
        self.label_3.setText(QtGui.QApplication.translate("FileDialog", "Files size", None, QtGui.QApplication.UnicodeUTF8))
        self.advancedSave.setText(QtGui.QApplication.translate("FileDialog", "Advanced Save..", None, QtGui.QApplication.UnicodeUTF8))
        self.saveButton.setText(QtGui.QApplication.translate("FileDialog", "Save Files...", None, QtGui.QApplication.UnicodeUTF8))
        self.closeButton.setText(QtGui.QApplication.translate("FileDialog", "Close", None, QtGui.QApplication.UnicodeUTF8))

