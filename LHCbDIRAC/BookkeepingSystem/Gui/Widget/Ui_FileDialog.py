# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file '../qt_resources/FileDialog.ui'
#
# Created: Fri Sep  2 17:30:45 2011
#      by: PyQt4 UI code generator 4.7
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_FileDialog(object):
    def setupUi(self, FileDialog):
        FileDialog.setObjectName("FileDialog")
        FileDialog.setWindowModality(QtCore.Qt.ApplicationModal)
        FileDialog.resize(713, 958)
        FileDialog.setModal(False)
        self.gridLayout_2 = QtGui.QGridLayout(FileDialog)
        self.gridLayout_2.setObjectName("gridLayout_2")
        self.tableView = QtGui.QTableView(FileDialog)
        self.tableView.setObjectName("tableView")
        self.gridLayout_2.addWidget(self.tableView, 0, 0, 5, 1)
        self.groupBox_3 = QtGui.QGroupBox(FileDialog)
        self.groupBox_3.setObjectName("groupBox_3")
        self.gridlayout = QtGui.QGridLayout(self.groupBox_3)
        self.gridlayout.setObjectName("gridlayout")
        self.gridlayout1 = QtGui.QGridLayout()
        self.gridlayout1.setObjectName("gridlayout1")
        self.configname = QtGui.QLineEdit(self.groupBox_3)
        self.configname.setReadOnly(True)
        self.configname.setObjectName("configname")
        self.gridlayout1.addWidget(self.configname, 0, 1, 1, 1)
        self.configversion = QtGui.QLineEdit(self.groupBox_3)
        self.configversion.setReadOnly(True)
        self.configversion.setObjectName("configversion")
        self.gridlayout1.addWidget(self.configversion, 1, 1, 1, 1)
        self.simulation = QtGui.QLineEdit(self.groupBox_3)
        self.simulation.setReadOnly(True)
        self.simulation.setObjectName("simulation")
        self.gridlayout1.addWidget(self.simulation, 2, 1, 1, 1)
        self.processing = QtGui.QLineEdit(self.groupBox_3)
        self.processing.setReadOnly(True)
        self.processing.setObjectName("processing")
        self.gridlayout1.addWidget(self.processing, 3, 1, 1, 1)
        self.eventtype = QtGui.QLineEdit(self.groupBox_3)
        self.eventtype.setReadOnly(True)
        self.eventtype.setObjectName("eventtype")
        self.gridlayout1.addWidget(self.eventtype, 4, 1, 1, 1)
        self.filetype = QtGui.QLineEdit(self.groupBox_3)
        self.filetype.setReadOnly(True)
        self.filetype.setObjectName("filetype")
        self.gridlayout1.addWidget(self.filetype, 5, 1, 1, 1)
        self.production = QtGui.QLineEdit(self.groupBox_3)
        self.production.setReadOnly(True)
        self.production.setObjectName("production")
        self.gridlayout1.addWidget(self.production, 6, 1, 1, 1)
        self.progrnameandversion = QtGui.QLineEdit(self.groupBox_3)
        self.progrnameandversion.setReadOnly(True)
        self.progrnameandversion.setObjectName("progrnameandversion")
        self.gridlayout1.addWidget(self.progrnameandversion, 7, 1, 1, 1)
        self.label_7 = QtGui.QLabel(self.groupBox_3)
        self.label_7.setObjectName("label_7")
        self.gridlayout1.addWidget(self.label_7, 0, 0, 1, 1)
        self.label_8 = QtGui.QLabel(self.groupBox_3)
        self.label_8.setObjectName("label_8")
        self.gridlayout1.addWidget(self.label_8, 1, 0, 1, 1)
        self.label_9 = QtGui.QLabel(self.groupBox_3)
        self.label_9.setObjectName("label_9")
        self.gridlayout1.addWidget(self.label_9, 2, 0, 1, 1)
        self.label_10 = QtGui.QLabel(self.groupBox_3)
        self.label_10.setObjectName("label_10")
        self.gridlayout1.addWidget(self.label_10, 3, 0, 1, 1)
        self.label_11 = QtGui.QLabel(self.groupBox_3)
        self.label_11.setObjectName("label_11")
        self.gridlayout1.addWidget(self.label_11, 4, 0, 1, 1)
        self.label_12 = QtGui.QLabel(self.groupBox_3)
        self.label_12.setObjectName("label_12")
        self.gridlayout1.addWidget(self.label_12, 5, 0, 1, 1)
        self.label_13 = QtGui.QLabel(self.groupBox_3)
        self.label_13.setObjectName("label_13")
        self.gridlayout1.addWidget(self.label_13, 6, 0, 1, 1)
        self.label_14 = QtGui.QLabel(self.groupBox_3)
        self.label_14.setObjectName("label_14")
        self.gridlayout1.addWidget(self.label_14, 7, 0, 1, 1)
        self.gridlayout.addLayout(self.gridlayout1, 0, 0, 1, 1)
        self.gridLayout_2.addWidget(self.groupBox_3, 0, 1, 1, 3)
        self.groupBox = QtGui.QGroupBox(FileDialog)
        self.groupBox.setObjectName("groupBox")
        self.gridlayout2 = QtGui.QGridLayout(self.groupBox)
        self.gridlayout2.setObjectName("gridlayout2")
        self.gridlayout3 = QtGui.QGridLayout()
        self.gridlayout3.setObjectName("gridlayout3")
        self.hboxlayout = QtGui.QHBoxLayout()
        self.hboxlayout.setObjectName("hboxlayout")
        self.label_2 = QtGui.QLabel(self.groupBox)
        self.label_2.setObjectName("label_2")
        self.hboxlayout.addWidget(self.label_2)
        self.gridlayout3.addLayout(self.hboxlayout, 1, 0, 1, 1)
        self.label = QtGui.QLabel(self.groupBox)
        self.label.setObjectName("label")
        self.gridlayout3.addWidget(self.label, 0, 0, 1, 1)
        self.lineEdit_2 = QtGui.QLineEdit(self.groupBox)
        self.lineEdit_2.setReadOnly(True)
        self.lineEdit_2.setObjectName("lineEdit_2")
        self.gridlayout3.addWidget(self.lineEdit_2, 1, 1, 1, 1)
        self.label_5 = QtGui.QLabel(self.groupBox)
        self.label_5.setObjectName("label_5")
        self.gridlayout3.addWidget(self.label_5, 5, 0, 1, 1)
        self.lineEdit_5 = QtGui.QLineEdit(self.groupBox)
        self.lineEdit_5.setReadOnly(True)
        self.lineEdit_5.setObjectName("lineEdit_5")
        self.gridlayout3.addWidget(self.lineEdit_5, 5, 1, 1, 1)
        self.alleventinputstat = QtGui.QLineEdit(self.groupBox)
        self.alleventinputstat.setObjectName("alleventinputstat")
        self.gridlayout3.addWidget(self.alleventinputstat, 2, 1, 1, 1)
        self.label_15 = QtGui.QLabel(self.groupBox)
        self.label_15.setObjectName("label_15")
        self.gridlayout3.addWidget(self.label_15, 2, 0, 1, 1)
        self.alltotalluminosity = QtGui.QLineEdit(self.groupBox)
        self.alltotalluminosity.setObjectName("alltotalluminosity")
        self.gridlayout3.addWidget(self.alltotalluminosity, 3, 1, 1, 1)
        self.allluminosity = QtGui.QLineEdit(self.groupBox)
        self.allluminosity.setObjectName("allluminosity")
        self.gridlayout3.addWidget(self.allluminosity, 4, 1, 1, 1)
        self.label_17 = QtGui.QLabel(self.groupBox)
        self.label_17.setObjectName("label_17")
        self.gridlayout3.addWidget(self.label_17, 3, 0, 1, 1)
        self.label_18 = QtGui.QLabel(self.groupBox)
        self.label_18.setObjectName("label_18")
        self.gridlayout3.addWidget(self.label_18, 4, 0, 1, 1)
        self.lineEdit = QtGui.QLineEdit(self.groupBox)
        self.lineEdit.setReadOnly(True)
        self.lineEdit.setObjectName("lineEdit")
        self.gridlayout3.addWidget(self.lineEdit, 0, 1, 1, 1)
        self.gridlayout2.addLayout(self.gridlayout3, 0, 0, 1, 1)
        self.gridLayout_2.addWidget(self.groupBox, 1, 1, 2, 3)
        self.groupBox_2 = QtGui.QGroupBox(FileDialog)
        self.groupBox_2.setObjectName("groupBox_2")
        self.gridlayout4 = QtGui.QGridLayout(self.groupBox_2)
        self.gridlayout4.setObjectName("gridlayout4")
        self.gridlayout5 = QtGui.QGridLayout()
        self.gridlayout5.setObjectName("gridlayout5")
        self.hboxlayout1 = QtGui.QHBoxLayout()
        self.hboxlayout1.setObjectName("hboxlayout1")
        self.label_4 = QtGui.QLabel(self.groupBox_2)
        self.label_4.setObjectName("label_4")
        self.hboxlayout1.addWidget(self.label_4)
        self.gridlayout5.addLayout(self.hboxlayout1, 1, 0, 1, 1)
        self.lineEdit_3 = QtGui.QLineEdit(self.groupBox_2)
        self.lineEdit_3.setReadOnly(True)
        self.lineEdit_3.setObjectName("lineEdit_3")
        self.gridlayout5.addWidget(self.lineEdit_3, 0, 1, 1, 1)
        self.lineEdit_4 = QtGui.QLineEdit(self.groupBox_2)
        self.lineEdit_4.setReadOnly(True)
        self.lineEdit_4.setObjectName("lineEdit_4")
        self.gridlayout5.addWidget(self.lineEdit_4, 1, 1, 1, 1)
        self.label_6 = QtGui.QLabel(self.groupBox_2)
        self.label_6.setObjectName("label_6")
        self.gridlayout5.addWidget(self.label_6, 0, 0, 1, 1)
        self.lineEdit_6 = QtGui.QLineEdit(self.groupBox_2)
        self.lineEdit_6.setReadOnly(True)
        self.lineEdit_6.setObjectName("lineEdit_6")
        self.gridlayout5.addWidget(self.lineEdit_6, 5, 1, 1, 1)
        self.label_3 = QtGui.QLabel(self.groupBox_2)
        self.label_3.setObjectName("label_3")
        self.gridlayout5.addWidget(self.label_3, 5, 0, 1, 1)
        self.eventInputstat = QtGui.QLineEdit(self.groupBox_2)
        self.eventInputstat.setObjectName("eventInputstat")
        self.gridlayout5.addWidget(self.eventInputstat, 2, 1, 1, 1)
        self.label_16 = QtGui.QLabel(self.groupBox_2)
        self.label_16.setObjectName("label_16")
        self.gridlayout5.addWidget(self.label_16, 2, 0, 1, 1)
        self.totalluminosity = QtGui.QLineEdit(self.groupBox_2)
        self.totalluminosity.setObjectName("totalluminosity")
        self.gridlayout5.addWidget(self.totalluminosity, 3, 1, 1, 1)
        self.luminosity = QtGui.QLineEdit(self.groupBox_2)
        self.luminosity.setObjectName("luminosity")
        self.gridlayout5.addWidget(self.luminosity, 4, 1, 1, 1)
        self.label_19 = QtGui.QLabel(self.groupBox_2)
        self.label_19.setObjectName("label_19")
        self.gridlayout5.addWidget(self.label_19, 3, 0, 1, 1)
        self.label_20 = QtGui.QLabel(self.groupBox_2)
        self.label_20.setObjectName("label_20")
        self.gridlayout5.addWidget(self.label_20, 4, 0, 1, 1)
        self.gridlayout4.addLayout(self.gridlayout5, 0, 0, 1, 1)
        self.groupBox_4 = QtGui.QGroupBox(self.groupBox_2)
        self.groupBox_4.setObjectName("groupBox_4")
        self.gridLayout = QtGui.QGridLayout(self.groupBox_4)
        self.gridLayout.setObjectName("gridLayout")
        self.tckButton = QtGui.QPushButton(self.groupBox_4)
        icon = QtGui.QIcon()
        icon.addPixmap(QtGui.QPixmap(":/icons/images/filter.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        self.tckButton.setIcon(icon)
        self.tckButton.setObjectName("tckButton")
        self.gridLayout.addWidget(self.tckButton, 0, 0, 1, 1)
        self.tckcloseButton = QtGui.QToolButton(self.groupBox_4)
        icon1 = QtGui.QIcon()
        icon1.addPixmap(QtGui.QPixmap(":/icons/images/reloadpage.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        self.tckcloseButton.setIcon(icon1)
        self.tckcloseButton.setObjectName("tckcloseButton")
        self.gridLayout.addWidget(self.tckcloseButton, 0, 1, 1, 1)
        spacerItem = QtGui.QSpacerItem(40, 20, QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Minimum)
        self.gridLayout.addItem(spacerItem, 0, 2, 1, 1)
        self.tckcombo = QtGui.QComboBox(self.groupBox_4)
        self.tckcombo.setObjectName("tckcombo")
        self.gridLayout.addWidget(self.tckcombo, 0, 3, 1, 1)
        self.filterWidget = FilterWidget(self.groupBox_4)
        self.filterWidget.setObjectName("filterWidget")
        self.gridLayout.addWidget(self.filterWidget, 1, 0, 1, 4)
        self.gridlayout4.addWidget(self.groupBox_4, 1, 0, 1, 1)
        self.gridLayout_2.addWidget(self.groupBox_2, 3, 1, 2, 3)
        self.nextButton = QtGui.QPushButton(FileDialog)
        icon2 = QtGui.QIcon()
        icon2.addPixmap(QtGui.QPixmap(":/icons/images/files3.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        self.nextButton.setIcon(icon2)
        self.nextButton.setObjectName("nextButton")
        self.gridLayout_2.addWidget(self.nextButton, 5, 0, 1, 1)
        self.advancedSave = QtGui.QPushButton(FileDialog)
        icon3 = QtGui.QIcon()
        icon3.addPixmap(QtGui.QPixmap(":/icons/images/file-save-as-48x48.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        self.advancedSave.setIcon(icon3)
        self.advancedSave.setObjectName("advancedSave")
        self.gridLayout_2.addWidget(self.advancedSave, 5, 1, 1, 1)
        self.saveButton = QtGui.QPushButton(FileDialog)
        icon4 = QtGui.QIcon()
        icon4.addPixmap(QtGui.QPixmap(":/icons/images/save.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        self.saveButton.setIcon(icon4)
        self.saveButton.setObjectName("saveButton")
        self.gridLayout_2.addWidget(self.saveButton, 5, 2, 1, 1)
        self.closeButton = QtGui.QPushButton(FileDialog)
        icon5 = QtGui.QIcon()
        icon5.addPixmap(QtGui.QPixmap(":/icons/images/close.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
        self.closeButton.setIcon(icon5)
        self.closeButton.setObjectName("closeButton")
        self.gridLayout_2.addWidget(self.closeButton, 5, 3, 1, 1)

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
        self.label_15.setText(QtGui.QApplication.translate("FileDialog", "EventInputStat", None, QtGui.QApplication.UnicodeUTF8))
        self.label_17.setText(QtGui.QApplication.translate("FileDialog", "TotalLuminosity", None, QtGui.QApplication.UnicodeUTF8))
        self.label_18.setText(QtGui.QApplication.translate("FileDialog", "Luminosity", None, QtGui.QApplication.UnicodeUTF8))
        self.groupBox_2.setTitle(QtGui.QApplication.translate("FileDialog", "Selected", None, QtGui.QApplication.UnicodeUTF8))
        self.label_4.setText(QtGui.QApplication.translate("FileDialog", "Number Of Events", None, QtGui.QApplication.UnicodeUTF8))
        self.label_6.setText(QtGui.QApplication.translate("FileDialog", "Number Of Files:", None, QtGui.QApplication.UnicodeUTF8))
        self.label_3.setText(QtGui.QApplication.translate("FileDialog", "Files size", None, QtGui.QApplication.UnicodeUTF8))
        self.label_16.setText(QtGui.QApplication.translate("FileDialog", "EventInputStat", None, QtGui.QApplication.UnicodeUTF8))
        self.label_19.setText(QtGui.QApplication.translate("FileDialog", "TotalLuminosity", None, QtGui.QApplication.UnicodeUTF8))
        self.label_20.setText(QtGui.QApplication.translate("FileDialog", "Luminosity", None, QtGui.QApplication.UnicodeUTF8))
        self.groupBox_4.setTitle(QtGui.QApplication.translate("FileDialog", "Filter(s)", None, QtGui.QApplication.UnicodeUTF8))
        self.tckButton.setText(QtGui.QApplication.translate("FileDialog", "TCK", None, QtGui.QApplication.UnicodeUTF8))
        self.tckcloseButton.setText(QtGui.QApplication.translate("FileDialog", "...", None, QtGui.QApplication.UnicodeUTF8))
        self.nextButton.setText(QtGui.QApplication.translate("FileDialog", "Next Page", None, QtGui.QApplication.UnicodeUTF8))
        self.advancedSave.setText(QtGui.QApplication.translate("FileDialog", "Advanced Save..", None, QtGui.QApplication.UnicodeUTF8))
        self.saveButton.setText(QtGui.QApplication.translate("FileDialog", "Save Files...", None, QtGui.QApplication.UnicodeUTF8))
        self.closeButton.setText(QtGui.QApplication.translate("FileDialog", "Close", None, QtGui.QApplication.UnicodeUTF8))

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.FilterWidget import FilterWidget
import Resources_rc
