# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'ProcessingPassDialog.ui'
#
# Created: Mon Feb  8 17:42:52 2010
#      by: PyQt4 UI code generator 4.3.3
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_ProcessingPassDialog(object):
    def setupUi(self, ProcessingPassDialog):
        ProcessingPassDialog.setObjectName("ProcessingPassDialog")
        ProcessingPassDialog.resize(QtCore.QSize(QtCore.QRect(0,0,813,342).size()).expandedTo(ProcessingPassDialog.minimumSizeHint()))

        palette = QtGui.QPalette()

        brush = QtGui.QBrush(QtGui.QColor(170,178,249))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Active,QtGui.QPalette.Button,brush)

        brush = QtGui.QBrush(QtGui.QColor(255,255,255))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Active,QtGui.QPalette.Base,brush)

        brush = QtGui.QBrush(QtGui.QColor(193,193,221))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Active,QtGui.QPalette.Window,brush)

        brush = QtGui.QBrush(QtGui.QColor(170,178,249))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Inactive,QtGui.QPalette.Button,brush)

        brush = QtGui.QBrush(QtGui.QColor(255,255,255))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Inactive,QtGui.QPalette.Base,brush)

        brush = QtGui.QBrush(QtGui.QColor(193,193,221))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Inactive,QtGui.QPalette.Window,brush)

        brush = QtGui.QBrush(QtGui.QColor(170,178,249))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Disabled,QtGui.QPalette.Button,brush)

        brush = QtGui.QBrush(QtGui.QColor(193,193,221))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Disabled,QtGui.QPalette.Base,brush)

        brush = QtGui.QBrush(QtGui.QColor(193,193,221))
        brush.setStyle(QtCore.Qt.SolidPattern)
        palette.setBrush(QtGui.QPalette.Disabled,QtGui.QPalette.Window,brush)
        ProcessingPassDialog.setPalette(palette)

        self.gridlayout = QtGui.QGridLayout(ProcessingPassDialog)
        self.gridlayout.setObjectName("gridlayout")

        self.widget = QtGui.QWidget(ProcessingPassDialog)
        self.widget.setObjectName("widget")

        self.gridlayout1 = QtGui.QGridLayout(self.widget)
        self.gridlayout1.setObjectName("gridlayout1")

        self.tabwidget = QtGui.QTabWidget(self.widget)
        self.tabwidget.setObjectName("tabwidget")

        self.tab = QtGui.QWidget()
        self.tab.setObjectName("tab")

        self.hboxlayout = QtGui.QHBoxLayout(self.tab)
        self.hboxlayout.setObjectName("hboxlayout")
        self.tabwidget.addTab(self.tab,"")
        self.gridlayout1.addWidget(self.tabwidget,0,0,1,1)
        self.gridlayout.addWidget(self.widget,0,0,1,1)

        self.groupBox = QtGui.QGroupBox(ProcessingPassDialog)
        self.groupBox.setObjectName("groupBox")

        self.hboxlayout1 = QtGui.QHBoxLayout(self.groupBox)
        self.hboxlayout1.setObjectName("hboxlayout1")

        self.label = QtGui.QLabel(self.groupBox)
        self.label.setObjectName("label")
        self.hboxlayout1.addWidget(self.label)

        self.lineEdit = QtGui.QLineEdit(self.groupBox)
        self.lineEdit.setReadOnly(True)
        self.lineEdit.setObjectName("lineEdit")
        self.hboxlayout1.addWidget(self.lineEdit)

        self.closeButton = QtGui.QPushButton(self.groupBox)
        self.closeButton.setObjectName("closeButton")
        self.hboxlayout1.addWidget(self.closeButton)
        self.gridlayout.addWidget(self.groupBox,1,0,1,1)

        self.retranslateUi(ProcessingPassDialog)
        self.tabwidget.setCurrentIndex(0)
        QtCore.QMetaObject.connectSlotsByName(ProcessingPassDialog)

    def retranslateUi(self, ProcessingPassDialog):
        ProcessingPassDialog.setWindowTitle(QtGui.QApplication.translate("ProcessingPassDialog", "Feicim - Processing Pass Viewer", None, QtGui.QApplication.UnicodeUTF8))
        self.tabwidget.setTabText(self.tabwidget.indexOf(self.tab), QtGui.QApplication.translate("ProcessingPassDialog", "PassGroup", None, QtGui.QApplication.UnicodeUTF8))
        self.groupBox.setTitle(QtGui.QApplication.translate("ProcessingPassDialog", "Description", None, QtGui.QApplication.UnicodeUTF8))
        self.label.setText(QtGui.QApplication.translate("ProcessingPassDialog", "Total Processing pass", None, QtGui.QApplication.UnicodeUTF8))
        self.closeButton.setText(QtGui.QApplication.translate("ProcessingPassDialog", "Close", None, QtGui.QApplication.UnicodeUTF8))

