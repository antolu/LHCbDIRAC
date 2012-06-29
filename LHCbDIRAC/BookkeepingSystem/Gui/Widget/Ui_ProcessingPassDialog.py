# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file '../qt_resources/ProcessingPassDialog.ui'
#
# Created: Fri Jun 29 16:03:20 2012
#      by: PyQt4 UI code generator snapshot-4.9-86ab82ddf2a6
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

try:
  _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
  _fromUtf8 = lambda s: s

class Ui_ProcessingPassDialog(object):
  def setupUi(self, ProcessingPassDialog):
    ProcessingPassDialog.setObjectName(_fromUtf8("ProcessingPassDialog"))
    ProcessingPassDialog.resize(813, 342)
    self.gridlayout = QtGui.QGridLayout(ProcessingPassDialog)
    self.gridlayout.setObjectName(_fromUtf8("gridlayout"))
    self.widget = QtGui.QWidget(ProcessingPassDialog)
    self.widget.setObjectName(_fromUtf8("widget"))
    self.gridlayout1 = QtGui.QGridLayout(self.widget)
    self.gridlayout1.setMargin(0)
    self.gridlayout1.setObjectName(_fromUtf8("gridlayout1"))
    self.tabwidget = QtGui.QTabWidget(self.widget)
    self.tabwidget.setObjectName(_fromUtf8("tabwidget"))
    self.tab = QtGui.QWidget()
    self.tab.setObjectName(_fromUtf8("tab"))
    self.hboxlayout = QtGui.QHBoxLayout(self.tab)
    self.hboxlayout.setObjectName(_fromUtf8("hboxlayout"))
    self.tabwidget.addTab(self.tab, _fromUtf8(""))
    self.gridlayout1.addWidget(self.tabwidget, 0, 0, 1, 1)
    self.gridlayout.addWidget(self.widget, 0, 0, 1, 1)
    self.groupBox = QtGui.QGroupBox(ProcessingPassDialog)
    self.groupBox.setObjectName(_fromUtf8("groupBox"))
    self.hboxlayout1 = QtGui.QHBoxLayout(self.groupBox)
    self.hboxlayout1.setObjectName(_fromUtf8("hboxlayout1"))
    self.label = QtGui.QLabel(self.groupBox)
    self.label.setObjectName(_fromUtf8("label"))
    self.hboxlayout1.addWidget(self.label)
    self.lineEdit = QtGui.QLineEdit(self.groupBox)
    self.lineEdit.setReadOnly(True)
    self.lineEdit.setObjectName(_fromUtf8("lineEdit"))
    self.hboxlayout1.addWidget(self.lineEdit)
    self.closeButton = QtGui.QPushButton(self.groupBox)
    icon = QtGui.QIcon()
    icon.addPixmap(QtGui.QPixmap(_fromUtf8(":/icons/images/close.png")), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.closeButton.setIcon(icon)
    self.closeButton.setObjectName(_fromUtf8("closeButton"))
    self.hboxlayout1.addWidget(self.closeButton)
    self.gridlayout.addWidget(self.groupBox, 1, 0, 1, 1)

    self.retranslateUi(ProcessingPassDialog)
    self.tabwidget.setCurrentIndex(0)
    QtCore.QMetaObject.connectSlotsByName(ProcessingPassDialog)

  def retranslateUi(self, ProcessingPassDialog):
    ProcessingPassDialog.setWindowTitle(QtGui.QApplication.translate("ProcessingPassDialog", "Feicim - Processing Pass Viewer", None, QtGui.QApplication.UnicodeUTF8))
    self.tabwidget.setTabText(self.tabwidget.indexOf(self.tab), QtGui.QApplication.translate("ProcessingPassDialog", "PassGroup", None, QtGui.QApplication.UnicodeUTF8))
    self.groupBox.setTitle(QtGui.QApplication.translate("ProcessingPassDialog", "Description", None, QtGui.QApplication.UnicodeUTF8))
    self.label.setText(QtGui.QApplication.translate("ProcessingPassDialog", "Total Processing pass", None, QtGui.QApplication.UnicodeUTF8))
    self.closeButton.setText(QtGui.QApplication.translate("ProcessingPassDialog", "Close", None, QtGui.QApplication.UnicodeUTF8))

import Resources_rc
