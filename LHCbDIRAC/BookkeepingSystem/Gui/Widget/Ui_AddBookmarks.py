# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file '../qt_resources/AddBookmarks.ui'
#
# Created: Fri Jun 29 15:57:53 2012
#      by: PyQt4 UI code generator snapshot-4.9-86ab82ddf2a6
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

try:
  _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
  _fromUtf8 = lambda s: s

class Ui_AddBookmarks(object):
  def setupUi(self, AddBookmarks):
    AddBookmarks.setObjectName(_fromUtf8("AddBookmarks"))
    AddBookmarks.resize(344, 167)
    self.gridLayout = QtGui.QGridLayout(AddBookmarks)
    self.gridLayout.setObjectName(_fromUtf8("gridLayout"))
    self.pathlineEdit = QtGui.QLineEdit(AddBookmarks)
    self.pathlineEdit.setObjectName(_fromUtf8("pathlineEdit"))
    self.gridLayout.addWidget(self.pathlineEdit, 4, 0, 1, 3)
    self.okButton = QtGui.QPushButton(AddBookmarks)
    icon = QtGui.QIcon()
    icon.addPixmap(QtGui.QPixmap(_fromUtf8(":/icons/images/ok.png")), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.okButton.setIcon(icon)
    self.okButton.setObjectName(_fromUtf8("okButton"))
    self.gridLayout.addWidget(self.okButton, 5, 1, 1, 1)
    self.cancelButton = QtGui.QPushButton(AddBookmarks)
    icon1 = QtGui.QIcon()
    icon1.addPixmap(QtGui.QPixmap(_fromUtf8(":/icons/images/stop.png")), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.cancelButton.setIcon(icon1)
    self.cancelButton.setObjectName(_fromUtf8("cancelButton"))
    self.gridLayout.addWidget(self.cancelButton, 5, 2, 1, 1)
    spacerItem = QtGui.QSpacerItem(40, 20, QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Minimum)
    self.gridLayout.addItem(spacerItem, 5, 0, 1, 1)
    self.label_2 = QtGui.QLabel(AddBookmarks)
    self.label_2.setObjectName(_fromUtf8("label_2"))
    self.gridLayout.addWidget(self.label_2, 2, 0, 1, 1)
    self.titlelineEdit = QtGui.QLineEdit(AddBookmarks)
    self.titlelineEdit.setObjectName(_fromUtf8("titlelineEdit"))
    self.gridLayout.addWidget(self.titlelineEdit, 1, 0, 1, 3)
    self.label = QtGui.QLabel(AddBookmarks)
    self.label.setObjectName(_fromUtf8("label"))
    self.gridLayout.addWidget(self.label, 0, 0, 1, 1)

    self.retranslateUi(AddBookmarks)
    QtCore.QMetaObject.connectSlotsByName(AddBookmarks)

  def retranslateUi(self, AddBookmarks):
    AddBookmarks.setWindowTitle(QtGui.QApplication.translate("AddBookmarks", "Dialog", None, QtGui.QApplication.UnicodeUTF8))
    self.okButton.setText(QtGui.QApplication.translate("AddBookmarks", "OK", None, QtGui.QApplication.UnicodeUTF8))
    self.cancelButton.setText(QtGui.QApplication.translate("AddBookmarks", "Cancel", None, QtGui.QApplication.UnicodeUTF8))
    self.label_2.setText(QtGui.QApplication.translate("AddBookmarks", "Path:", None, QtGui.QApplication.UnicodeUTF8))
    self.label.setText(QtGui.QApplication.translate("AddBookmarks", "Title:", None, QtGui.QApplication.UnicodeUTF8))

import Resources_rc
