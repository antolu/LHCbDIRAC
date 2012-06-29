# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file '../qt_resources/BookmarksWidget.ui'
#
# Created: Fri Jun 29 15:59:54 2012
#      by: PyQt4 UI code generator snapshot-4.9-86ab82ddf2a6
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

try:
  _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
  _fromUtf8 = lambda s: s

class Ui_BookmarksWidget(object):
  def setupUi(self, BookmarksWidget):
    BookmarksWidget.setObjectName(_fromUtf8("BookmarksWidget"))
    BookmarksWidget.resize(508, 794)
    self.gridLayout_4 = QtGui.QGridLayout(BookmarksWidget)
    self.gridLayout_4.setObjectName(_fromUtf8("gridLayout_4"))
    self.groupBox_3 = QtGui.QGroupBox(BookmarksWidget)
    self.groupBox_3.setTitle(_fromUtf8(""))
    self.groupBox_3.setObjectName(_fromUtf8("groupBox_3"))
    self.gridLayout_3 = QtGui.QGridLayout(self.groupBox_3)
    self.gridLayout_3.setObjectName(_fromUtf8("gridLayout_3"))
    self.groupBox_2 = QtGui.QGroupBox(self.groupBox_3)
    self.groupBox_2.setTitle(_fromUtf8(""))
    self.groupBox_2.setObjectName(_fromUtf8("groupBox_2"))
    self.gridLayout_2 = QtGui.QGridLayout(self.groupBox_2)
    self.gridLayout_2.setObjectName(_fromUtf8("gridLayout_2"))
    self.lineEdit = QtGui.QLineEdit(self.groupBox_2)
    self.lineEdit.setEnabled(False)
    self.lineEdit.setObjectName(_fromUtf8("lineEdit"))
    self.gridLayout_2.addWidget(self.lineEdit, 1, 0, 1, 2)
    self.bookmarks = QtGui.QTableView(self.groupBox_2)
    self.bookmarks.setObjectName(_fromUtf8("bookmarks"))
    self.gridLayout_2.addWidget(self.bookmarks, 2, 0, 1, 1)
    self.gridLayout_3.addWidget(self.groupBox_2, 1, 1, 1, 1)
    self.groupBox = QtGui.QGroupBox(self.groupBox_3)
    self.groupBox.setTitle(_fromUtf8(""))
    self.groupBox.setObjectName(_fromUtf8("groupBox"))
    self.gridLayout = QtGui.QGridLayout(self.groupBox)
    self.gridLayout.setObjectName(_fromUtf8("gridLayout"))
    spacerItem = QtGui.QSpacerItem(40, 20, QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Minimum)
    self.gridLayout.addItem(spacerItem, 0, 0, 1, 1)
    self.removeButton = QtGui.QPushButton(self.groupBox)
    icon = QtGui.QIcon()
    icon.addPixmap(QtGui.QPixmap(_fromUtf8(":/icons/images/remove.png")), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.removeButton.setIcon(icon)
    self.removeButton.setObjectName(_fromUtf8("removeButton"))
    self.gridLayout.addWidget(self.removeButton, 0, 1, 1, 1)
    self.addButton = QtGui.QPushButton(self.groupBox)
    icon1 = QtGui.QIcon()
    icon1.addPixmap(QtGui.QPixmap(_fromUtf8(":/icons/images/add.png")), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.addButton.setIcon(icon1)
    self.addButton.setObjectName(_fromUtf8("addButton"))
    self.gridLayout.addWidget(self.addButton, 0, 2, 1, 1)
    self.gridLayout_3.addWidget(self.groupBox, 3, 0, 1, 2)
    self.gridLayout_4.addWidget(self.groupBox_3, 0, 0, 1, 1)

    self.retranslateUi(BookmarksWidget)
    QtCore.QMetaObject.connectSlotsByName(BookmarksWidget)

  def retranslateUi(self, BookmarksWidget):
    BookmarksWidget.setWindowTitle(QtGui.QApplication.translate("BookmarksWidget", "Form", None, QtGui.QApplication.UnicodeUTF8))
    self.removeButton.setText(QtGui.QApplication.translate("BookmarksWidget", "Remove", None, QtGui.QApplication.UnicodeUTF8))
    self.addButton.setText(QtGui.QApplication.translate("BookmarksWidget", "Add", None, QtGui.QApplication.UnicodeUTF8))

import Resources_rc
