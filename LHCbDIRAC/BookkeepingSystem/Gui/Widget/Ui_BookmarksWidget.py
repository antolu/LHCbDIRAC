# pylint: skip-file

# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'BookmarksWidget.ui'
#
# Created: Fri Sep  7 13:25:55 2012
#      by: PyQt4 UI code generator 4.7
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_BookmarksWidget(object):
  def setupUi(self, BookmarksWidget):
    BookmarksWidget.setObjectName("BookmarksWidget")
    BookmarksWidget.resize(508, 794)
    self.gridLayout_4 = QtGui.QGridLayout(BookmarksWidget)
    self.gridLayout_4.setObjectName("gridLayout_4")
    self.groupBox_3 = QtGui.QGroupBox(BookmarksWidget)
    self.groupBox_3.setObjectName("groupBox_3")
    self.gridLayout_3 = QtGui.QGridLayout(self.groupBox_3)
    self.gridLayout_3.setObjectName("gridLayout_3")
    self.groupBox_2 = QtGui.QGroupBox(self.groupBox_3)
    self.groupBox_2.setObjectName("groupBox_2")
    self.gridLayout_2 = QtGui.QGridLayout(self.groupBox_2)
    self.gridLayout_2.setObjectName("gridLayout_2")
    self.lineEdit = QtGui.QLineEdit(self.groupBox_2)
    self.lineEdit.setEnabled(False)
    self.lineEdit.setObjectName("lineEdit")
    self.gridLayout_2.addWidget(self.lineEdit, 1, 0, 1, 2)
    self.bookmarks = QtGui.QTableView(self.groupBox_2)
    self.bookmarks.setObjectName("bookmarks")
    self.gridLayout_2.addWidget(self.bookmarks, 2, 0, 1, 1)
    self.gridLayout_3.addWidget(self.groupBox_2, 1, 1, 1, 1)
    self.groupBox = QtGui.QGroupBox(self.groupBox_3)
    self.groupBox.setObjectName("groupBox")
    self.gridLayout = QtGui.QGridLayout(self.groupBox)
    self.gridLayout.setObjectName("gridLayout")
    spacerItem = QtGui.QSpacerItem(40, 20, QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Minimum)
    self.gridLayout.addItem(spacerItem, 0, 0, 1, 1)
    self.removeButton = QtGui.QPushButton(self.groupBox)
    icon = QtGui.QIcon()
    icon.addPixmap(QtGui.QPixmap(":/icons/images/remove.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.removeButton.setIcon(icon)
    self.removeButton.setObjectName("removeButton")
    self.gridLayout.addWidget(self.removeButton, 0, 1, 1, 1)
    self.addButton = QtGui.QPushButton(self.groupBox)
    icon1 = QtGui.QIcon()
    icon1.addPixmap(QtGui.QPixmap(":/icons/images/add.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.addButton.setIcon(icon1)
    self.addButton.setObjectName("addButton")
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
