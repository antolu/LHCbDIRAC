# pylint: skip-file

# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'TreeWidget.ui'
#
# Created: Fri Sep  7 13:27:01 2012
#      by: PyQt4 UI code generator 4.7
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_TreeWidget(object):
  def setupUi(self, TreeWidget):
    TreeWidget.setObjectName("TreeWidget")
    TreeWidget.resize(648, 736)
    TreeWidget.setAutoFillBackground(True)
    self.gridLayout_2 = QtGui.QGridLayout(TreeWidget)
    self.gridLayout_2.setObjectName("gridLayout_2")
    self.Bookmarks = BookmarksWidget(TreeWidget)
    self.Bookmarks.setObjectName("Bookmarks")
    self.gridLayout_2.addWidget(self.Bookmarks, 0, 0, 3, 1)
    self.groupBox = QtGui.QGroupBox(TreeWidget)
    self.groupBox.setObjectName("groupBox")
    self.gridLayout = QtGui.QGridLayout(self.groupBox)
    self.gridLayout.setObjectName("gridLayout")
    self.standardQuery = QtGui.QCheckBox(self.groupBox)
    self.standardQuery.setObjectName("standardQuery")
    self.gridLayout.addWidget(self.standardQuery, 0, 0, 1, 1)
    self.advancedQuery = QtGui.QCheckBox(self.groupBox)
    self.advancedQuery.setObjectName("advancedQuery")
    self.gridLayout.addWidget(self.advancedQuery, 0, 1, 1, 1)
    self.pageSize = QtGui.QLineEdit(self.groupBox)
    self.pageSize.setObjectName("pageSize")
    self.gridLayout.addWidget(self.pageSize, 4, 1, 1, 2)
    self.label = QtGui.QLabel(self.groupBox)
    self.label.setObjectName("label")
    self.gridLayout.addWidget(self.label, 4, 0, 1, 1)
    spacerItem = QtGui.QSpacerItem(40, 20, QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Minimum)
    self.gridLayout.addItem(spacerItem, 4, 3, 1, 1)
    self.bookmarksButton = QtGui.QPushButton(self.groupBox)
    self.bookmarksButton.setAutoFillBackground(False)
    icon = QtGui.QIcon()
    icon.addPixmap(QtGui.QPixmap(":/icons/images/bookmarks2.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.bookmarksButton.setIcon(icon)
    self.bookmarksButton.setObjectName("bookmarksButton")
    self.gridLayout.addWidget(self.bookmarksButton, 0, 3, 1, 1)
    spacerItem1 = QtGui.QSpacerItem(40, 20, QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Minimum)
    self.gridLayout.addItem(spacerItem1, 0, 2, 1, 1)
    self.closeButton = QtGui.QToolButton(self.groupBox)
    icon1 = QtGui.QIcon()
    icon1.addPixmap(QtGui.QPixmap(":/icons/images/reloadpage.png"), QtGui.QIcon.Normal, QtGui.QIcon.Off)
    self.closeButton.setIcon(icon1)
    self.closeButton.setObjectName("closeButton")
    self.gridLayout.addWidget(self.closeButton, 0, 4, 1, 1)
    self.gridLayout_2.addWidget(self.groupBox, 0, 1, 1, 1)
    self.tree = TreePanel(TreeWidget)
    self.tree.setEnabled(True)
    self.tree.setProperty("cursor", QtCore.Qt.ArrowCursor)
    self.tree.setLineWidth(1)
    self.tree.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
    self.tree.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
    self.tree.setTabKeyNavigation(True)
    self.tree.setTextElideMode(QtCore.Qt.ElideNone)
    self.tree.setAnimated(False)
    self.tree.setObjectName("tree")
    self.gridLayout_2.addWidget(self.tree, 1, 1, 1, 1)
    self.selection = QtGui.QGroupBox(TreeWidget)
    self.selection.setObjectName("selection")
    self.gridlayout = QtGui.QGridLayout(self.selection)
    self.gridlayout.setObjectName("gridlayout")
    self.configNameRadioButton = QtGui.QRadioButton(self.selection)
    self.configNameRadioButton.setObjectName("configNameRadioButton")
    self.gridlayout.addWidget(self.configNameRadioButton, 0, 0, 1, 1)
    self.radioButton_2 = QtGui.QRadioButton(self.selection)
    self.radioButton_2.setObjectName("radioButton_2")
    self.gridlayout.addWidget(self.radioButton_2, 1, 0, 1, 1)
    self.productionRadioButton = QtGui.QRadioButton(self.selection)
    self.productionRadioButton.setObjectName("productionRadioButton")
    self.gridlayout.addWidget(self.productionRadioButton, 2, 0, 1, 1)
    self.runLookup = QtGui.QRadioButton(self.selection)
    self.runLookup.setObjectName("runLookup")
    self.gridlayout.addWidget(self.runLookup, 3, 0, 1, 1)
    self.gridLayout_2.addWidget(self.selection, 2, 1, 1, 1)
    self.configNameRadioButton1 = QtGui.QAction(TreeWidget)
    self.configNameRadioButton1.setObjectName("configNameRadioButton1")

    self.retranslateUi(TreeWidget)
    QtCore.QMetaObject.connectSlotsByName(TreeWidget)

  def retranslateUi(self, TreeWidget):
    TreeWidget.setWindowTitle(QtGui.QApplication.translate("TreeWidget", "Form", None, QtGui.QApplication.UnicodeUTF8))
    self.standardQuery.setText(QtGui.QApplication.translate("TreeWidget", "Standard", None, QtGui.QApplication.UnicodeUTF8))
    self.advancedQuery.setText(QtGui.QApplication.translate("TreeWidget", "Advanced Queries", None, QtGui.QApplication.UnicodeUTF8))
    self.pageSize.setText(QtGui.QApplication.translate("TreeWidget", "ALL", None, QtGui.QApplication.UnicodeUTF8))
    self.label.setText(QtGui.QApplication.translate("TreeWidget", "Page Size:", None, QtGui.QApplication.UnicodeUTF8))
    self.bookmarksButton.setText(QtGui.QApplication.translate("TreeWidget", "Bookmarks", None, QtGui.QApplication.UnicodeUTF8))
    self.closeButton.setText(QtGui.QApplication.translate("TreeWidget", "...", None, QtGui.QApplication.UnicodeUTF8))
    self.tree.setSortingEnabled(False)
    self.tree.headerItem().setText(0, QtGui.QApplication.translate("TreeWidget", "                                Tree                                                     ", None, QtGui.QApplication.UnicodeUTF8))
    self.tree.headerItem().setText(1, QtGui.QApplication.translate("TreeWidget", "Description", None, QtGui.QApplication.UnicodeUTF8))
    self.selection.setTitle(QtGui.QApplication.translate("TreeWidget", "Queries", None, QtGui.QApplication.UnicodeUTF8))
    self.configNameRadioButton.setText(QtGui.QApplication.translate("TreeWidget", "SimCond/ProcessingPass/Eventtype/Production/FileType/Program/Files", None, QtGui.QApplication.UnicodeUTF8))
    self.radioButton_2.setText(QtGui.QApplication.translate("TreeWidget", "Event type/SimCond/ProcessingPass/Production/FileType/Program/Files", None, QtGui.QApplication.UnicodeUTF8))
    self.productionRadioButton.setText(QtGui.QApplication.translate("TreeWidget", "Production lookup", None, QtGui.QApplication.UnicodeUTF8))
    self.runLookup.setText(QtGui.QApplication.translate("TreeWidget", "Run lookup", None, QtGui.QApplication.UnicodeUTF8))
    self.configNameRadioButton1.setText(QtGui.QApplication.translate("TreeWidget", "config_click", None, QtGui.QApplication.UnicodeUTF8))

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.TreePanel import TreePanel
from LHCbDIRAC.BookkeepingSystem.Gui.Widget.BookmarksWidget import BookmarksWidget
import Resources_rc
