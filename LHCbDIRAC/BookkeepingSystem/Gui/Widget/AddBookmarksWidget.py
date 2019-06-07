###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
# pylint: skip-file

"""
AddBookmarks widget
"""
########################################################################
from PyQt4.QtCore  import SIGNAL, Qt
from PyQt4.QtGui   import QDialog

from LHCbDIRAC.BookkeepingSystem.Gui.Widget.Ui_AddBookmarks           import Ui_AddBookmarks
from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAddBookmarks  import ControlerAddBookmarks

__RCSID__ = "$Id$"


#############################################################################
class AddBookmarksWidget(QDialog, Ui_AddBookmarks):
  """
  AddBookmarksWidget class
  """
  #############################################################################
  def __init__(self, parent = None):
    """
    Constructor

    @param parent parent widget (QWidget)
    """
    QDialog.__init__(self, parent)
    Ui_AddBookmarks.__init__(self)
    self.setupUi(self)

    self.__controler = ControlerAddBookmarks(self, parent.getControler())

    self.connect(self.okButton, SIGNAL("clicked()"), self.__controler.okButton)
    self.connect(self.cancelButton, SIGNAL("clicked()"), self.__controler.cancelButton)
    self.__model = None


  #############################################################################
  def getControler(self):
    """returns the controller"""
    return self.__controler

  #############################################################################
  def getTitle(self):
    """returns the title"""
    return str(self.titlelineEdit.text())

  #############################################################################
  def getPath(self):
    """returns the path"""
    return str(self.pathlineEdit.text())

  #############################################################################
  def setTitle(self, string):
    """sets the title"""
    self.titlelineEdit.setText(string)

  #############################################################################
  def setPath(self, string):
    """sets the path"""
    self.pathlineEdit.setText(string)

  #############################################################################
  def waitCursor(self):
    """wait cursor"""
    self.setCursor(Qt.WaitCursor)

  #############################################################################
  def arrowCursor(self):
    """normal cursor"""
    self.setCursor(Qt.ArrowCursor)
