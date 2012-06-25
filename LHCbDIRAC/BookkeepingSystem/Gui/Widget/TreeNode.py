########################################################################
# $Id$
########################################################################

from PyQt4.QtGui           import QTreeWidgetItem

__RCSID__ = "$Id$"

#############################################################################
class TreeNode(QTreeWidgetItem):

  #############################################################################
  def __init__(self, parent=None):
    QTreeWidgetItem.__init__(self, parent)
    self.__item = None

  #############################################################################
  def setUserObject(self, obj):
    self.__item = obj

  #############################################################################
  def getUserObject(self):
    return self.__item

  #############################################################################
