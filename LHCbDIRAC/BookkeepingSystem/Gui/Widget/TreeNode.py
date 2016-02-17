# pylint: skip-file

"""Tree node widget"""
########################################################################
# $Id: TreeNode.py 84842 2015-08-11 13:47:15Z fstagni $
########################################################################

from PyQt4.QtGui           import QTreeWidgetItem

__RCSID__ = "$Id$"

#############################################################################
class TreeNode(QTreeWidgetItem):
  """
  TreeNode class
  """
  #############################################################################
  def __init__(self, parent=None):
    """initialize a node"""
    QTreeWidgetItem.__init__(self, parent)
    self.__item = None

  #############################################################################
  def setUserObject(self, obj):
    """sets the user data to the node"""
    self.__item = obj

  #############################################################################
  def getUserObject(self):
    """returns the user data """
    return self.__item

  #############################################################################

