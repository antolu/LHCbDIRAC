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

"""Tree node widget"""

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

