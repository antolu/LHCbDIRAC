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
"""used to store a tree structure"""

__RCSID__ = "$Id$"

#############################################################################
class Item(dict):
  """Item class"""
  #############################################################################
  def __init__(self, properties, parent):
    """ initializes the class member"""
    super(Item, self).__init__(properties)
    self.__parent = parent
    self.__children = {}
    self.__childrenNumber = 0
    self.__childmap = {}
  #############################################################################
  def getParent(self):
    """returns the parent of a node"""
    return self.__parent

  #############################################################################
  def addItem(self, item):
    """adds an item to the node"""
    name = item['name']
    self.__children[name] = item
    self.__childmap[self.__childrenNumber] = item
    self.__childrenNumber = self.__childrenNumber + 1


  #############################################################################
  def getChildren(self):
    """returns the children"""
    return self.__children

  #############################################################################
  def child(self, i):
    """retuns the i-th csild"""
    return self.__childmap[i]

  #############################################################################
  def name(self):
    """returns the name of the node"""
    return self['name']

  #############################################################################
  def childnum(self):
    """returns the number of childs"""
    return self.__childrenNumber

  #############################################################################
  def expandable(self):
    """is the node has children"""
    return self['expandable']

  #############################################################################
