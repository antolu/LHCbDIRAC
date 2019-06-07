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
"""
basic controller used
"""

__RCSID__ = "$Id$"

#############################################################################
class ControlerAbstract(object):
  """abstarct class"""
  #############################################################################
  def __init__(self, widget, parent):
    """initialie the class member"""
    self.__widget = widget
    self.__children = {}
    self.__parent = parent

  #############################################################################
  def getWidget(self):
    """returns the associated widget"""
    return self.__widget

  #############################################################################
  def messageFromParent(self, message):
    """must be re-implemented in all subclasses"""
    pass

  #############################################################################
  def messageFromChild(self, sender, message):
    """must be re-implemented in all subclasses"""
    pass

  #############################################################################
  def addChild(self, name, child):
    """add a child controller"""
    self.__children[name] = child

  #############################################################################
  def removeChild(self, name):
    """removes a child"""
    del self.__children[name]

  #############################################################################
  def getChildren(self):
    """returns the children of the current controller"""
    return self.__children

  #############################################################################
  def getParent(self):
    """returns the parent controller of the current controller"""
    return self.__parent

  #############################################################################
  def setParent(self, parent):
    """sets the parent of the current controller"""
    self.__parent = parent

