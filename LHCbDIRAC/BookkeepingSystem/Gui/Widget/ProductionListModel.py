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
Production list model
"""

from LHCbDIRAC.BookkeepingSystem.Gui.Basic.ListModel                   import ListModel


__RCSID__ = "$Id$"

class ProductionListModel(ListModel):
  """
  ProductionListModel class
  """
  #############################################################################
  def __init__(self, datain=None, parent=None, *args):
    if datain != None:
      datain = datain.getChildren()
    ListModel.__init__(self, datain, parent, *args)


