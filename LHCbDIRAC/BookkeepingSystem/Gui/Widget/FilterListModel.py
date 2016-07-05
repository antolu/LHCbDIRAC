"""
Implementation of the ListModel
"""

from LHCbDIRAC.BookkeepingSystem.Gui.Basic.ListModel                   import ListModel


__RCSID__ = "$Id$"

class FilterListModel(ListModel):
  """
  FilterListModel class
  """
  #############################################################################
  def __init__(self, parent=None, *args):
    """initialize the model"""
    ListModel.__init__(self, parent, *args)

