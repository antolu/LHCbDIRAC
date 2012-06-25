########################################################################
# $Id: $
########################################################################

from LHCbDIRAC.BookkeepingSystem.Gui.Basic.ListModel                   import ListModel


__RCSID__ = "$Id: $"

class FilterListModel(ListModel):
  #############################################################################
  def __init__(self, parent=None, *args):
      ListModel.__init__(self, parent, *args)
