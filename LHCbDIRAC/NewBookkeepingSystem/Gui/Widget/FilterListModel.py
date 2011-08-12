########################################################################
# $Id: $
########################################################################
from PyQt4.QtGui                                                          import *
from PyQt4.QtCore                                                         import *
from LHCbDIRAC.NewBookkeepingSystem.Gui.Basic.ListModel                   import ListModel


__RCSID__ = "$Id: $"

class FilterListModel(ListModel):
  #############################################################################
  def __init__(self, parent=None, *args):
      ListModel.__init__(self, parent, *args)
