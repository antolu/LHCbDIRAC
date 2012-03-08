########################################################################
# $Id$
########################################################################
from PyQt4.QtGui                                                          import *
from PyQt4.QtCore                                                         import *
from LHCbDIRAC.BookkeepingSystem.Gui.Basic.ListModel                   import ListModel


__RCSID__ = "$Id$"

class ProductionListModel(ListModel):
  #############################################################################
  def __init__(self, datain = None, parent=None, *args):
      if datain != None:
       datain = datain.getChildren()
      ListModel.__init__(self,datain, parent, *args)
