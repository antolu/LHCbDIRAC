"""
Production list model
"""
########################################################################
# $Id: ProductionListModel.py 55878 2012-09-05 16:08:28Z zmathe $
########################################################################
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


