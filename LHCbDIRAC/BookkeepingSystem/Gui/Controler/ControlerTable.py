"""
Controlles a table widget
"""

from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract import ControlerAbstract

__RCSID__ = "$Id$"

#############################################################################
class ControlerTable(ControlerAbstract):
  """
  ControlerTable class
  """
  #############################################################################
  def __init__(self, widget, parent):
    """initialize the controller"""
    ControlerAbstract.__init__(self, widget, parent)

  #############################################################################
  def messageFromParent(self, message):
    pass

  #############################################################################
  def messageFromChild(self, sender, message):
    pass

