########################################################################
# $Id$
########################################################################

from LHCbDIRAC.NewBookkeepingSystem.Gui.Controler.ControlerAbstract import ControlerAbstract

__RCSID__ = "$Id$"

#############################################################################  
class ControlerTable(ControlerAbstract):
  
  #############################################################################  
  def __init__(self, widget, parent):
    super(ControlerTable, self).__init__(widget, parent)
  
  #############################################################################  
  def messageFromParent(self, message):
    print 'Uzit kaptam',message
  
  #############################################################################  
  def messageFromChild(self, sender, message):
    pass
  
    