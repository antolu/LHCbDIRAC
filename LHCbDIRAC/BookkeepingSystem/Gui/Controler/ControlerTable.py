########################################################################
# $Id: ControlerTable.py 18175 2009-11-11 14:02:19Z zmathe $
########################################################################

from LHCbDIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract import ControlerAbstract

__RCSID__ = "$Id: ControlerTable.py 18175 2009-11-11 14:02:19Z zmathe $"

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
  
    