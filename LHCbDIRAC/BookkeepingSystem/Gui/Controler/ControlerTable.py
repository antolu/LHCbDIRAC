########################################################################
# $Id: ControlerTable.py,v 1.1 2008/09/25 15:50:33 zmathe Exp $
########################################################################

from DIRAC.BookkeepingSystem.Gui.Controler.ControlerAbstract import ControlerAbstract

__RCSID__ = "$Id: ControlerTable.py,v 1.1 2008/09/25 15:50:33 zmathe Exp $"

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
  
  #############################################################################  