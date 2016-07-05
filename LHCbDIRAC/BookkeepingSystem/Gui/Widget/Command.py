"""
Interfcae of the history browser command
"""


__RCSID__ = "$Id$"

########################################################################
class Command:
  """Command inteface"""
  def __init__(self):
    pass
  ########################################################################
  def execute(self):
    """must be reimplemented"""
    pass

  ########################################################################
  def unexecute(self):
    """must be reimplemented"""
    pass
