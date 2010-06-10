from DIRAC.ResourceStatusSystem.PolicySystem.PolicyBase import PolicyBase

class Fake_Policy(PolicyBase):
  
  def evaluate(self):
    """ 
    """ 
      
    Fake_R = super(Fake_Policy, self).evaluate()
    
    return {'SAT':'NeedConfirmation'}
    
  evaluate.__doc__ = PolicyBase.evaluate.__doc__ + evaluate.__doc__