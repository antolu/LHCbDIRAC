
 
#############################################################################
class Context:
  def __init__(self, conditions, part = 'LHCb'):
    self.__input = conditions
    self.__output = ''
    self.__partition = part
  
  def getInput(self):
    return self.__input
  
  def setOutput(self, text):
    if text != None:
      self.__output += str(text)
  
  def getOutput(self):
    return self.__output.strip('-')
  
  def getParticionName(self):
    return self.__partition
  
#############################################################################
class Conditions:
  
  def __init__(self):
    self.__condition = 'NOT INCLUDED'
  
  def getCondition(self):
    return self.__condition
   
  def interpret(self, context):
    if context.getInput().has_key(self.BeamCond()):
      context.setOutput(self.Template(context.getInput()[self.BeamCond()]))
          
    if context.getInput().has_key(self.Beamenergy()):
      context.setOutput(self.Template(context.getInput()[self.Beamenergy()]))
    
    if context.getInput().has_key(self.VeloCond()):
      if context.getParticionName().upper() =='LHCB' or context.getParticionName().upper() =='VELO':
        context.setOutput(self.Template(context.getInput()[self.VeloCond()]))
      
    if context.getInput().has_key(self.MagneticField()):
      context.setOutput(self.Template(context.getInput()[self.MagneticField()]))
      
    if context.getInput().has_key(self.Ecal()):
      if context.getInput()[self.Ecal()] == self.getCondition():
        if context.getParticionName().upper() =='LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Ecal()]))
      
    
    if context.getInput().has_key(self.Hcal()):
      if context.getInput()[self.Hcal()] == self.getCondition():
        if context.getParticionName().upper() =='LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Hcal()]))
      
    if context.getInput().has_key(self.Hlt()):
      if context.getInput()[self.Hlt()] == self.getCondition():
        if context.getParticionName().upper() =='LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Hlt()]))
          
    if context.getInput().has_key(self.It()):
      if context.getInput()[self.It()] == self.getCondition():
        if context.getParticionName().upper() =='LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.It()]))
          
    if context.getInput().has_key(self.Lo()):
      if context.getInput()[self.Lo()] == self.getCondition():
        if context.getParticionName().upper() =='LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Lo()]))
          
    if context.getInput().has_key(self.Muon()):
      if context.getInput()[self.Muon()] == self.getCondition():
        if context.getParticionName().upper() =='LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Muon()]))
          
    if context.getInput().has_key(self.Ot()):
      if context.getInput()[self.Ot()] == self.getCondition():
        if context.getParticionName().upper() =='LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Ot()]))
      
    if context.getInput().has_key(self.Rich1()):
      if context.getInput()[self.Rich1()] == self.getCondition():
        if context.getParticionName().upper() =='LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Rich1()]))
          
    if context.getInput().has_key(self.Rich2()):
      if context.getInput()[self.Rich2()] == self.getCondition():
        if context.getParticionName().upper() =='LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Rich2()]))
          
    if context.getInput().has_key(self.Spd_prs()):
      if context.getInput()[self.Spd_prs()] == self.getCondition():
        if context.getParticionName().upper() =='LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Spd_prs()]))
          
    if context.getInput().has_key(self.Tt()):
      if context.getInput()[self.Tt()] == self.getCondition():
        if context.getParticionName().upper() =='LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Tt()]))
          
    if context.getInput().has_key(self.Velo()):
      if context.getInput()[self.Velo()] == self.getCondition():
        if context.getParticionName().upper() =='LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Velo()]))
         
         
  def BeamCond(self):
    pass
  
  def Beamenergy(self):
    pass
  
  def Velo(self):
    pass
  
  def MagneticField(self):
    pass

  def Ecal(self):
    pass
  
  def Hcal(self):
    pass
  
  def Hlt(self):
    pass
  
  def It(self):
    pass
  
  def Lo(self):
    pass
  
  def Muon(self):
    pass
  
  def Ot(self):
    pass
  
  def Rich1(self):
    pass
  
  def Rich2(self):
    pass
  
  def  Spd_prs(self):
    pass
  
  def Tt(self):
    pass
  
  def VeloCond(self):
    pass
  
  def Template(self, value):
    pass
  
  def Template2(self, value):
    return ''
  
  def Excl(self, context):
    if context.getOutput().find('Excl') < 0:
      context.setOutput('Excl-')

#############################################################################  
class BeamCondition(Conditions):
  
  def BeamCond(self):
    return 'BeamCond'
  
  def Template(self, value):
    return value

#############################################################################
class BeamEnergyCondition(Conditions):
  
  def Beamenergy(self):
    return 'BeamEnergy'
  
  def Template(self, value):   
    try:
      if value != None:
        if (value.strip() != 'None'):
          if value == 'UNKOWN' or float(value) == 0 or float(value) == 7864 or float(value) >= 7864: 
            return 'BeamOff-'
          else:
            return 'Beam'+str(int(float(value))) +'GeV-'
    except Exception ,e:
      print e
    return 'BeamOff-'

#############################################################################
class MagneticFieldCondition(Conditions):
  
  def MagneticField(self):
    return 'MagneticField'
  
  def Template(self, value):
    if value.upper()=='OFF':
      return 'MagOff-'
    elif value.upper() == 'DOWN':
      return 'MagDown-'
    elif value.upper() == 'UP':
      return 'MagUp-' 
    else:
      return 'Mag'+value+'-'

#############################################################################
class VeloPosition(Conditions):
  
  def Velo(self):
    return 'VELO'

  def Template(self, value):
    if value == self.getCondition():
      return 'VE-'
  
#############################################################################  
class EcalCondition(Conditions):
  
  def Ecal(self):
    return 'ECAL'
  
  def Template(self, value):
    if value == self.getCondition():
      return 'EC-'
#############################################################################
class HcalCondition(Conditions):
  def Hcal(self):
    return 'HCAL'
  
  def Template(self, value):
    if value == self.getCondition():
      return 'HC-'
    
#############################################################################
class HltCondition(Conditions):
  def Hlt(self):
    return 'HLT'
  
  def Template(self, value):
    if value == self.getCondition():
      return 'HL-'

#############################################################################
class ItCondition(Conditions):
  
  def It(self):
    return 'IT'
  
  def Template(self, value):
    if value == self.getCondition():
      return 'IT-'

#############################################################################
class LoCondition(Conditions):
  
  def Lo(self):
    return 'LO' 
  
  def Template(self, value):
    if value == self.getCondition():
      return 'LO-'

#############################################################################
class MuonCondition(Conditions):
  
  def Muon(self):
    return 'MUON'
  
  def Template(self, value):
    if value == self.getCondition():
      return 'MU-'
    
#############################################################################
class OtCondition(Conditions):
  
  def Ot(self):
    return 'OT'
  
  def Template(self, value):
    if value == self.getCondition():
      return 'OT-'

#############################################################################    
class Rich1Condition(Conditions):
  
  def Rich1(self):
    return 'RICH1' 
  
  def Template(self, value):
    if value == self.getCondition():
      return 'R1-'

#############################################################################
class Rich2Condition(Conditions):
  
  def Rich2(self):
    return 'RICH2'
  
  def Template(self, value):
    if value == self.getCondition():
      return 'R2-'

#############################################################################
class Spd_prsCondition(Conditions):
  def  Spd_prs(self):
    return 'SPD_PRS'
  
  def Template(self, value):
    if value == self.getCondition():
      return 'SP-'

#############################################################################
class TtCondition(Conditions):
  def Tt(self):
    return 'TT'
  
  def Template(self, value):
    if value == self.getCondition():
      return 'TT-'

#############################################################################
class VeloCondition(Conditions):
  def VeloCond(self):
    return 'VeloPosition'
  
  def Template(self, value):
    if value.upper()=='OPEN':
      return 'VeloOpen-'
    elif value.upper() == 'CLOSED':
      return 'VeloClosed-' 
    else:    
      return 'Velo'+value+'-'

#############################################################################

if __name__ == "__main__":
  datataking = {  'Description':'Blalbla',\
                    'BeamCond':'UNKNOWN', \
                    'BeamEnergy':'0.0', \
                    'MagneticField':'OFF', \
                    'VELO':'NOT INCLUDED', \
                    'IT':'NOT INCLUDED',  \
                    'TT':'NOT INCLUDED', \
                    'OT':'NOT INCLUDED', \
                    'RICH1':'NOT INCLUDED',  \
                    'RICH2':'NOT INCLUDED', \
                    'SPD_PRS':'INCLUDED',\
                    'ECAL':'NOT INCLUDED', \
                    'HCAL':'NOT INCLUDED', \
                    'MUON':'NOT INCLUDED', \
                    'L0':'NOT INCLUDED', \
                    'HLT':'UNKOWN', \
                    'VeloPosition':'OPEN'}
    
  print datataking
  
  context = Context(datataking, 'PRS')
  conditions = [BeamEnergyCondition(),VeloCondition(), MagneticFieldCondition(), EcalCondition(), HcalCondition(), HltCondition(), ItCondition(), LoCondition(), \
              MuonCondition(), OtCondition(), Rich1Condition(), Rich2Condition(), Spd_prsCondition(), TtCondition(), VeloPosition()]
      
 
  for condition in conditions:
    condition.interpret(context)
  
  print context.getOutput()