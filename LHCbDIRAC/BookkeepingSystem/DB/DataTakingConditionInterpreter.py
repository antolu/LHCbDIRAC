"""interpret the data taking conditions"""
__RCSID__ = "$Id$"

#############################################################################
class Context:
  """the data taking condition"""
  def __init__(self, conditions, part='LHCb'):
    """initialize the variables"""
    self.__input = conditions
    self.__output = ''
    self.__partition = part

  def getInput(self):
    """get input"""
    return self.__input

  def setOutput(self, text):
    """output string"""
    if text != None:
      self.__output += str(text)

  def getOutput(self):
    """get output"""
    return self.__output.strip('-')

  def getParticionName(self):
    """partition """
    return self.__partition

#############################################################################
class Conditions:
  """different conditions"""

  def __init__(self):
    """initialize the variables"""
    self.__condition = 'NOT INCLUDED'

  def getCondition(self):
    """conditions"""
    return self.__condition

  def interpret(self, context):
    """interpret the context"""
    if context.getInput().has_key(self.BeamCond()):
      context.setOutput(self.Template(context.getInput()[self.BeamCond()]))

    if context.getInput().has_key(self.Beamenergy()):
      context.setOutput(self.Template(context.getInput()[self.Beamenergy()]))

    if context.getInput().has_key(self.VeloCond()):
      if context.getParticionName().upper() == 'LHCB' or context.getParticionName().upper() == 'VELO':
        context.setOutput(self.Template(context.getInput()[self.VeloCond()]))

    if context.getInput().has_key(self.MagneticField()):
      context.setOutput(self.Template(context.getInput()[self.MagneticField()]))

    if context.getInput().has_key(self.Ecal()):
      if context.getInput()[self.Ecal()] == self.getCondition():
        if context.getParticionName().upper() == 'LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Ecal()]))


    if context.getInput().has_key(self.Hcal()):
      if context.getInput()[self.Hcal()] == self.getCondition():
        if context.getParticionName().upper() == 'LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Hcal()]))

    if context.getInput().has_key(self.Hlt()):
      if context.getInput()[self.Hlt()] == self.getCondition():
        if context.getParticionName().upper() == 'LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Hlt()]))

    if context.getInput().has_key(self.It()):
      if context.getInput()[self.It()] == self.getCondition():
        if context.getParticionName().upper() == 'LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.It()]))

    if context.getInput().has_key(self.Lo()):
      if context.getInput()[self.Lo()] == self.getCondition():
        if context.getParticionName().upper() == 'LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Lo()]))

    if context.getInput().has_key(self.Muon()):
      if context.getInput()[self.Muon()] == self.getCondition():
        if context.getParticionName().upper() == 'LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Muon()]))

    if context.getInput().has_key(self.Ot()):
      if context.getInput()[self.Ot()] == self.getCondition():
        if context.getParticionName().upper() == 'LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Ot()]))

    if context.getInput().has_key(self.Rich1()):
      if context.getInput()[self.Rich1()] == self.getCondition():
        if context.getParticionName().upper() == 'LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Rich1()]))

    if context.getInput().has_key(self.Rich2()):
      if context.getInput()[self.Rich2()] == self.getCondition():
        if context.getParticionName().upper() == 'LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Rich2()]))

    if context.getInput().has_key(self.Spd_prs()):
      if context.getInput()[self.Spd_prs()] == self.getCondition():
        if context.getParticionName().upper() == 'LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Spd_prs()]))

    if context.getInput().has_key(self.Tt()):
      if context.getInput()[self.Tt()] == self.getCondition():
        if context.getParticionName().upper() == 'LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Tt()]))

    if context.getInput().has_key(self.Velo()):
      if context.getInput()[self.Velo()] == self.getCondition():
        if context.getParticionName().upper() == 'LHCB':
          self.Excl(context)
          context.setOutput(self.Template(context.getInput()[self.Velo()]))


  def BeamCond(self):
    """define a condition"""
    pass

  def Beamenergy(self):
    """define a condition"""
    pass

  def Velo(self):
    """define a condition"""
    pass

  def MagneticField(self):
    """define a condition"""
    pass

  def Ecal(self):
    """define a condition"""
    pass

  def Hcal(self):
    """define a condition"""
    pass

  def Hlt(self):
    """define a condition"""
    pass

  def It(self):
    """define a condition"""
    pass

  def Lo(self):
    """define a condition"""
    pass

  def Muon(self):
    """define a condition"""
    pass

  def Ot(self):
    """define a condition"""
    pass

  def Rich1(self):
    """define a condition"""
    pass

  def Rich2(self):
    """define a condition"""
    pass

  def  Spd_prs(self):
    """define a condition"""
    pass

  def Tt(self):
    """define a condition"""
    pass

  def VeloCond(self):
    """define a condition"""
    pass

  def Template(self, value):
    """define a condition"""
    pass

  def Template2(self, value):
    """define a condition"""
    return ''

  def Excl(self, context):
    """define a condition"""
    if context.getOutput().find('Excl') < 0:
      context.setOutput('Excl-')

#############################################################################
class BeamCondition(Conditions):
  """condition class"""
  def BeamCond(self):
    """beamcondition"""
    return 'BeamCond'

  def Template(self, value):
    """template method"""
    return value

#############################################################################
class BeamEnergyCondition(Conditions):
  """Energy """
  def Beamenergy(self):
    """beam energy"""
    return 'BeamEnergy'

  def Template(self, value):
    """Template method"""
    try:
      if value != None:
        if (value.strip() != 'None'):
          if value == 'UNKOWN' or float(value) == 0 or float(value) == 7864 or float(value) >= 7864:
            return 'BeamOff-'
          else:
            return 'Beam' + str(int(float(value))) + 'GeV-'
    except Exception , e:
      print e
    return 'BeamOff-'

#############################################################################
class MagneticFieldCondition(Conditions):
  """magnetic field"""
  def MagneticField(self):
    """magfild"""
    return 'MagneticField'

  def Template(self, value):
    """..."""
    if value.upper() == 'OFF':
      return 'MagOff-'
    elif value.upper() == 'DOWN':
      return 'MagDown-'
    elif value.upper() == 'UP':
      return 'MagUp-'
    else:
      return 'Mag' + value + '-'

#############################################################################
class VeloPosition(Conditions):
  """Velo position class"""
  def Velo(self):
    """Open or Closed"""
    return 'VELO'

  def Template(self, value):
    """tempalte method"""
    if value == self.getCondition():
      return 'VE-'

#############################################################################
class EcalCondition(Conditions):
  """ECAL class"""
  def Ecal(self):
    """status of the subdetector"""
    return 'ECAL'

  def Template(self, value):
    """template method"""
    if value == self.getCondition():
      return 'EC-'
#############################################################################
class HcalCondition(Conditions):
  """HCAL class"""
  def Hcal(self):
    """status of the subdetector"""
    return 'HCAL'

  def Template(self, value):
    """template method"""
    if value == self.getCondition():
      return 'HC-'

#############################################################################
class HltCondition(Conditions):
  """HLT class"""
  def Hlt(self):
    """status of the subdetector"""
    return 'HLT'

  def Template(self, value):
    """template methos"""
    if value == self.getCondition():
      return 'HL-'

#############################################################################
class ItCondition(Conditions):
  """It class"""
  def It(self):
    """status of the subdetector"""
    return 'IT'

  def Template(self, value):
    """template method"""
    if value == self.getCondition():
      return 'IT-'

#############################################################################
class LoCondition(Conditions):
  """Lo class"""
  def Lo(self):
    """status of the subdetector"""
    return 'LO'

  def Template(self, value):
    """template method"""
    if value == self.getCondition():
      return 'LO-'

#############################################################################
class MuonCondition(Conditions):
  """Muon class"""
  def Muon(self):
    """status of the subdetector"""
    return 'MUON'

  def Template(self, value):
    """template method"""
    if value == self.getCondition():
      return 'MU-'

#############################################################################
class OtCondition(Conditions):
  """Ot class"""
  def Ot(self):
    """status of the subdetector"""
    return 'OT'

  def Template(self, value):
    """template method"""
    if value == self.getCondition():
      return 'OT-'

#############################################################################
class Rich1Condition(Conditions):
  """RICH1 class"""
  def Rich1(self):
    """status of the subdetector"""
    return 'RICH1'

  def Template(self, value):
    """template method"""
    if value == self.getCondition():
      return 'R1-'

#############################################################################
class Rich2Condition(Conditions):
  """RICH2 class"""
  def Rich2(self):
    """status of the subdetector"""
    return 'RICH2'

  def Template(self, value):
    """template method"""
    if value == self.getCondition():
      return 'R2-'

#############################################################################
class Spd_prsCondition(Conditions):
  """SPD class"""
  def  Spd_prs(self):
    """status of the subdetector"""
    return 'SPD_PRS'

  def Template(self, value):
    """template method"""
    if value == self.getCondition():
      return 'SP-'

#############################################################################
class TtCondition(Conditions):
  """TT class"""
  def Tt(self):
    """status of the subdetector"""
    return 'TT'

  def Template(self, value):
    """template method"""
    if value == self.getCondition():
      return 'TT-'

#############################################################################
class VeloCondition(Conditions):
  """Velo class"""
  def VeloCond(self):
    """status of the subdetector"""
    return 'VeloPosition'

  def Template(self, value):
    """template method"""
    if value.upper() == 'OPEN':
      return 'VeloOpen-'
    elif value.upper() == 'CLOSED':
      return 'VeloClosed-'
    else:
      return 'Velo' + value + '-'

#############################################################################

if __name__ == "__main__":
  datataking = {  'Description':'Blalbla', \
                    'BeamCond':'UNKNOWN', \
                    'BeamEnergy':'0.0', \
                    'MagneticField':'OFF', \
                    'VELO':'NOT INCLUDED', \
                    'IT':'NOT INCLUDED', \
                    'TT':'NOT INCLUDED', \
                    'OT':'NOT INCLUDED', \
                    'RICH1':'NOT INCLUDED', \
                    'RICH2':'NOT INCLUDED', \
                    'SPD_PRS':'INCLUDED', \
                    'ECAL':'NOT INCLUDED', \
                    'HCAL':'NOT INCLUDED', \
                    'MUON':'NOT INCLUDED', \
                    'L0':'NOT INCLUDED', \
                    'HLT':'UNKOWN', \
                    'VeloPosition':'OPEN'}

  datataking = {'VELO': 'INCLUDED',
                'RICH2': 'INCLUDED',
                'RICH1': 'INCLUDED',
                'BeamEnergy': '4000.0',
                'SPD_PRS': 'INCLUDED',
                'ECAL': 'INCLUDED',
                'TT': 'INCLUDED',
                'MagneticField': 'DOWN',
                'IT': 'INCLUDED',
                'BeamCond': 'UNKNOWN',
                'MUON': 'INCLUDED',
                'L0': 'INCLUDED',
                'HLT': 'UNKNOWN',
                'HCAL': 'INCLUDED',
                'VeloPosition': 'CLOSED',
                'OT': 'INCLUDED'}
  print datataking

  context = Context(datataking, 'PRS')
  conditions = [BeamEnergyCondition(), VeloCondition(), MagneticFieldCondition(), \
                EcalCondition(), HcalCondition(), HltCondition(), ItCondition(), LoCondition(), \
              MuonCondition(), OtCondition(), Rich1Condition(), Rich2Condition(), Spd_prsCondition(), \
              TtCondition(), VeloPosition()]


  for condition in conditions:
    condition.interpret(context)

  print context.getOutput()

