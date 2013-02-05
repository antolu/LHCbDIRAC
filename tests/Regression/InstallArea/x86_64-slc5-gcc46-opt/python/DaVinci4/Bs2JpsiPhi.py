
# limit what we get with from Bs2JpsiPhi import *
__all__ = ('name',
           'Phi2KK',
           'LooseJpsi2MuMu',
           'Jpsi2MuMu',
           'Bs2Jpsi',
           'SeqBs2JpsiPhi')

import GaudiKernel.SystemOfUnits as Units
from Gaudi.Configuration import *
from GaudiConfUtils import ConfigurableGenerators
from Configurables import FilterDesktop, CombineParticles
from PhysSelPython.Wrappers import Selection, SelectionSequence
from StandardParticles import StdLooseMuons, StdLooseKaons

# J/Psi configurable
_jpsi2mumu = ConfigurableGenerators.CombineParticles()
_jpsi2mumu.DecayDescriptor = "J/psi(1S) -> mu+ mu-"
_jpsi2mumu.CombinationCut = "ADAMASS('J/psi(1S)')<350*MeV"
_jpsi2mumu.MotherCut = "(VFASPF(VCHI2/VDOF)<10)"

# J/Psi -> MuMu Selection
LooseJpsi2MuMu = Selection("SelLooseJpsi2MuMu", 
                           Algorithm = _jpsi2mumu, 
                           RequiredSelections = [StdLooseMuons])

# Tighter J/Psi -> MuMu filter
_jpsifilter = ConfigurableGenerators.FilterDesktop()
_jpsifilter.Code = "(PT>1*GeV) & (P>3*GeV)"

# Tighter J/Psi -> MuMu Selection
Jpsi2MuMu = Selection("SelJpsi2MuMu",
                      Algorithm = _jpsifilter,
                      RequiredSelections = [LooseJpsi2MuMu])

# Phi configurable
_phi2kk = ConfigurableGenerators.CombineParticles()
_phi2kk.DecayDescriptor  =  "phi(1020) -> K+ K-"
_phi2kk.CombinationCut = "ADAMASS('phi(1020)')<150*MeV"
_phi2kk.MotherCut = "(VFASPF(VCHI2/VDOF)<64)"

# Phi -> KK Selection
Phi2KK = Selection("SelPhi2KK",
                   Algorithm = _phi2kk,
                   RequiredSelections = [StdLooseKaons])

# Bs configurable
_bs2jpsiphi = CombineParticles("Bs2JpsiPhi",
                               DecayDescriptor = "B_s0 -> phi(1020) J/psi(1S)",
                               CombinationCut = "ADAMASS('B_s0')<300*MeV",
                               MotherCut = "(VFASPF(VCHI2/VDOF)<10) & (BPVIPCHI2()<64)" )

# Now let's add some plots
from Configurables import LoKi__Hybrid__PlotTool as PlotTool
_bs2jpsiphi.HistoProduce = True
_bs2jpsiphi.addTool( PlotTool("DaughtersPlots") )
# Note that it's using the same functors as above. Hence the same syntax. 
_bs2jpsiphi.DaughtersPlots.Histos = { "P/1000"  : ('momentum_%1%',0,100,500) ,
                                      "PT/1000" : ('p_transverse_%1% ^^',0,5,500) ,
                                      "M"       : ('mass in MeV_%1%_%2%_%3%',0.8*Units.GeV,4*Units.GeV) }

_bs2jpsiphi.addTool( PlotTool("MotherPlots") )
_bs2jpsiphi.MotherPlots.Histos = { "P/1000"  : ('momentum_%1%',0,100) ,
                                   "PT/1000" : ('p_transverse_%1%',0,5,500) ,
                                   "M"       : ('mass in MeV_%1%_%2%_%3%',4*Units.GeV,6*Units.GeV) }

# Bs -> J/Psi Phi Selection
Bs2JpsiPhi = Selection("SelBs2JpsiPhi", 
                       Algorithm = _bs2jpsiphi, 
                       RequiredSelections = [ Jpsi2MuMu, Phi2KK ])

# finally, the Bs -> J/Psi Phi SelectionSequence
SeqBs2JpsiPhi = SelectionSequence("SeqBs2JpsiPhi", TopSelection = Bs2JpsiPhi)
