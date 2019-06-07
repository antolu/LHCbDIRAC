#!/usr/bin/env python
###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################

import random
import time

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()


from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
cl = BookkeepingClient()

runs = [164452, 164454, 164455, 164457, 164459, 164460, 164462, 164524, 164527, 164528, 164529, 164531, 164533,
        164534, 164536, 164537, 164538, 164539, 164589, 164594, 164641, 164642, 164643, 164644, 164646, 164647,
        164649, 164651, 164661, 164662, 164663, 164665, 164666, 164667, 164668, 164670, 164672, 164680, 164681,
        164683, 164684, 164686, 164688, 164689, 164692, 164694, 164695, 164696, 164697, 164698,
        164699, 164700, 164701, 164702, 164703, 164704, 164705, 164732, 164764, 164769, 164770, 164771, 164774,
        164776, 164778, 164779, 164780, 164781, 164782, 164783, 164784, 164785, 164786, 164787, 164788, 164789, 164790,
        164791, 164796, 164819, 164823, 164824, 164825, 164826, 164877, 164878, 164879, 164880, 164883, 164885, 164888,
        164908, 164911, 164914, 164916, 164917, 164918, 164920, 164921, 164923]

datasets = [{'ConfigName': 'LHCb',
             'ConditionDescription': 'Beam6500GeV-VeloClosed-MagDown',
             'EventType': '90000000',
             'FileType': 'BHADRON.MDST',
             'ProcessingPass': '/Real Data/Reco15a/Stripping24',
             'Visible': 'Y', 'fullpath':
             '/LHCb/Collision15/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco15a/Stripping24/90000000/BHADRON.MDST',
             'ConfigVersion': 'Collision15',
             'DataQuality': ['OK']}]

evt1 = [90000000, 91000000, 94000000, 95100000]
c = 0
for i in evt1:
  datasets.extend([{'ConfigName': 'LHCb',
                    'ConditionDescription': 'Beam6500GeV-VeloClosed-MagDown',
                    'EventType': i,
                    'FileType': 'RAW',
                    'ProcessingPass': '/Real Data',
                    'Visible': 'Y', 'fullpath': c,
                    'ConfigVersion': 'Collision15'}])
  c += 1


for i in evt1:
  datasets.extend([{'ConfigName': 'LHCb',
                    'ConditionDescription': 'Beam6500GeV-VeloClosed-MagDown',
                    'EventType': i,
                    'FileType': 'RAW',
                    'ProcessingPass': '/Real Data',
                    'Visible': 'Y',
                    'fullpath': '/LHCb/Collision15/Beam6500GeV-VeloClosed-MagDown/Real Data/90000000/RAW',
                    'ConfigVersion': 'Collision15em'}])

datasets.extend([{'ConfigName': 'LHCb',
                  'ConditionDescription': 'Beam6500GeV-VeloClosed-MagDown',
                  'EventType': '90000000',
                  'FileType': 'RDST',
                  'ProcessingPass': '/Real Data/Reco15',
                  'Visible': 'Y',
                  'fullpath': '/LHCb/Collision15/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco15/90000000/RDST',
                  'ConfigVersion': 'Collision15',
                  'DataQuality': [u'OK', u'UNCHECKED']}])

datasets.extend([{'ConfigName': 'LHCb',
                  'ConditionDescription': 'Beam6500GeV-VeloClosed-MagDown',
                  'EventType': '90000000',
                  'FileType': 'RDST',
                  'ProcessingPass': '/Real Data/Reco15',
                  'Visible': 'Y',
                  'fullpath': '/LHCb/Collision15/Beam6500GeV-VeloClosed-MagDown/Real Data/Reco15/90000000/RDST',
                  'ConfigVersion': 'Collision15em',
                  'DataQuality': [u'OK', u'UNCHECKED']}])

datasets.extend([{'ConfigName': 'LHCb',
                  'ConditionDescription': 'Beam6500GeV-VeloClosed-MagDown',
                  'EventType': '95100000',
                  'FileType': 'FULLTURBO.DST',
                  'ProcessingPass': '/Real Data/Reco15/Turbo01',
                  'Visible': 'Y',
                  'fullpath': '/LHCb/Collision15/Beam6500GeV-VeloClosed-MagDown/\
                  Real Data/Reco15/Turbo01/95100000/FULLTURBO.DST',
                  'ConfigVersion': 'Collision15',
                  'DataQuality': [u'OK', u'UNCHECKED']}])

datasets.extend([{'ConfigName': 'LHCb',
                  'ConditionDescription': 'Beam6500GeV-VeloClosed-MagDown',
                  'EventType': '95100000',
                  'FileType': 'FULLTURBO.DST',
                  'ProcessingPass': '/Real Data/Reco15/Turbo01em',
                  'Visible': 'Y',
                  'fullpath': '/LHCb/Collision15/Beam6500GeV-VeloClosed-MagDown/Real Data/\
                  Reco15/Turbo01/95100000/FULLTURBO.DST',
                  'ConfigVersion': 'Collision15em',
                  'DataQuality': [u'OK', u'UNCHECKED']}])


for i in ['RADIATIVE.DST', 'MDST.DST', 'BHADRON.MDST', 'MINIBIAS.DST', 'PID.MDST',
          'BHADRONCOMPLETEEVENT.DST', 'EW.DST', 'LEPTONIC.MDST', 'CALIBRATION.DST', 'CHARM.MDST',
          'CHARMCOMPLETEEVENT.DST', 'DIMUON.DST', 'SEMILEPTONIC.DST']:
  datasets.extend([{'ConfigName': 'LHCb',
                    'ConditionDescription': 'Beam6500GeV-VeloClosed-MagDown',
                    'EventType': '90000000',
                    'FileType': i,
                    'ProcessingPass': '/Real Data/Reco15/Stripping23r1',
                    'Visible': 'Y',
                    'fullpath': c,
                    'ConfigVersion': 'Collision15',
                    'DataQuality': [u'OK', u'UNCHECKED']}])
  c += 1

datasets += [{'ConfigName': 'LHCb',
              'ConditionDescription': 'Beam6500GeV-VeloClosed-MagDown',
              'EventType': '94000000',
              'FileType': 'TURBO.MDST',
              'ProcessingPass': '/Real Data/Turbo01',
              'Visible': 'Y',
              'fullpath': '/LHCb/Collision15/Beam6500GeV-VeloClosed-MagDown/Real Data/Turbo01/94000000/TURBO.MDST',
              'ConfigVersion': 'Collision15',
              'DataQuality': [u'OK', u'UNCHECKED']}]

for i in ['RADIATIVE.DST', 'MDST.DST', 'BHADRON.MDST', 'MINIBIAS.DST', 'PID.MDST', 'BHADRONCOMPLETEEVENT.DST',
          'EW.DST', 'LEPTONIC.MDST', 'CHARMTOBESWUM.DST', 'CALIBRATION.DST', 'CHARM.MDST', 'CHARMCOMPLETEEVENT.DST',
          'DIMUON.DST', 'SEMILEPTONIC.DST']:
  datasets = [{'ConfigName': 'LHCb',
               'ConditionDescription': 'Beam4000GeV-VeloClosed-MagDown',
               'EventType': '90000000',
               'FileType': i,
               'ProcessingPass': '/Real Data/Reco14/Stripping21',
               'Visible': 'Y',
               'fullpath': '/LHCb/Collision12/Beam4000GeV-VeloClosed-MagDown/\
               Real Data/Reco14/Stripping21/90000000/EW.DST',
               'ConfigVersion': 'Collision12',
               'DataQuality': [u'OK', u'UNCHECKED']}]
for i in ['RADIATIVE.DST', 'MDST.DST', 'BHADRON.MDST', 'MINIBIAS.DST', 'PID.MDST', 'BHADRONCOMPLETEEVENT.DST', 'EW.DST',
          'LEPTONIC.MDST', 'CHARMTOBESWUM.DST', 'CALIBRATION.DST', 'CHARM.MDST', 'CHARMCOMPLETEEVENT.DST',
          'DIMUON.DST', 'SEMILEPTONIC.DST']:
  datasets += [{'ConfigName': 'LHCb',
                'ConditionDescription': 'Beam4000GeV-VeloClosed-MagDown',
                'EventType': '90000000',
                'FileType': i,
                'ProcessingPass': '/Real Data/Reco14/Stripping20r0p3',
                'Visible': 'Y',
                'fullpath': '/LHCb/Collision12/Beam4000GeV-VeloClosed-MagDown\
                /Real Data/Reco14/Stripping21/90000000/EW.DST',
                'ConfigVersion': 'Collision12',
                'DataQuality': [u'OK', u'UNCHECKED']}]

for i in ['RADIATIVE.DST', 'MDST.DST', 'BHADRON.MDST', 'MINIBIAS.DST', 'PID.MDST', 'BHADRONCOMPLETEEVENT.DST',
          'EW.DST', 'LEPTONIC.MDST', 'CHARMTOBESWUM.DST', 'CALIBRATION.DST', 'CHARM.MDST',
          'CHARMCOMPLETEEVENT.DST', 'DIMUON.DST', 'SEMILEPTONIC.DST']:
  datasets += [{'ConfigName': 'LHCb',
                'ConditionDescription': 'Beam4000GeV-VeloClosed-MagDown',
                'EventType': '90000000',
                'FileType': i,
                'ProcessingPass': '/Real Data/Reco14/Stripping20r0p2',
                'Visible': 'Y',
                'fullpath': '/LHCb/Collision12/Beam4000GeV-VeloClosed-MagDown\
                /Real Data/Reco14/Stripping21/90000000/EW.DST',
                'ConfigVersion': 'Collision12',
                'DataQuality': [u'OK', u'UNCHECKED']}]

for i in ['RADIATIVE.DST', 'MDST.DST', 'BHADRON.MDST', 'MINIBIAS.DST', 'PID.MDST', 'BHADRONCOMPLETEEVENT.DST', 'EW.DST',
          'LEPTONIC.MDST', 'CHARMTOBESWUM.DST', 'CALIBRATION.DST', 'CHARM.MDST', 'CHARMCOMPLETEEVENT.DST',
          'DIMUON.DST', 'SEMILEPTONIC.DST']:
  datasets += [{'ConfigName': 'LHCb',
                'ConditionDescription': 'Beam4000GeV-VeloClosed-MagDown',
                'EventType': '90000000',
                'FileType': i,
                'ProcessingPass': '/Real Data/Reco14/Stripping20r0p1',
                'Visible': 'Y',
                'fullpath': '/LHCb/Collision12/Beam4000GeV-VeloClosed-MagDown\
                /Real Data/Reco14/Stripping21/90000000/EW.DST',
                'ConfigVersion': 'Collision12',
                'DataQuality': [u'OK', u'UNCHECKED']}]

for i in ['RADIATIVE.DST', 'MDST.DST', 'BHADRON.MDST', 'MINIBIAS.DST', 'PID.MDST', 'BHADRONCOMPLETEEVENT.DST',
          'EW.DST', 'LEPTONIC.MDST', 'CHARMTOBESWUM.DST', 'CALIBRATION.DST', 'CHARM.MDST', 'CHARMCOMPLETEEVENT.DST',
          'DIMUON.DST', 'SEMILEPTONIC.DST']:
  datasets += [{'ConfigName': 'LHCb',
                'ConditionDescription': 'Beam4000GeV-VeloClosed-MagDown',
                'EventType': '90000000',
                'FileType': i,
                'ProcessingPass': '/Real Data/Reco14/Stripping20',
                'Visible': 'Y',
                'fullpath': '/LHCb/Collision12/Beam4000GeV-VeloClosed-MagDown\
                /Real Data/Reco14/Stripping21/90000000/EW.DST',
                'ConfigVersion': 'Collision12',
                'DataQuality': [u'OK', u'UNCHECKED']}]

# 2015
evts = [18112001, 28142001, 18112021, 27462411, 12875401, 11874401, 12873441, 12143010, 13774003, 18112011, 11874091]
for i in evts:
  datasets += [{'ConfigName': 'MC',
                'ConditionDescription': 'Beam6500GeV-Jun2015-MagDown-Nu1.6-Pythia6',
                'EventType': i,
                'FileType': 'DST',
                'ProcessingPass': '/Sim08h/Trig0x40f9014e/Reco15em/Turbo01em',
                'Visible': 'Y',
                'fullpath': '/MC/2015/Beam6500GeV-Jun2015-MagDown-Nu1.6-Pythia6\
                /Sim08h/Trig0x40f9014e/Reco15em/Turbo01em/11874091/DST',
                'ConfigVersion': '2015',
                'DataQuality': [u'OK', u'UNCHECKED']}]

evts = [23103020, 11442012, 21103010, 21263002, 23263011, 27165006, 12875401, 21103003, 11874401,
        12873441, 27163003, 12143010, 21263010, 13774003, 23103011, 27265002, 27163071, 23263020, 11874091]
for i in evts:
  datasets += [{'ConfigName': 'MC',
                'ConditionDescription': 'Beam6500GeV-Jun2015-MagDown-Nu1.6-Pythia8',
                'EventType': i,
                'FileType': 'DST',
                'ProcessingPass': '/Sim08h/Trig0x40f9014e/Reco15em/Turbo01em',
                'Visible': 'Y',
                'fullpath': '/MC/2015/Beam6500GeV-Jun2015-MagDown-Nu1.6-Pythia8\
                /Sim08h/Trig0x40f9014e/Reco15em/Turbo01em/12873441/DST',
                'ConfigVersion': '2015',
                'DataQuality': [u'OK', u'UNCHECKED']}]

evts = [24142001, 18112001, 28142001, 18112021, 27462411, 12875401,
        11874401, 12873441, 12143010, 13774003, 18112011, 11874091]
for i in evts:
  datasets += [{'ConfigName': 'MC',
                'ConditionDescription': 'Beam6500GeV-Jun2015-MagUp-Nu1.6-Pythia6',
                'EventType': i,
                'FileType': 'DST',
                'ProcessingPass': '/Sim08h/Trig0x40f9014e/Reco15em/Turbo01em',
                'Visible': 'Y',
                'fullpath': '/MC/2015/Beam6500GeV-Jun2015-MagUp-Nu1.6-Pythia6\
                /Sim08h/Trig0x40f9014e/Reco15em/Turbo01em/11874091/DST',
                'ConfigVersion': '2015',
                'DataQuality': [u'OK', u'UNCHECKED']}]

evts = [11442012, 21263002, 23263011, 12875401, 11874401, 12873441,
        27163003, 12143010, 21263010, 13774003, 27265002, 23263020, 11874091]
for i in evts:
  datasets += [{'ConfigName': 'MC',
                'ConditionDescription': 'Beam6500GeV-Jun2015-MagUp-Nu1.6-Pythia8',
                'EventType': i,
                'FileType': 'DST',
                'ProcessingPass': '/Sim08h/Trig0x40f9014e/Reco15em/Turbo01em',
                'Visible': 'Y',
                'fullpath': '/MC/2015/Beam6500GeV-Jun2015-MagUp-Nu1.6-Pythia8\
                /Sim08h/Trig0x40f9014e/Reco15em/Turbo01em/12143010/DST',
                'ConfigVersion': '2015',
                'DataQuality': [u'OK', u'UNCHECKED']}]

# Upgrade
evts = [11114001, 30000000, 13104011]
for i in evts:
  datasets += [{'ConfigName': 'MC',
                'ConditionDescription': 'Beam7000GeV-Upgrade-MagDown-Nu3.8-25ns-Pythia6',
                'EventType': i,
                'FileType': 'XDST',
                'ProcessingPass': '/Sim08c/Digi13/Reco14U4',
                'Visible': 'Y',
                'fullpath': '/MC/Upgrade/Beam7000GeV-Upgrade-MagDown-Nu3.8-25ns-Pythia6\
                /Sim08c/Digi13/Reco14U4/11114001/XDST',
                'ConfigVersion': 'Upgrade',
                'DataQuality': [u'OK', u'UNCHECKED']}]
evts = [27165103, 30000000, 13102201, 13104013, 13104011, 13104012, 27165175, 13104015, 27165100]
for i in evts:
  datasets += [{'ConfigName': 'MC',
                'ConditionDescription': 'Beam7000GeV-Upgrade-MagDown-Nu3.8-25ns-Pythia8',
                'EventType': i,
                'FileType': 'XDST',
                'ProcessingPass': '/Sim08c/Digi13/Reco14U4',
                'Visible': 'Y',
                'fullpath': '/MC/Upgrade/Beam7000GeV-Upgrade-MagDown-Nu3.8-25ns-Pythia8\
                /Sim08c/Digi13/Reco14U4/13102201/XDST',
                'ConfigVersion': 'Upgrade',
                'DataQuality': [u'OK', u'UNCHECKED']}]

datasets += [{'ConfigName': 'MC',
              'ConditionDescription': 'Beam7000GeV-Upgrade-MagDown-Nu3.8-25ns-Pythia8',
              'EventType': '13104012',
              'FileType': 'XDIGI',
              'ProcessingPass': '/Sim08c/Digi13/Reco14U4/Digi13-R',
              'Visible': 'Y',
              'fullpath': '/MC/Upgrade/Beam7000GeV-Upgrade-MagDown-Nu3.8-25ns-Pythia8\
              /Sim08c/Digi13/Reco14U4/Digi13-R/13104012/XDIGI',
              'ConfigVersion': 'Upgrade',
              'DataQuality': [u'OK', u'UNCHECKED']}]

evts = [30000000, 13102201, 13104013]
for i in evts:
  datasets += [{'ConfigName': 'MC',
                'ConditionDescription': 'Beam7000GeV-Upgrade-MagDown-Nu3.8-Pythia8',
                'EventType': i,
                'FileType': 'XDST',
                'ProcessingPass': '/Sim08c/Digi13/Reco14U4',
                'Visible': 'Y',
                'fullpath': '/MC/Upgrade/Beam7000GeV-Upgrade-MagDown-Nu3.8-Pythia8\
                /Sim08c/Digi13/Reco14U4/13102201/XDST',
                'ConfigVersion': 'Upgrade',
                'DataQuality': [u'OK', u'UNCHECKED']}]

evts = [13784200, 11124001]
for i in evts:
  datasets += [{'ConfigName': 'MC',
                'ConditionDescription': 'Beam7000GeV-Upgrade-MagUp-Nu3.8-25ns-Pythia8',
                'EventType': i,
                'FileType': 'XDST',
                'ProcessingPass': '/Sim08c-NoRichSpill/Digi13/Reco14U5',
                'Visible': 'Y',
                'fullpath': '/MC/Upgrade/Beam7000GeV-Upgrade-MagUp-Nu3.8-25ns-Pythia8\
                /Sim08c-NoRichSpill/Digi13/Reco14U5/11124001/XDST',
                'ConfigVersion': 'Upgrade',
                'DataQuality': [u'OK', u'UNCHECKED']}]

# 2010
datasets += [{'ConfigName': 'MC',
              'ConditionDescription': 'Beam3500GeV-May2010-MagDown-Fix1',
              'EventType': '30000000',
              'FileType': 'DST',
              'ProcessingPass': '/Sim01/Trig0x002e002aFlagged/Reco08-MINBIAS',
              'Visible': 'Y',
              'fullpath': '/MC/2010/Beam3500GeV-May2010-MagDown-Fix1\
              /Sim01/Trig0x002e002aFlagged/Reco08-MINBIAS/30000000/DST',
              'ConfigVersion': '2010',
              'DataQuality': [u'OK', u'UNCHECKED']}]
evts = [60001008, 60002008, 60001001, 60002001]
for i in evts:
  datasets += [{'ConfigName': 'MC',
                'ConditionDescription': 'Beam3500GeV-May2010-MagDown-Fix1-Hijing',
                'EventType': i,
                'FileType': 'DST',
                'ProcessingPass': '/Sim01/Reco08',
                'Visible': 'Y',
                'fullpath': '/MC/2010/Beam3500GeV-May2010-MagDown-Fix1-Hijing/Sim01/Reco08/60001001/DST',
                'ConfigVersion': '2010',
                'DataQuality': [u'OK', u'UNCHECKED']}]

datasets += [{'ConfigName': 'MC',
              'ConditionDescription': 'Beam3500GeV-May2010-MagOff-Fix1',
              'EventType': '30000000',
              'FileType': 'DST',
              'ProcessingPass': '/Sim01/Reco08',
              'Visible': 'Y',
              'fullpath': '/MC/2010/Beam3500GeV-May2010-MagOff-Fix1/Sim01/Reco08/30000000/DST',
              'ConfigVersion': '2010',
              'DataQuality': [u'OK', u'UNCHECKED']}]

evts = [60001001, 60001008, 60002008, 60002001]
for i in evts:
  datasets += [{'ConfigName': 'MC',
                'ConditionDescription': 'Beam3500GeV-May2010-MagUp-Fix1-Hijing',
                'EventType': i,
                'FileType': 'DST',
                'ProcessingPass': '/Sim01/Reco08',
                'Visible': 'Y',
                'fullpath': '/MC/2010/Beam3500GeV-May2010-MagUp-Fix1-Hijing/Sim01/Reco08/60001001/DST',
                'ConfigVersion': '2010',
                'DataQuality': [u'OK', u'UNCHECKED']}]

evts += [12165122, 15144100, 13102002, 10012004, 24142001, 42122001, 30000000, 11144103, 10000000, 23573003,
         11166141, 42100001, 12365521, 27183000, 12165102, 11144001, 20000000, 11166131, 12365501, 13112001,
         20072000, 11960000, 13960000, 27572001, 12143001, 21573001, 11114005, 11102003, 42321000, 10012007,
         11102013, 13144002, 13296000, 42300000, 27573001, 13102013, 13514001, 11112001, 10012005, 11166121,
         11166501, 12365511, 12960000, 12165511, 15960000]
for i in evts:
  datasets += [{'ConfigName': 'MC',
                'ConditionDescription': 'Beam3500GeV-Oct2010-MagDown-Nu2.5',
                'EventType': i,
                'FileType': 'ALLSTREAMS.DST',
                'ProcessingPass': '/Sim01/Trig0x002e002aFlagged/Reco08/Stripping12Flagged',
                'Visible': 'Y',
                'fullpath': '/MC/2010/Beam3500GeV-Oct2010-MagDown-Nu2.5\
                /Sim01/Trig0x002e002aFlagged/Reco08/Stripping12Flagged/11166121/ALLSTREAMS.DST',
                'ConfigVersion': '2010',
                'DataQuality': [u'OK', u'UNCHECKED']}]

evts = [12165021, 12165011, 10000020]
for i in evts:
  datasets += [{'ConfigName': 'MC',
                'ConditionDescription': 'Beam3500GeV-Oct2010-MagDown-Nu2.5',
                'EventType': i,
                'FileType': 'ALLSTREAMS.DST',
                'ProcessingPass': '/Sim01a/Trig0x002e002aFlagged/Reco08/Stripping12Flagged',
                'Visible': 'Y',
                'fullpath': '/MC/2010/Beam3500GeV-Oct2010-MagDown-Nu2.5\
                /Sim01a/Trig0x002e002aFlagged/Reco08/Stripping12Flagged/10000020/ALLSTREAMS.DST',
                'ConfigVersion': '2010',
                'DataQuality': [u'OK', u'UNCHECKED']}]

evts += [12165122, 15144100, 13102002, 10012004, 24142001, 42122001, 30000000, 11144103, 10000000, 23573003, 11166141,
         42100001, 12365521, 27183000, 12165102, 11144001, 20000000, 11166131, 12365501, 13112001, 20072000, 11960000,
         13960000, 27572001, 12143001, 21573001, 11114005, 11102003, 42321000, 10012007, 11102013, 13144002, 13296000,
         42300000, 27573001, 13102013, 13514001, 11112001, 10012005, 11166121, 11166501, 12365511, 12960000, 12165511,
         15960000]
for i in evts:
  datasets += [{'ConfigName': 'MC',
                'ConditionDescription': 'Beam3500GeV-Oct2010-MagUp-Nu2.5',
                'EventType': i,
                'FileType': 'ALLSTREAMS.DST',
                'ProcessingPass': '/Sim01/Trig0x002e002aFlagged/Reco08/Stripping12Flagged',
                'Visible': 'Y',
                'fullpath': '/MC/2010/Beam3500GeV-Oct2010-MagUp-Nu2.5\
                /Sim01/Trig0x002e002aFlagged/Reco08/Stripping12Flagged/10000000/ALLSTREAMS.DST',
                'ConfigVersion': '2010',
                'DataQuality': [u'OK', u'UNCHECKED']}]

datasets.extend([{'ConfigName': 'LHCb',
                  'ConditionDescription': 'Beam6500GeV-VeloClosed-MagDown',
                  'EventType': 90000000,
                  'FileType': 'RAW',
                  'ProcessingPass': '/Real Data',
                  'Visible': 'Y',
                  'fullpath': '/LHCb/Collision15/Beam6500GeV-VeloClosed-MagDown/Real Data/90000000/RAW',
                  'ConfigVersion': 'Collision15'}])

datasets.extend([{'Visible': 'No',
                  'ConfigName': 'MC',
                  'ConditionDescription': 'Beam3500GeV-2011-MagUp-Nu2-Pythia8',
                  'EventType': ['11104041', '11104051', '11104114', '11104141',
                                '11104161', '11104341', '12103034', '12103044',
                                '12103054', '12103331', '12103500', '12133041',
                                '13104151', '13304104', '15104001', '15104141', '22104112',
                                '41900003', '42112050', '42112051', '42112100', '42511100'],
                  'FileType': 'ALLSTREAMS.DST',
                  'ConfigVersion': '2011',
                  'ProcessingPass': '/Sim08a/Digi13/Trig0x40760037/Reco14a/Stripping20r1NoPrescalingFlagged',
                  'SimulationConditions': 'Beam3500GeV-2011-MagUp-Nu2-Pythia8'}])


class Transaction(object):

  def __init__(self):
    self.custom_timers = {}

  def run(self):
    print len(datasets)
    i = random.randint(0, len(datasets) - 1)
    dataset = datasets[i]
    start_time = time.time()
    if 'ProcessingPass' in dataset and dataset['ProcessingPass'] == '/Real Data':
      retVal = cl.getFilesSummary(dataset)
    else:
      retVal = cl.getFilesWithMetadata(dataset)
    if not retVal['OK']:
      print retVal['Message']
    end_time = time.time()
    query_time = end_time - start_time
    if query_time > 10:
      self.custom_timers['LongQueries'] = query_time
    self.custom_timers['Bkk_ResponseTime'] = query_time
    q = dataset.get('fullpath', 0)
    if q and q in xrange(0, c):
      print 'Query-%s' % q
      self.custom_timers["Query-%s" % q] = query_time


if __name__ == '__main__':
  trans = Transaction()
  trans.run()
  print trans.custom_timers
