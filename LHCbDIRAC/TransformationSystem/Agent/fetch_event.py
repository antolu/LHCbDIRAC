#!/usr/bin/env python
import os
import sys
EVENT_INDEX_DIR = "/var/www/grid-collector/src"
sys.path.append(EVENT_INDEX_DIR)
from LHCbConfig import ApplicationMgr, INFO, InputCopyStream
import GaudiPython

from config import STATUS_RUNNING, STATUS_DONE
from request import Request


# def lfns2pfns(LFNs, SE="CERN"):
#     """
#     converts list or single LFN into list of PFNs
#     (NB even if LFNs is a string, will return list)
#     """
#     j = 0
#     lfn2pfn_map = {}
#     cmd_args = ["dirac-dms-lfn-replicas", ]
#     if type(LFNs) == str:
#         LFNs = [LFNs, ]
#     cmd_args.extend(LFNs)
#     cmd_args.extend(["|", "grep", SE, "|", "awk", "'{print $3}'"])
#     cmd = " ".join(cmd_args)
#     p = subp.Popen(cmd, shell=True, stdout=subp.PIPE, stderr=subp.PIPE)
#     stdout, stderr = p.communicate()
#     if len(stderr) > 0:
#         print "ERROR:", stderr
#     pfns = stdout.strip().split('\n')
#     for i, lfn in enumerate(sorted(LFNs)):
#         if lfn in lfn2pfn_map:
#             continue
#         lfn2pfn_map[lfn] = pfns[j] if len(pfns[j]) > 0 else None
#         j += 1
#     return lfn2pfn_map


# def test_lfn2pfn():
#     etalon_map = {
#         "LFN:/lhcb/LHCb/Collision12/DIMUON.DST/00020350/0000/00020350_00002887_1.dimuon.dst":
#         "srm://srm-eoslhcb.cern.ch/eos/lhcb/LHCb/Collision12/DIMUON.DST/00020350/0000/00020350_00002887_1.dimuon.dst",
#         "LFN:/lhcb/LHCb/Collision12/DIMUON.DST/00020350/0000/00020350_00005981_1.dimuon.dst":
#         "srm://srm-eoslhcb.cern.ch/eos/lhcb/LHCb/Collision12/DIMUON.DST/00020350/0000/00020350_00005981_1.dimuon.dst"
#     }

#     for GARBAGE_LFN in ["non_existent", "space separated"]:
#         pfns = lfns2pfns(GARBAGE_LFN)
#         assert len(pfns) == 1, "invalid len: %d" % len(pfns)
#         assert GARBAGE_LFN in pfns, "no key %s" % GARBAGE_LFN
#         assert pfns[GARBAGE_LFN] is None, "wrong result (not None): %s" % pfns[GARBAGE_LFN]

#     pfns = lfns2pfns(etalon_map.keys())
#     assert len(pfns) == 2, "len is not 2: %d" % len(pfns)

#     for lfn, pfn in pfns.iteritems():
#         assert pfn == etalon_map[lfn], "etalon mismatch (%s, %s)" % (lfn, pfn)

#     LFN0 = etalon_map.keys()[0]
#     pfns = lfns2pfns(LFN0)
#     assert len(pfns) == 1, "len of pfns is not 1: %s" % pfns
#     assert pfns[LFN0] == etalon_map[LFN0], "not equal to etalon: %s" % pfns

#     pfns = lfns2pfns([LFN0, LFN0])
#     assert len(pfns) == 1, "len is not 1: %d" % len(pfns)
#     assert LFN0 in pfns, "cannot find key: %s" % LFN0
#     assert pfns[LFN0] == etalon_map[LFN0], "etalon mismatch (%s, %s)" % (etalon_map[LFN0], pfns[LFN0])


def process_pfn(sel, evt, appMgr, PFN, positions):
    assert PFN is not None, "PFN is None"
    print "process_PFN: %s" % PFN
    sel.open([PFN])

    cur_pos = 0
    rv = 0
    for pos in map(int, positions):
        offset = pos - cur_pos - 1
        assert offset >= 0, "wrong offset: %d" % offset
        if offset > 0:
            appMgr.run(offset)
            cur_pos += offset
        appMgr.run(1)
        cur_pos += 1
        if evt['Rec/Header']:
            print 'write event', evt['Rec/Header']
            appMgr.algorithm('InputCopyStream').execute()
        else:
            rv = 1
            print "no event header (%d)" % pos
    return rv


def fetch_event(pfn_request_list, output_file):
    rv = 0
    InputCopyStream().Output = \
        "DATAFILE='PFN:%s' TYP='POOL_ROOTTREE' OPT='REC' " % output_file
    appConf = ApplicationMgr(OutputLevel=INFO, AppName='fetch_event')
    appConf.OutStream = [InputCopyStream()]
    appMgr = GaudiPython.AppMgr()
    sel = appMgr.evtsel()
    evt = appMgr.evtsvc()
    appMgr.algorithm('InputCopyStream').Enable = False

    for PFN, positions in pfn_request_list:
        rv += process_pfn(sel, evt, appMgr, PFN, positions)
    return rv


def init():
    if len(sys.argv) < 3:
        print "Usage: fetch_event.py request_file output_file"
        exit(1)
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    assert os.path.exists(input_file), "no file: " + input_file
    print "EventFetch: %s -> %s" % (input_file, output_file)
    return input_file, output_file


def main():
    input_file, output_file = init()
    request = Request(req_file=input_file)
    print request
    assert request.pfn_req_list is not None, "empty PFN request list"
    return fetch_event(request.pfn_req_list, output_file)


if __name__ == '__main__':
    main()
