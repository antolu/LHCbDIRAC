#!/bin/bash
source /afs/cern.ch/project/gd/LCG-share/sl4/etc/profile.d/grid-env.sh
cat ~santinel/private/pas | voms-proxy-init --voms lhcb:/lhcb/Role=production -valid 144:00 -pwstdin

unset SRM_PATH

echo "-------- NEW ------ "
date
echo "Start loop over the Tier1"
echo " "
#remember to set the three variables sito (ToA sitename), endpoint and spacetoken
#------

sito=CERN-LHCb_RAW
endpoint=srm://srm-lhcb.cern.ch
sp_token=LHCb_RAW
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=CERN-LHCb_MC_M-DST
endpoint=srm://srm-lhcb.cern.ch
sp_token=LHCb_MC_M-DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=CERN-LHCb_USER
endpoint=srm://srm-lhcb.cern.ch
sp_token=LHCb_USER
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=CERN-LHCb_RDST
endpoint=srm://srm-lhcb.cern.ch
sp_token=LHCb_RDST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=CERN-LHCb_M-DST
endpoint=srm://srm-lhcb.cern.ch
sp_token=LHCb_M-DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=CERN-LHCb_FAILOVER
endpoint=srm://srm-lhcb.cern.ch
sp_token=LHCb_FAILOVER
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=CNAF-LHCb_RAW
endpoint=srm://srm-v2.cr.cnaf.infn.it
sp_token=LHCb_RAW
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=CNAF-LHCb_DST
endpoint=srm://storm-fe-lhcb.cr.cnaf.infn.it:8444
sp_token=LHCb_DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=CNAF-LHCb_RDST
endpoint=srm://srm-v2.cr.cnaf.infn.it
sp_token=LHCb_RDST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=CNAF-LHCb_M-DST
endpoint=srm://storm-fe-lhcb.cr.cnaf.infn.it:8444
sp_token=LHCb_M-DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=CNAF-LHCb_USER
endpoint=srm://storm-fe-lhcb.cr.cnaf.infn.it:8444
sp_token=LHCb_USER
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=CNAF-LHCb_FAILOVER
endpoint=srm://storm-fe-lhcb.cr.cnaf.infn.it:8444
sp_token=LHCb_FAILOVER
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=CNAF-LHCb_MC_DST
endpoint=srm://storm-fe-lhcb.cr.cnaf.infn.it:8444
sp_token=LHCb_MC_DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=CNAF-LHCb_MC_M-DST
endpoint=srm://storm-fe-lhcb.cr.cnaf.infn.it:8444
sp_token=LHCb_MC_M-DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh


sito=GRIDKA-LHCb_RAW
endpoint=srm://gridka-dCache.fzk.de
sp_token=LHCb_RAW
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=GRIDKA-LHCb_DST
endpoint=srm://gridka-dCache.fzk.de
sp_token=LHCb_DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=GRIDKA-LHCb_RDST
endpoint=srm://gridka-dCache.fzk.de
sp_token=LHCb_RDST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=GRIDKA-LHCb_M-DST
endpoint=srm://gridka-dCache.fzk.de
sp_token=LHCb_M-DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=GRIDKA-LHCb_USER
endpoint=srm://gridka-dCache.fzk.de
sp_token=LHCb_USER
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=GRIDKA-LHCb_FAILOVER
endpoint=srm://gridka-dCache.fzk.de
sp_token=LHCb_FAILOVER
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=GRIDKA-LHCb_MC_DST
endpoint=srm://gridka-dCache.fzk.de
sp_token=LHCb_MC_DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=GRIDKA-LHCb_MC_M-DST
endpoint=srm://gridka-dCache.fzk.de
sp_token=LHCb_MC_M-DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=IN2P3-LHCb_RAW
endpoint=srm://ccsrm.in2p3.fr
sp_token=LHCb_RAW
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=IN2P3-LHCb_DST
endpoint=srm://ccsrm.in2p3.fr
sp_token=LHCb_DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=IN2P3-LHCb_RDST
endpoint=srm://ccsrm.in2p3.fr
sp_token=LHCb_RDST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=IN2P3-LHCb_M-DST
endpoint=srm://ccsrm.in2p3.fr
sp_token=LHCb_M-DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=IN2P3-LHCb_USER
endpoint=srm://ccsrm.in2p3.fr
sp_token=LHCb_USER
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=IN2P3-LHCb_FAILOVER
endpoint=srm://ccsrm.in2p3.fr
sp_token=LHCb_FAILOVER
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=IN2P3-LHCb_MC_DST
endpoint=srm://ccsrm.in2p3.fr
sp_token=LHCb_MC_DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=IN2P3-LHCb_MC_M-DST
endpoint=srm://ccsrm.in2p3.fr
sp_token=LHCb_MC_M-DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=RAL-LHCb_RAW
endpoint=srm://srm-lhcb.gridpp.rl.ac.uk
sp_token=LHCb_RAW
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=RAL-LHCb_DST
endpoint=srm://srm-lhcb.gridpp.rl.ac.uk
sp_token=LHCb_DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=RAL-LHCb_RDST
endpoint=srm://srm-lhcb.gridpp.rl.ac.uk
sp_token=LHCb_RDST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=RAL-LHCb_M-DST
endpoint=srm://srm-lhcb.gridpp.rl.ac.uk
sp_token=LHCb_M-DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=RAL-LHCb_USER
endpoint=srm://srm-lhcb.gridpp.rl.ac.uk
sp_token=LHCb_USER
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=RAL-LHCb_FAILOVER
endpoint=srm://srm-lhcb.gridpp.rl.ac.uk
sp_token=LHCb_FAILOVER
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=RAL-LHCb_MC_DST
endpoint=srm://srm-lhcb.gridpp.rl.ac.uk
sp_token=LHCb_MC_DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=RAL-LHCb_MC_M-DST
endpoint=srm://srm-lhcb.gridpp.rl.ac.uk
sp_token=LHCb_MC_M-DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=SARA-LHCb_RAW
endpoint=srm://srm.grid.sara.nl
sp_token=LHCb_RAW
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=SARA-LHCb_DST
endpoint=srm://srm.grid.sara.nl
sp_token=LHCb_DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=SARA-LHCb_RDST
endpoint=srm://srm.grid.sara.nl
sp_token=LHCb_RDST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=SARA-LHCb_M-DST
endpoint=srm://srm.grid.sara.nl
sp_token=LHCb_M-DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=SARA-LHCb_USER
endpoint=srm://srm.grid.sara.nl
sp_token=LHCb_USER
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=SARA-LHCb_FAILOVER
endpoint=srm://srm.grid.sara.nl
sp_token=LHCb_FAILOVER
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=SARA-LHCb_MC_M-DST
endpoint=srm://srm.grid.sara.nl
sp_token=LHCb_MC_M-DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=SARA-LHCb_MC_DST
endpoint=srm://srm.grid.sara.nl
sp_token=LHCb_MC_DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=PIC-LHCb_RAW
endpoint=srm://srmlhcb.pic.es
sp_token=LHCb_RAW
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=PIC-LHCb_DST
endpoint=srm://srmlhcb.pic.es
sp_token=LHCb_DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=PIC-LHCb_RDST
endpoint=srm://srmlhcb.pic.es
sp_token=LHCb_RDST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=PIC-LHCb_M-DST
endpoint=srm://srmlhcb.pic.es
sp_token=LHCb_M-DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=PIC-LHCb_USER
endpoint=srm://srmlhcb.pic.es
sp_token=LHCb_USER
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=PIC-LHCb_FAILOVER
endpoint=srm://srmlhcb.pic.es
sp_token=LHCb_FAILOVER
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=PIC-LHCb_MC_DST
endpoint=srm://srmlhcb.pic.es
sp_token=LHCb_MC_DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

sito=PIC-LHCb_MC_M-DST
endpoint=srm://srmlhcb.pic.es
sp_token=LHCb_MC_M-DST
echo $sito; source /afs/cern.ch/user/s/santinel/srm_space_monitor/srm_request.sh

cd /afs/cern.ch/user/s/santinel/public/www/sls/storage_space
rm /afs/cern.ch/user/s/santinel/public/www/sls/storage_space/log
python parser_xml.py > /afs/cern.ch/user/s/santinel/public/www/sls/storage_space/log
python parse.py

