====
Test
====

The BOINCDirac repository is here: https://github.com/DIRACGrid/BoincDIRAC

This is *generic*, right now (04/2017) is not used by LHCb. All the code used by LHCB is within LHCbDIRAC.


- VM creation and contextualization files (lhcb_pilot, create_image): https://gitlab.cern.ch/vc/vm/tree/master/bin

- Pilot v3 (the one used in VMs) code repository: https://github.com/DIRACGrid/Pilot


Start a pilot on VM to test the BOINC gateway service:

 1.  MrBoinc Host certificate under ``/etc/grid-security`` needed (or somewhere else)

 2. Get the pilot files::


      curl --insecure -L -O https://lhcb-portal-dirac.cern.ch/pilot/pilot.json
      curl --insecure -L -O https://lhcb-portal-dirac.cern.ch/pilot/pilotTools.py
      curl --insecure -L -O https://lhcb-portal-dirac.cern.ch/pilot/pilotCommands.py
      curl --insecure -L -O https://lhcb-portal-dirac.cern.ch/pilot/LHCbPilotCommands.py
      curl --insecure -L -O https://lhcb-portal-dirac.cern.ch/pilot/dirac-pilot.py
      curl --insecure -L -O https://lhcb-portal-dirac.cern.ch/pilot/dirac-install.py

 4. Define the environment variables needed::

      export X509_CERT_DIR=/cvmfs/lhcb.cern.ch/etc/grid-security/certificates/
      export X509_VOMS_DIR=/cvmfs/lhcb.cern.ch/etc/grid-security/vomsdir


 3. start the pilot with::


       #run the dirac-pilot script
       python ./dirac-pilot.py \
        --debug \
        --setup=<YOUR SETUP (LHCb-Certification)> \
        --pilotCFGFile=pilot.json \
        -l LHCb \
        -o LbRunOnly \
        --Name=<YOUR CE NAME (BOINCCert-World-CE.org) \
        --Queue=<YOUR QUEUE NAME (BoincCert.World.Queue)> \
        --MaxCycles=1 \
        --name=<YOUR BOINC SITE (BOINCCert.World.org)> \
        --cert \
        --certLocation=<CERTIFICATE LOCATION (/etc/grid-security/) \
        --commandExtensions LHCbPilot \
        --configurationServer <YOUR BOINC CONFIG SERVER (dips://lbboinccertif.cern.ch:9135/Configuration/Server)>
