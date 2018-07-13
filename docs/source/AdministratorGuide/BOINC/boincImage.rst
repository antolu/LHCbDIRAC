BOINC Image
===========


The client side interaction is handled by CERN IT (Laurence Field atm).
I am not sure how their whole system works, but the main point is that the client ends up booting an image:
* contextualized by a user_data file which basically just specify the cvmfs path to use, and contain the MrBoincHost certificate and key (see below)
* Start a pilot bootstrap script available here: https://gitlab.cern.ch/vc/vm/raw/master/bin/lhcb-pilot




Generate the MrBoinc host certificate
=====================================

This certificate/key needs to be given to IT to add in the contextualization file.

Work into ``/path/to/boincCertificate`` and there:
::
    mkdir MrBoincHost

Create the ``openssl_config_host.cnf`` file in the ``MrBoincHost/`` directory.
::
    openssl genrsa -out MrBoincHost/hostkey.pem 4096
    chmod 400 MrBoincHost/hostkey.pem

::

    openssl req -config MrBoincHost/openssl_config_host.cnf -key MrBoincHost/hostkey.pem  -new -out MrBoincHost/request.csr.pem

::

    openssl ca -config ca/openssl_config_ca.cnf \
         -extensions server_cert \
         -in MrBoincHost/request.csr.pem \
         -out MrBoincHost/hostcert.pem


This self-signed host certificate (MrBoinc Host) must then be saved on any BOINC VM in ``/etc/grid-security``.
Do not forget to add it to the list of trusted host for ProxyDelegation in the dirac.cfg of the gateway
