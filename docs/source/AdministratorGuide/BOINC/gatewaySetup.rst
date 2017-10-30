=============
Gateway setup
=============

********************
Certificate handling
********************

First of all, a real certificate, trusted by your real system must be present on this machine. This is the one that will be used by the services and agents running on it.
However, the untrusted world runs with self signed certificates. So we need to have our own CA.

Generate the CA certs
=====================

see https://jamielinux.com/docs/openssl-certificate-authority/index.html for detailed guide.

Work into ``/path/to/boincCertificate`` and there:
::
    mkdir -p ca/newcerts ca/certs ca/crl
    touch ca/index.txt
    echo 1000 > ca/serial
    echo 1000 > ca/crlnumber

Create the ``openssl_config_ca.cnf`` file (see below) in the ``ca/`` directory.
You might need to edit it some path in the file.

Create the CA key
*****************
::

    cd ca
    # Type one of the two command below
    openssl genrsa -aes256 -out ca.key.pem 4096    # for encrypted key
    openssl genrsa -out ca.key.pem 4096            # for unencrypted key

    chmod 400 ca.key.pem


Create the CA root certificate
******************************
::

    openssl req -config openssl_config_ca.cnf  -key ca.key.pem  -new -x509 -days 7300 -sha256 -extensions v3_ca -out ca.cert.pem

# Sure ?
#On the gateway machine, the CA certificate and key should be copied (*sym-linked*) in ``/opt/dirac/etc/grid-security/``, a copy (*sym-link*) of the CA certificate (``cacert.pem``) should also be in ``/etc/grid-security/certificates``.

On the gateway machine, the CA certificate should be copied (*sym-link*) (``ca.cert.pem``) in ``/etc/grid-security/certificates``.


Create/Renew the MrBoinc User certificate and proxy
===================================================

.. _mrBoincCert:

Then we need a user certificate (MrBoinc User) self-signed by our own CA to be used in the untrusted world to obtain a MrBoinc user proxy.

Work into ``/path/to/boincCertificate`` and there:
::

	mkdir MrBoinc

Create/use the ``openssl_config_user.cnf`` file (see below) in the ``MrBoinc/`` directory.

Create the MrBoinc User private key
***********************************

::

    openssl genrsa -out MrBoinc/userkey.pem 4096
    chmod 400 MrBoinc/userkey.pem

Generate the certificate request
********************************
::

    openssl req -config MrBoinc/openssl_config_user.cnf -key MrBoinc/userkey.pem  -new -sha256 -out MrBoinc/request.csr.pem

Create the MrBoinc User certificate, valid for 375 days
*******************************************************
::

    openssl ca -config ca/openssl_config_ca.cnf \
         -extensions server_cert -days 375 -notext -md sha256 \
         -in MrBoinc/request.csr.pem \
         -out MrBoinc/usercert.pem

The MrBoinc user certificate must be then saved on the gateway machine under ``~/.globus``.




MrBoinc Proxy generation
========================

From the gateway machine, become the dirac user, and source the bashrc. Then::

   lhcb-proxy-init \
      -o '/Systems/Framework/<your Setup>/URLs/ProxyManager=dips://<your gw machine>:9152/Framework/ProxyManager'\
      -o '/DIRAC/Configuration/Servers=dips://<your gw machine>:9135/Configuration/Server'\
      -U
   #e.g.
   lhcb-proxy-init \
    -o '/Systems/Framework/Certification/URLs/ProxyManager=dips://lbboinccertif.cern.ch:9152/Framework/ProxyManager'\
    -o '/DIRAC/Configuration/Servers=dips://lbboinccertif.cern.ch:9135/Configuration/Server'\
    -U


MrBoinc Proxy generation (DEPRECATED)
=====================================

This procedure must be done after the gateway is setup

1. From lxplus: Ban the BOINC site

   - ``lb-run LHCbDirac/prod bash --norc``
   - ``lhcb-proxy-init -g lhcb_admin``
   - ``dirac-admin-ban-site BOINC.World.org “boinc_proxy_renewal”``

2. From lbvobox46: you need to upload the new proxy in the proxydb of lbvobox46. As dirac user, do

   - ``lhcb-proxy-init -o /Systems/Framework/Production/URLs/ProxyManager="dips://lbvobox46.cern.ch:9152/Framework/ProxyManager” -o /DIRAC/Configuration/Servers="dips://lbvobox46.cern.ch:9135/Configuration/Server" -g lhcb_pilot -v 1500:00 -U``

3. Allow the site from lxplus

    - ``dirac-admin-allow-site BOINC.World.org “boinc_proxy_updated”``
