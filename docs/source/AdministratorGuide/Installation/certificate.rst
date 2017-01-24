.. _certificate_renewal:

==========================================
 Renewal of certificate for ONLINE machine
==========================================

Login as lhcbprod on **lbdirac.cern.ch** and generate the certificate request

::

  openssl req -new -subj /CN=lbdirac.cern.ch -out newcsr.csr -nodes -sha1


Open in your browser the page http://ca.cern.ch  cut the content of
*newcsr.csr* (created in the previous step) in the web page and click on
the submit button. Save the Base 64 encoded certificate as a file
*newcert.cer*. Copy this file to lbdirac.cern.ch. Then convert the
certificate in the correct format.

::

  openssl pkcs12 -export -inkey privkey.pem -in newcert.cer -out myCertificate.pks (You will have to type the PEM password you typed in the previous step. Type also an export password, and don't forget it. Your certificate in PKCS12 format is ready in file myCertificate.pks, you can delete the other files.)
  openssl pkcs12 -in myCertificate.pks -clcerts -nokeys -out hostcert.pem
  openssl pkcs12 -in myCertificate.pks -nocerts -out hostkey.pem.passwd
  openssl rsa -in hostkey.pem.passwd -out hostkey.pem (remove the password)


If you want to test that the new host certificate is valid without any password, just do

::

  dirac-proxy-init -C <cert> -K <key>
