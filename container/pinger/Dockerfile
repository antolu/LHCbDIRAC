FROM centos:7
MAINTAINER Christophe HAEN <christophe.haen@cern.ch>


RUN yum install -y epel-release
RUN yum update -y
RUN yum install -y gcc python-pip python-devel



#pyGSI
# http://service-spi.web.cern.ch/service-spi/external/tarFiles/pygsi-0.6.3.tar.gz
RUN yum install -y openssl-devel
RUN pip install pycrypto GSI pyparsing


# Clean to make the image smaller
#RUN yum remove -y git gcc gcc-c++ make python-pip python-devel unzip -> seem to remove too much
RUN yum clean all
RUN yum autoremove -y

RUN mkdir -p /opt/dirac/etc
WORKDIR /opt/dirac/
ENV PYTHONPATH $PYTHONPATH:/opt/dirac/

ENV DIRAC_VERSION v6r16p2

RUN curl -L http://lhcbproject.web.cern.ch/lhcbproject/dist/Dirac_project/installSource/DIRAC-"$DIRAC_VERSION".tar.gz -o "$DIRAC_VERSION".tar.gz
RUN tar xvzf "$DIRAC_VERSION".tar.gz


COPY dirac_ping/ /opt/dirac/dirac_ping
ENTRYPOINT [ "python", "/opt/dirac/dirac_ping/ping_server.py"]
