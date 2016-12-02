FROM hepsw/cc7-base
MAINTAINER Christophe HAEN <christophe.haen@cern.ch>

RUN mkdir -p /opt/dirac/etc

# Copy the self pinging script
COPY dirac_self_ping.py /opt/dirac/

WORKDIR /opt/dirac

# Create wrapper scripts that parse the environment before executing the actual command
RUN  echo -e "#!/bin/bash\nsource /opt/dirac/bashrc\n\"\$@\"\n" > /usr/local/bin/dirac-wrp && chmod 755 /usr/local/bin/dirac-wrp
#RUN for comp in "service" "agent" "executor"; do echo -e "#!/bin/bash\nsource /opt/dirac/bashrc\n/opt/dirac/scripts/dirac-$comp \"\$@\"\n" > /usr/local/bin/dirac-"$comp"-wrp && chmod 755  /usr/local/bin/dirac-"$comp"-wrp; done

COPY dockerEntrypoint.sh /opt/dirac/dockerEntrypoint.sh
RUN chmod 755 /opt/dirac/dockerEntrypoint.sh
ENTRYPOINT [ "/opt/dirac/dockerEntrypoint.sh" ]

# Specify the version
ENV LHCB_DIRAC_VERSION $$LHCB_DIRAC_VERSION$$

RUN curl -L -o dirac-install https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/Core/scripts/dirac-install.py && chmod +x dirac-install && ./dirac-install -r $LHCB_DIRAC_VERSION -l LHCb -e LHCb -t server -i 27 && rm -rf /opt/dirac/.installCache

# Copy the script so that when loging interactively the environment is correct
RUN cp /opt/dirac/bashrc /root/.bashrc


