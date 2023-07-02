#!/bin/sh

LOGFERRYDIR=/var/run/ntpmon

getent group logferry >/dev/null || groupadd -r logferry
getent passwd logferry >/dev/null || \
    useradd -r -g logferry -d ${LOGFERRYDIR} -s /sbin/nologin \
      -c "GeoDNS log shipper" logferry

mkdir -p ${LOGFERRYDIR}
chmod 700 ${LOGFERRYDIR}
chown -R logferry:logferry ${LOGFERRYDIR}

exit 0
