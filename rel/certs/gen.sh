#!/bin/bash

/usr/bin/openssl req -new -sha256 \
    -key privkey.pem \
    -subj "/C=UA/O=LightUponCloud/CN=lightupon.cloud" \
    -reqexts SAN \
    -config <(cat /etc/ssl/openssl.cnf \
        <(printf "\n[SAN]\nsubjectAltName=DNS:the-integrationtests-integration1-res,DNS:the-dubstack-arkhitektori-res")) \
    -out domain.csr
