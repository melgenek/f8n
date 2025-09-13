#!/bin/sh
### CA ###

# CA private key
openssl genpkey -algorithm RSA -out ca-private-key.pem

# CA self-signed certificate:
openssl req -x509 -new -nodes -key ca-private-key.pem -sha256 -days 3650 -out ca-certificate.pem -subj "/C=US/ST=State/L=City/O=Organization/OU=Organizational Unit/CN=Common Name/emailAddress=email@example.com"
### End CA ###

### Server ###
# Server private key:
openssl genpkey -algorithm RSA -out server-private-key.pem -aes256 -pass pass:server_password

# Server certificate signing request (CSR):
openssl req -new -key server-private-key.pem -passin pass:server_password -out server.csr -config openssl-server.cnf -subj "/C=US/ST=California/L=San Francisco/O=My Organization/OU=IT/CN=example.com"

# Server certificate signed by the CA:
openssl x509 -req -in server.csr \
    -CA ca-certificate.pem -CAkey ca-private-key.pem \
    -out server-certificate.pem -days 3650 -sha256 \
    -copy_extensions copyall

rm server.csr
### End Server ###


### Client ###
# Client private key:
openssl genpkey -algorithm RSA -out client-private-key.pem -aes256 -pass pass:client_password

# Client certificate signing request (CSR):
openssl req -new -key client-private-key.pem -passin pass:client_password -out client.csr -config openssl-client.cnf -subj "/C=US/ST=California/L=San Francisco/O=My Organization/OU=IT/CN=example.com"

# Client certificate signed by the CA:
openssl x509 -req -in client.csr \
    -CA ca-certificate.pem -CAkey ca-private-key.pem \
    -out client-certificate.pem -days 3650 -sha256 \
    -copy_extensions copyall

rm client.csr
### End Client ###
