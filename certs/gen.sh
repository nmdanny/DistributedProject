#!/usr/bin/env bash

# based on https://gist.github.com/cecilemuller/9492b848eb8fe46d462abeb26656c4f8

openssl req -x509 -new -sha256 -days 1024 \
		-newkey rsa:2048 -nodes -keyout ca.key -out ca.pem \
	    -subj "/C=IL/CN=localhost" \
		-addext 'subjectAltName = DNS:localhost'

num_nodes=5
starting_port=18200
mkdir tls-server -p

for ((i=0; i < num_nodes; i++))
do
	port=$((starting_port + i))
	openssl req -new -subj "/C=IL/CN=localhost" \
		-addext 'subjectAltName = DNS:localhost' -extensions v3_req \
		-newkey rsa:2048 -nodes -keyout "tls-server/server$i.key" \
		-out "tls-server/server$i.csr"
	openssl x509 -req -sha256 -days 1024 \
		-extfile v3.ext \
		-in "tls-server/server$i.csr" -CA ca.pem -CAkey ca.key -CAcreateserial \
		-out "tls-server/server$i.pem"
done
