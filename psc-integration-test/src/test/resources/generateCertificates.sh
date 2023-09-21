#!/bin/bash

KEYSTORE_PATH="$1"
TRUSTSTORE_PATH="$2"
CLIENT_KEYSTORE_PATH="$3"
CLIENT_TRUSTSTORE_PATH="$4"
CA_CERT_PATH="$5"
CA_KEY_PATH="$6"
CERT_FILE_PATH="$7"
CERT_FILE_SIGNED_PATH="$8"
DIRECTORY="$9"
PASSWORD="${10}"
ROTATE="${11}"
LOCALHOST_ALIAS="localhost"
CA_ALIAS="ca"

mkdir -p $DIRECTORY
cd $DIRECTORY
mkdir -p "$(dirname "$KEYSTORE_PATH")"
mkdir -p "$(dirname "$TRUSTSTORE_PATH")"
mkdir -p "$(dirname "$CLIENT_KEYSTORE_PATH")"
mkdir -p "$(dirname "$CLIENT_TRUSTSTORE_PATH")"

echo "keystore path: $KEYSTORE_PATH"
echo "truststore path: $TRUSTSTORE_PATH"
echo "client keystore path: $CLIENT_KEYSTORE_PATH"
echo "client truststore path: $CLIENT_TRUSTSTORE_PATH"

if [ "$ROTATE" = "rotate" ]; then

##########################################################################
# Here, we are replacing the CA and host certs signed by the old CA from #
# the client keystore. We then rotate the CA by creating a new one, and  #
# use that CA to sign the new client certs and tell the client to trust  #
# this CA instead. Nothing is done to the broker keystore and truststore #
##########################################################################

  echo "rotating client certs"

  keytool -delete -alias $CA_ALIAS -keystore $CLIENT_KEYSTORE_PATH -storepass $PASSWORD
  keytool -delete -alias $LOCALHOST_ALIAS -keystore $CLIENT_KEYSTORE_PATH -storepass $PASSWORD

  echo "creating new Certificate Authority"
  openssl req -new -days 365 -x509 -subj "/CN=localhost" -keyout $CA_KEY_PATH -out $CA_CERT_PATH -passout pass:$PASSWORD

  echo "CA cert contents:"
  openssl x509 -text -noout -in $CA_CERT_PATH

  echo "generate keys and cert for client:"
  keytool -genkey -keystore $CLIENT_KEYSTORE_PATH -alias $LOCALHOST_ALIAS -validity 365 -keyalg RSA -storepass $PASSWORD -keypass $PASSWORD -dname "CN=localhost" -ext san=dns:localhost

  echo "client keystore contents:"
  keytool -list -v -keystore $CLIENT_KEYSTORE_PATH -storepass $PASSWORD

  echo "create certificate signing request:"
  keytool -certreq -keystore $CLIENT_KEYSTORE_PATH -alias $LOCALHOST_ALIAS -file $CERT_FILE_PATH -storepass $PASSWORD

  echo "sign with CA:"
  openssl x509 -req -CA $CA_CERT_PATH -CAkey $CA_KEY_PATH -in $CERT_FILE_PATH -out $CERT_FILE_SIGNED_PATH -days 365 -CAcreateserial -passin pass:$PASSWORD
#
  echo "import CA cert into client keystore:"
  keytool -keystore $CLIENT_KEYSTORE_PATH -import -alias $CA_ALIAS -file $CA_CERT_PATH -storepass $PASSWORD -keypass $PASSWORD -noprompt

  echo "import signed cert into client keystore:"
  keytool -import -file $CERT_FILE_SIGNED_PATH -keystore $CLIENT_KEYSTORE_PATH -alias $LOCALHOST_ALIAS -storepass $PASSWORD -noprompt

  echo "client keystore contents:"
  keytool -list -v -keystore $KEYSTORE_PATH -storepass $PASSWORD

  echo "importing CA cert to client truststore:"
  keytool -import -file $CA_CERT_PATH -keystore $CLIENT_TRUSTSTORE_PATH -alias $CA_ALIAS -storepass $PASSWORD -noprompt
else

##########################################################################
# Here, we create brand new client and broker keystore and truststores   #
# to establish a successful SSL connection between the two               #
##########################################################################

  echo "creating certs"

  echo "creating new Certificate Authority"
  openssl req -new -days 365 -x509 -subj "/CN=localhost" -keyout $CA_KEY_PATH -out $CA_CERT_PATH -passout pass:$PASSWORD

  echo "CA cert contents:"
  openssl x509 -text -noout -in $CA_CERT_PATH

  echo "generate keys and cert for broker:"
  keytool -genkey -keystore $KEYSTORE_PATH -alias $LOCALHOST_ALIAS -validity 365 -keyalg RSA -storepass $PASSWORD -keypass $PASSWORD -dname "CN=localhost" -ext san=dns:localhost

  echo "broker keystore contents:"
  keytool -list -v -keystore $KEYSTORE_PATH -storepass $PASSWORD

  echo "create certificate signing request on broker:"
  keytool -certreq -keystore $KEYSTORE_PATH -alias $LOCALHOST_ALIAS -file $CERT_FILE_PATH -storepass $PASSWORD

  echo "sign with CA:"
  openssl x509 -req -CA $CA_CERT_PATH -CAkey $CA_KEY_PATH -in $CERT_FILE_PATH -out $CERT_FILE_SIGNED_PATH -days 365 -CAcreateserial -passin pass:$PASSWORD

  echo "import CA cert into broker keystore:"
  keytool -keystore $KEYSTORE_PATH -import -alias $CA_ALIAS -file $CA_CERT_PATH -storepass $PASSWORD -keypass $PASSWORD -noprompt

  echo "import CA cert into client keystore:"
  keytool -keystore $CLIENT_KEYSTORE_PATH -import -alias $CA_ALIAS -file $CA_CERT_PATH -storepass $PASSWORD -keypass $PASSWORD -noprompt

  echo "import signed cert into broker keystore:"
  keytool -import -file $CERT_FILE_SIGNED_PATH -keystore $KEYSTORE_PATH -alias $LOCALHOST_ALIAS -storepass $PASSWORD -noprompt

  echo "import signed cert into client keystore:"
  keytool -import -file $CERT_FILE_SIGNED_PATH -keystore $CLIENT_KEYSTORE_PATH -alias $LOCALHOST_ALIAS -storepass $PASSWORD -noprompt

  echo "broker keystore contents:"
  keytool -list -v -keystore $KEYSTORE_PATH -storepass $PASSWORD

  echo "client keystore contents:"
  keytool -list -v -keystore $CLIENT_KEYSTORE_PATH -storepass $PASSWORD

  echo "importing CA cert to client truststore:"
  keytool -import -file $CA_CERT_PATH -keystore $CLIENT_TRUSTSTORE_PATH -alias $CA_ALIAS -storepass $PASSWORD -noprompt

  echo "importing CA cert to broker truststore:"
  keytool -import -file $CA_CERT_PATH -keystore $TRUSTSTORE_PATH -alias $CA_ALIAS -storepass $PASSWORD -noprompt

  echo "client truststore contents:"
  keytool -list -v -keystore $CLIENT_TRUSTSTORE_PATH -storepass $PASSWORD
fi
