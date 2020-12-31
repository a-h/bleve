#!/bin/sh
if ! [ -f ./DynamoDBLocal.jar ]; then
  curl https://s3.eu-central-1.amazonaws.com/dynamodb-local-frankfurt/dynamodb_local_latest.zip -o d.zip
  unzip d.zip
  rm d.zip
fi
mkdir -p data
nix-shell --packages jdk11 --run "java -jar DynamoDBLocal.jar -sharedDb -dbPath ./data"
