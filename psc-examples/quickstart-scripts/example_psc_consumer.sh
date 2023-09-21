#!/bin/bash
DIR=$(dirname "${BASH_SOURCE[0]}")
cd $DIR

DIR_ABS=$(pwd)

java -DtempServersetDir=$DIR_ABS/configs/ -cp ../target/psc-examples-*.jar com.pinterest.psc.example.kafka.ExamplePscConsumer plaintext:/rn:kafka:dev:local-cloud_local-region::local-cluster:my_test_topic