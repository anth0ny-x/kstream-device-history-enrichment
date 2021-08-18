#!/bin/bash

# mvn install

mvn package 

java -cp target/kafka-streams-examples-0.0.1-SNAPSHOT.jar:target/lib/* org.anthony.kstream.WordCountLambdaExample