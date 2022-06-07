#!/bin/bash -xe

mvn clean
mvn -f pom-step1.xml package
mvn -f pom-step2.xml package
mvn -f pom-step3.xml package
mvn -f pom-step4.xml package
mvn -f pom-step5.xml package
