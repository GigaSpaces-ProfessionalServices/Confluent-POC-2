#!/bin/bash

# setting a locstor let your access a specific host for looking up the space.
# export GS_LOOKUP_LOCATORS=127.0.0.1:4174
mvn exec:java -f ./../pom.xml -Dexec.mainClass=com.gigaspaces.demo.kstreams.app.DataGridTest -Dexec.args="$@"
