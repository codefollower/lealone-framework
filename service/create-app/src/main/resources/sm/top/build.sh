#!/bin/sh
mvn package assembly:assembly -Dmaven.test.skip=true
