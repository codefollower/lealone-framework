#!/bin/sh
mvn package -Dmaven.test.skip=true
java -cp target/${artifactId}-all-${version}.jar org.lealone.plugins.service.template.TemplateCompiler -webRoot web -targetDir target
mvn assembly:assembly -Dmaven.test.skip=true

