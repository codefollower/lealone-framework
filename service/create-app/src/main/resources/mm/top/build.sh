#!/bin/sh
mvn package -Dmaven.test.skip=true
java -cp ${appName}-dist/target/${artifactId}-${version}.jar org.lealone.plugins.service.template.TemplateCompiler -webRoot ${appName}-web/web -targetDir ${appName}-dist/target
mvn assembly:assembly -Dmaven.test.skip=true

