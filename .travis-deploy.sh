#!/usr/bin/env bash
if [[ $JAVA_HOME == *"openjdk"* ]]; then
    mvn -Ptravis-deploy-snapshot deploy --settings ./.travis.settings.xml -B -Dgpg.skip
else
    mvn verify -B -Dgpg.skip
fi
