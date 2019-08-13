#!/usr/bin/env bash
if [[ $JAVA_HOME = *"java-7-openjdk"* && $TRAVIS_PULL_REQUEST != "false" ]]; then
    echo "SNAPSHOT DEPLOYING"
    mvn -Ptravis-deploy-snapshot deploy --settings ./.travis.settings.xml -B -Dgpg.skip
else
    echo "SKIP DEPLOYING"
    mvn verify -B -Dgpg.skip
fi
