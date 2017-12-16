#!/usr/bin/env bash

export ORG="bluelens"
export IMAGE="bl-crawler"
export TAG='prod'
export NAMESPACE="prod"

docker login

docker build -t $IMAGE:$TAG .
docker tag $IMAGE:$TAG $ORG/$IMAGE:$TAG
docker push $ORG/$IMAGE:$TAG
