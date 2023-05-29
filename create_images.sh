#!/bin/bash
./mvnw package -PnativeTest -Dquarkus.native.container-build=true -Dquarkus.container-image.build=true -Dskip.tests
TAG="$1"
docker build -f src/main/docker/Dockerfile.native -t ${TAG}-test .
./mvnw package -Pnative -Dquarkus.native.container-build=true -Dquarkus.container-image.build=true -Dskip.tests
docker build -f src/main/docker/Dockerfile.native -t ${TAG} .
echo "Test image without kubernetes is ${TAG}-test"
echo "Production image with kubernetes is ${TAG}"