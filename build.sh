#!/bin/bash
set -e

IMAGE_NAME="jamesbrooks/sapowernetworks-exporter"
TAG="${1:-latest}"

echo "Building ${IMAGE_NAME}:${TAG} for linux/amd64 and linux/arm64..."

# Ensure buildx builder exists and supports multi-platform
docker buildx inspect multiarch >/dev/null 2>&1 || \
    docker buildx create --name multiarch --use

docker buildx use multiarch

# Build and push multi-architecture image
docker buildx build \
    --platform linux/amd64,linux/arm64 \
    --tag "${IMAGE_NAME}:${TAG}" \
    --push \
    .

echo "Done! Pushed ${IMAGE_NAME}:${TAG}"
