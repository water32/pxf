#!/usr/bin/env bash

set -e

: "${GOOGLE_CREDENTIALS:?GOOGLE_CREDENTIALS must be set}"

pxf_version=$(cat pxf_src/version)

# Tagging should only happen during the release
if [[ "${pxf_version}" =~ -SNAPSHOT || "${ENVIRONMENT}" != "prod" ]]; then
    echo "SNAPSHOT files or non-production environment detected"
    echo "Skipping tagging release image..."
    exit 0
fi

echo "PXF release version ${pxf_version} detected, start tagging process..."

echo "Authenticating with Google service account..."
gcloud auth activate-service-account --key-file=<(echo "${GOOGLE_CREDENTIALS}") >/dev/null 2>&1

current_release_tag=release-${pxf_version}
branch_release_tag=release-${pxf_version%.*}.x

echo "New release tag for images: ${current_release_tag}"
echo "New branch release tag for images: ${branch_release_tag}"

# Assume the images to tag have folder names ending with `-image`
image_folders=(./*-image)
for folder in "${image_folders[@]}"; do
    echo "Found image folder: ${folder}"
    image=$(cat "${folder}/repository")
    digest=$(cat "${folder}/digest")
    echo "Tagging image ${image} with digest: ${digest}"
    gcloud container images add-tag --quiet "${image}:${digest}" "${image}:${current_release_tag}"
    gcloud container images add-tag --quiet "${image}:${digest}" "${image}:${branch_release_tag}"
done
