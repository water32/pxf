#!/usr/bin/env bash

set -e
: "${GIT_REMOTE:=origin}" # if your remote is not called origin, you can override it here

if [ $# -ne 2 ]; then
    echo "ERROR: 2 arguments required. See usage below"
    echo ""
    echo "Usage: ./create-release-branch.bash <git sha> <major.minor version>"
    echo "    <git sha>              commit from which to branch"
    echo "    <major.minor version>  desired release version"
    echo ""
    echo "Example: ./create-release-branch.bash abcdef 1.2"
    echo "    creates a branch for the release called \`branch-1.2.x\` off commit abcdef"
    echo "    creates a commit on the branch where version is set to \`1.2.0-SNAPSHOT\`"
    exit 1
fi

GIT_SHA="${1}"
MAJOR_MINOR_VERSION="${2}"
pwd_print=${PWD}
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pxf_src=$(dirname ${SCRIPT_DIR})

echo "SCRIPT_DIR: $SCRIPT_DIR"
echo "pxf_src: $pxf_src"

# validate new version:
#  major + minor, numbers only, etc

branch_name=branch-"${MAJOR_MINOR_VERSION}".x
current_branch=$(git rev-parse --abbrev-ref HEAD)

echo "Creating new branch ${branch_name}..."
git checkout "${GIT_SHA}"
git checkout -b "${branch_name}"

pxf_version="${MAJOR_MINOR_VERSION}".0

patch=${pxf_version##*.}
# bump patch and add -SNAPSHOT
SNAPSHOT_VERSION="${pxf_version}"-SNAPSHOT

echo "Changing version ${pxf_version} -> ${SNAPSHOT_VERSION} and committing change..."
echo "${SNAPSHOT_VERSION}" > ${pxf_src}/version
git -C ${pxf_src} add version
git -C ${pxf_src} commit -m "Bump version to ${SNAPSHOT_VERSION} [skip ci]"

echo "Pushing new branch ${TAG} and new SNAPSHOT version ${SNAPSHOT_VERSION}"
git -C ${pxf_src} push -u ${GIT_REMOTE} $branch_name

git checkout ${current_branch}
