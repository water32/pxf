#!/usr/bin/env bash

set -e
: "${GIT_REMOTE:=origin}" # if your remote is not called origin, you can override it here

GIT_SHA="${1}"
MAJOR_MINOR_VERSION="${2}"
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
pxf_src=$(dirname ${SCRIPT_DIR})

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

# validate version input
if ! [[ "${MAJOR_MINOR_VERSION}" =~ ^[0-9]+\.[0-9]+$ ]]; then
  echo "Expected <major.minor> and received \`${MAJOR_MINOR_VERSION}\`"
fi

branch_name=branch-"${MAJOR_MINOR_VERSION}".x
current_branch=$(git rev-parse --abbrev-ref HEAD)

git checkout "${GIT_SHA}"
echo "Creating new branch ${branch_name}..."
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
