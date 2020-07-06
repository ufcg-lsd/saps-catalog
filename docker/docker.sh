#!/bin/bash

readonly REPOSITORY=ufcgsaps/catalog
readonly USAGE="usage: docker.sh [-a] {build|push|publish|run} <TAG>"
readonly MY_PATH=$(cd "$(dirname "${0}")" || { echo "For some reason, the path is not accessible"; exit 1; }; pwd )
readonly WORKING_DIRECTORY="$(dirname "${MY_PATH}")"
readonly CATALOG_DOCKER_FILE_PATH="${MY_PATH}/Dockerfile"
readonly ARTEFACT_DOCKER_FILE_PATH="${MY_PATH}/ArtefactBuilder"

# Catalog Properties
readonly CATALOG_CONTAINER=saps-catalog
readonly CATALOG_NETWORK=saps-network
readonly CATALOG_USER=admin
readonly CATALOG_PASSWORD=admin
readonly CATALOG_DB=saps
readonly CATALOG_PORT=5432

# Flag to indicate use of the ArtefactBuilder dockerfile.
# Use "-a" to assign it
artefact_flag=false

build() {
  local TAG="${1-latest}"
  local DOCKER_FILE_PATH="${CATALOG_DOCKER_FILE_PATH}"

  if "${artefact_flag}"; then
    DOCKER_FILE_PATH=${ARTEFACT_DOCKER_FILE_PATH}
  fi

  docker build --tag "${REPOSITORY}":"${TAG}" \
            --file "${DOCKER_FILE_PATH}" "${WORKING_DIRECTORY}"
}

push() {
  local TAG="${1-latest}"
  docker push "${REPOSITORY}":"${TAG}"
}

run() {
  local TAG="${1-latest}"
  docker run -dit \
    --name "${CATALOG_CONTAINER}" \
    -p "${CATALOG_PORT}":5432 \
    --net="${CATALOG_NETWORK}" --net-alias=catalog \
    -v catalogdata:/var/lib/postgresql/data \
    -e POSTGRES_USER="${CATALOG_USER}" \
    -e POSTGRES_PASSWORD="${CATALOG_PASSWORD}" \
    -e POSTGRES_DB="${CATALOG_DB}" \
    "${REPOSITORY}":"${TAG}"
}

set_options() {
  while getopts 'a' OPTION
  do
    case ${OPTION} in
      a) artefact_flag=true
        ;;
      ?) echo "${USAGE}" >&2
        exit 2
        ;;
    esac
  done
}

main() {
  if [ "$#" -eq 0 ]; then
      echo "Use: ${USAGE}"
      exit 1
  fi

  set_options "$@"
  shift $((OPTIND - 1))

  case ${1} in
    build) shift
      build "$@"
      ;;
    push) shift
      push "$@"
      ;;
    publish) shift
      build "$@"
      push "$@"
      ;;
    run) shift
      run "$@"
      ;;
    *)
      echo "${USAGE}" >&2
      exit 1
  esac
}

main "$@"