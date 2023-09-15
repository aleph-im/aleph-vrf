#!/bin/bash

set -euo pipefail

docker build -t aleph-vrf .
docker run --rm -ti -v "$(pwd)":/usr/src/aleph_vrf aleph-vrf \
  mksquashfs /opt/packages aleph-vrf-venv.squashfs