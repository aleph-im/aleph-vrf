docker build -t aleph-vrf .
docker run --rm -ti -v "$(pwd)":/usr/src/aleph_vrf aleph-vrf bash