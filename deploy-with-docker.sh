docker build --platform=linux/amd64 -t aleph-vrf .
docker run --rm -ti -v "$(pwd)":/usr/src/aleph_vrf -v "$(echo $HOME/.aleph-im/private-keys/)":/root/.aleph-im/private-keys/ --platform linux/amd64 aleph-vrf python3 ./deployment/deploy_vrf_vms.py


