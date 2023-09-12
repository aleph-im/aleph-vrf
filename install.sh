#!/bin/bash

python3 -m virtualenv venv
source venv/bin/activate

pip install -e .[testing]