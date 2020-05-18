#!/bin/bash

rm -f src/logs/*.log
rm -rf .mypy_cache
find . -name __pycache__ | xargs rm -rf

rm -rf notebooks/.ipynb_checkpoints