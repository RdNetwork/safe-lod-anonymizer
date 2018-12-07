#!/bin/bash

# Standard case
echo "Testing standard case (basic safety)..."
for i in {1..20}; do
    time for j in {1..100}; do python main.py 10 -t >/dev/null; done
done