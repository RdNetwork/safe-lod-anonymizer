#!/bin/bash

# 'Modulo explicit sameAs' case
echo "Testing safety modulo explicit sameAs links..."
for i in {1..20}; do
    time for j in {1..100}; do python main.py 10 -s -t >/dev/null; done
done