#!/bin/bash

# 'Modulo explicit sameAs' case
echo "Testing safety modulo explicit sameAs links..."
for i in {1..10}; do
    time for j in {1..100}; do python main.py 10 -p -s -t >/dev/null; done
done