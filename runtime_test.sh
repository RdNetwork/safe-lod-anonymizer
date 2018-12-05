#!/bin/bash

# Standard case
for i in {1..20}; do
    time for j in {1..100}; do python main.py 10 -t >/dev/null; done
done

# Modulo explicit sameAs case
for i in {1..20}; do
    time for j in {1..100}; do python main.py 10 -s -t >/dev/null; done
done