# jx-sqlite 
JSON query expressions using SQLite

## Overview

An attempt to store JSON documents in SQLite so that they are accessible via SQL. The hope is this will serve a basis for a general document-relational map (DRM), and leverage the database's query optimizer.

## Status

It looks like many tests already pass, but those are the easy ones. The difficult tests, testing queries into nested arrays, remain to be solved.  Hopefully there will be a GSOC project to refactor and finish this work.

The tests fail because what I have written does not handle the most interesting, and most important features: We want to query nested object arrays as if they were just another table.  This is important for two reasons; 1) Inner objects {"a":{"b":0}} are a shortcut for nested arrays {"a":[{"b":0}]}, plus schemas can be expanded from one-to-one  to one-to-many {"a":[{"b":0}, {"b":1}]}.

## Running tests

    export PYTHONPATH=.
    python -m unittest discover -v -s tests
