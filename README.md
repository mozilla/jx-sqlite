# jx-sqlite 
JSON query expressions using SQLite

## Overview

An attempt to store JSON documents in SQLite so that they are accessible via SQL. The hope is this will serve a basis for a general document-relational map (DRM), and leverage the database's query optimizer.

## Status

It looks like many tests already pass, but those are the easy ones. The difficult tests, testing queries into nested arrays, remain to be solved.  Hopefully there will be a GSOC project to refactor and finish this work. 