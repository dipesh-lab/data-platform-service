#!/usr/bin/env bash

set -euo pipefail

tar -czf catalog.tar.gz --transform 's,^catalog-data,java/catalog-data,' -C "$(dirname "../catalog-data")" "$(basename "../catalog-data")"
tar -xzf catalog.tar.gz
zip -rq catalog.zip java
mv catalog.zip target/
rm -rf java catalog.tar.gz