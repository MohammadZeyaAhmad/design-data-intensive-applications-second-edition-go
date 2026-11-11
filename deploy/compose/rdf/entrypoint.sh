#!/bin/sh
set -eu
umask 077
export FUSEKI_BASE=/tmp/fuseki-base
: "${ATLAS_RDF_PASSWORD:?configure the RDF profile first}"
printf 'atlas: %s,atlas\n' "$ATLAS_RDF_PASSWORD" > /tmp/fuseki.passwd
exec java -Xms128m -Xmx384m -jar /app/fuseki-server.jar --tdb2 --loc=/data --update --port=3030 --auth=basic --passwd=/tmp/fuseki.passwd --ping --stats --timeout=10000 --no-cors --strict /routes
