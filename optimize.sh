#!/usr/bin/env bash

set -euo pipefail

WORKLOAD=$1
OUTPUT_DIR=$2

echo "Invoking optimize.sh."
echo -e "\tWorkload: ${WORKLOAD}"
echo -e "\tOutput Dir: ${OUTPUT_DIR}"

mkdir -p "${OUTPUT_DIR}"
mkdir -p input/

# Extract the workload.
tar xzf "${WORKLOAD}" --directory input/

# Feel free to add more steps here.
rm -rf ./items.db
cat ./input/data/schema.sql ./input/data/load.sql | ./duckdb items.db 

# Build and run the Calcite app.
cd calcite_app/
./gradlew build
./gradlew shadowJar
./gradlew --stop

if [ $# -eq 3 ] 
then
	java -agentlib:jdwp=transport=dt_socket,server=y,address=5005 -Xmx4096m -jar build/libs/calcite_app-1.0-SNAPSHOT-all.jar "../input/queries" "../${OUTPUT_DIR}"
else
	java -Xmx4096m -jar build/libs/calcite_app-1.0-SNAPSHOT-all.jar "../input/queries" "../${OUTPUT_DIR}"
fi
cd -
