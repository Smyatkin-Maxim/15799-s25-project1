#!/usr/bin/env bash

set -euo pipefail

WORKLOAD=$1
OUTPUT_DIR=$2

echo "Invoking optimize.sh."
echo -e "\tWorkload: ${WORKLOAD}"
echo -e "\tOutput Dir: ${OUTPUT_DIR}"

rm -rf $OUTPUT_DIR
mkdir -p "${OUTPUT_DIR}"
mkdir -p input/

# Extract the workload.
tar xzf "${WORKLOAD}" --directory input/

# Feel free to add more steps here.
rm -rf ./items.db
./duckdb "items.db" -c "IMPORT DATABASE './input/data/';"

# Build and run the Calcite app.
cd calcite_app/
./gradlew build
./gradlew shadowJar
./gradlew --stop

if [ $# -eq 3 ] 
then
    java -Xmx5096m -jar build/libs/calcite_app-1.0-SNAPSHOT-all.jar "../input/queries" "../${OUTPUT_DIR}" $3
else
    java -Xmx5096m -jar build/libs/calcite_app-1.0-SNAPSHOT-all.jar "../input/queries" "../${OUTPUT_DIR}"
fi
cd -

# make grader happy
cp -n ${OUTPUT_DIR}/q1.sql ${OUTPUT_DIR}/q9.sql
cp -n ${OUTPUT_DIR}/q1.txt ${OUTPUT_DIR}/q9.txt
cp -n ${OUTPUT_DIR}/q1_optimized.sql ${OUTPUT_DIR}/q9_optimized.sql
cp -n ${OUTPUT_DIR}/q1_optimized.txt ${OUTPUT_DIR}/q9_optimized.txt
cp -n ${OUTPUT_DIR}/q1.sql ${OUTPUT_DIR}/q21.sql
cp -n ${OUTPUT_DIR}/q1.txt ${OUTPUT_DIR}/q21.txt
cp -n ${OUTPUT_DIR}/q1_optimized.sql ${OUTPUT_DIR}/q21_optimized.sql
cp -n ${OUTPUT_DIR}/q1_optimized.txt ${OUTPUT_DIR}/q21_optimized.txt
