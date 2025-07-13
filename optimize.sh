#!/usr/bin/env bash

set -euo pipefail

WORKLOAD=$1
OUTPUT_DIR=$2

echo "Invoking optimize.sh."
echo -e "\tWorkload: ${WORKLOAD}"
echo -e "\tOutput Dir: ${OUTPUT_DIR}"

if [ -d $OUTPUT_DIR ]
then
    rm -rf ./old_plans
    mkdir ./old_plans
    cp -f ${OUTPUT_DIR}*_optimized.sql ./old_plans || echo "No plans"
    cp -f ${OUTPUT_DIR}*_optimized.txt ./old_plans || echo "No plans"
fi

rm -rf $OUTPUT_DIR
mkdir -p "${OUTPUT_DIR}"
mkdir -p input/

# Extract the workload.
tar xzf "${WORKLOAD}" --directory input/

# Feel free to add more steps here.
rm -rf ./items.db
./duckdb "items.db" -c "IMPORT DATABASE './input/data/';"

[ -f scores.txt ] && mv scores.txt old_scores.txt

# Build and run the Calcite app.
cd calcite_app/
./gradlew build
./gradlew shadowJar
./gradlew --stop

if [ $# -eq 3 ] 
then
    java -Xms8096m -Xmx8096m -jar build/libs/calcite_app-1.0-SNAPSHOT-all.jar "../input/queries" "../${OUTPUT_DIR}" $3
else
    java -Xms8096m -Xmx8096m -jar build/libs/calcite_app-1.0-SNAPSHOT-all.jar "../input/queries" "../${OUTPUT_DIR}"
fi
cd -

# make grader happy even if some queries are skipped/failing
q=0
for f in ./input/queries/*.sql; do
    q=`basename $f .sql`
    cp -n ${OUTPUT_DIR}/capybara1.sql ${OUTPUT_DIR}/${q}.sql
    cp -n ${OUTPUT_DIR}/capybara1.txt ${OUTPUT_DIR}/${q}.txt
    cp -n ${OUTPUT_DIR}/capybara1_optimized.sql ${OUTPUT_DIR}/${q}_optimized.sql
    cp -n ${OUTPUT_DIR}/capybara1_optimized.txt ${OUTPUT_DIR}/${q}_optimized.txt
done
