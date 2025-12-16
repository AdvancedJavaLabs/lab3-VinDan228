set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

INPUT="/input/sales"
OUT_PREFIX="/output/bench"
SERVICE="${HADOOP_NAMENODE_SERVICE:-namenode}"
LOCAL_CSV="$ROOT_DIR/results/bench.csv"

REDUCERS_LIST="${REDUCERS_LIST:-1 2 4}"
MAP_THREADS_LIST="${MAP_THREADS_LIST:-1 2 4 8}"

mkdir -p "$(dirname "$LOCAL_CSV")"
if [[ ! -f "$LOCAL_CSV" ]]; then
  echo "timestamp,reducers,map_threads,seconds,output" > "$LOCAL_CSV"
fi

echo "Benchmarking..."
echo "reducers:     $REDUCERS_LIST"
echo "map_threads:  $MAP_THREADS_LIST"
echo "input:        $INPUT"
echo "out_prefix:   $OUT_PREFIX"
echo "results file: $LOCAL_CSV"

(cd "$ROOT_DIR" && ./gradlew -q shadowJar)

ts="$(date +%s)"

for r in $REDUCERS_LIST; do
  for t in $MAP_THREADS_LIST; do
    out="${OUT_PREFIX}_${ts}_r${r}_t${t}"
    start="$(date +%s)"
    "$ROOT_DIR/scripts/run_job.sh" --no-build --input "$INPUT" --output "$out" --reducers "$r" --map-threads "$t"
    end="$(date +%s)"
    secs="$(( end - start ))"
    echo "$(date -Iseconds),$r,$t,$secs,$out" >> "$LOCAL_CSV"

    docker compose exec -T "$SERVICE" bash -lc "'${HADOOP_HOME:-/opt/hadoop-3.2.1}/bin/hdfs' dfs -rm -r -f '$out' >/dev/null 2>&1 || true"
  done
done

echo "Done. See: $LOCAL_CSV"



