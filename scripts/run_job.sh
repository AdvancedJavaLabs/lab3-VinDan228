set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SERVICE="${HADOOP_JOB_SERVICE:-resourcemanager}"
JAR_IN_CONTAINER="${HADOOP_JOB_JAR_PATH:-/tmp/sales-job.jar}"
HADOOP_BIN="${HADOOP_BIN:-${HADOOP_HOME:-/opt/hadoop-3.2.1}/bin/hadoop}"
HDFS_BIN="${HDFS_BIN:-${HADOOP_HOME:-/opt/hadoop-3.2.1}/bin/hdfs}"

INPUT="/input/sales"
OUTPUT="/output/sales_result"
REDUCERS="2"
MAP_THREADS="4"
NO_BUILD="0"

usage() {
  cat <<EOF >&2
Usage:
  $0 --input <hdfs_input> --output <hdfs_output> [--reducers N] [--map-threads N] [--no-build]

Defaults:
  --input       $INPUT
  --output      $OUTPUT
  --reducers    $REDUCERS
  --map-threads $MAP_THREADS
EOF
}

while (( $# > 0 )); do
  case "$1" in
    --input) INPUT="${2:-}"; shift 2 ;;
    --output) OUTPUT="${2:-}"; shift 2 ;;
    --reducers) REDUCERS="${2:-}"; shift 2 ;;
    --map-threads) MAP_THREADS="${2:-}"; shift 2 ;;
    --no-build) NO_BUILD="1"; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "$INPUT" || -z "$OUTPUT" ]]; then
  usage
  exit 2
fi

JAR_LOCAL="$ROOT_DIR/build/libs/Solution-1.0-SNAPSHOT-all.jar"

if [[ "$NO_BUILD" != "1" ]]; then
  (cd "$ROOT_DIR" && ./gradlew -q shadowJar)
fi

if [[ ! -f "$JAR_LOCAL" ]]; then
  echo "Jar not found: $JAR_LOCAL" >&2
  echo "Try: ./gradlew shadowJar" >&2
  exit 2
fi

echo "Copying jar into container: $SERVICE:$JAR_IN_CONTAINER"
docker compose cp "$JAR_LOCAL" "$SERVICE:$JAR_IN_CONTAINER"

echo "Checking HDFS input exists: $INPUT"
if ! docker compose exec -T "$SERVICE" bash -lc "'$HDFS_BIN' dfs -test -e '$INPUT'"; then
  echo "ERROR: HDFS input path does not exist: $INPUT" >&2
  echo "Hint: upload local csv/*.csv into HDFS first, e.g.:" >&2
  echo "  bash scripts/hdfs_put_csv.sh ./csv $INPUT" >&2
  exit 4
fi

echo "Running Hadoop job on YARN..."
docker compose exec -T "$SERVICE" bash -lc \
  "'$HADOOP_BIN' jar '$JAR_IN_CONTAINER' \
    --input '$INPUT' --output '$OUTPUT' --reducers '$REDUCERS' --map-threads '$MAP_THREADS'"

echo "Done. Output (HDFS): $OUTPUT"
echo "Tip: fetch with scripts/fetch_result.sh --output '$OUTPUT' --local results/result.tsv"


