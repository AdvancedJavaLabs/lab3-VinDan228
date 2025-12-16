set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

SERVICE="${HADOOP_NAMENODE_SERVICE:-namenode}"
OUTPUT="/output/sales_result"
LOCAL="$ROOT_DIR/results/result.tsv"

usage() {
  cat <<EOF >&2
Usage:
  $0 [--output <hdfs_output_dir>] [--local <local_file>]

Defaults:
  --output $OUTPUT
  --local  $LOCAL
EOF
}

while (( $# > 0 )); do
  case "$1" in
    --output) OUTPUT="${2:-}"; shift 2 ;;
    --local) LOCAL="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

mkdir -p "$(dirname "$LOCAL")"

PART="$OUTPUT/part-r-00000"
echo "Fetching: HDFS:$PART -> $LOCAL"

docker compose exec -T "$SERVICE" bash -lc "'${HADOOP_HOME:-/opt/hadoop-3.2.1}/bin/hdfs' dfs -test -e '$PART'"
docker compose exec -T "$SERVICE" bash -lc "'${HADOOP_HOME:-/opt/hadoop-3.2.1}/bin/hdfs' dfs -cat '$PART'" > "$LOCAL"

echo "Saved: $LOCAL"



