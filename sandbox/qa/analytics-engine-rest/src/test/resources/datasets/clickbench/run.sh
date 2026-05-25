#!/usr/bin/env bash
# Manual ingest + query helper for the analytics ClickBench dataset.
# Self-locating: works from anywhere as long as bulk.json, mapping.json,
# and ppl/q*.ppl sit alongside this script.
#
# Subcommands:
#   ingest [-i INDEX] [-s SHARDS]
#       Create $INDEX with parquet settings + $SHARDS primaries, then bulk-load.
#   dual [-l LEFT_SHARDS] [-r RIGHT_SHARDS] [-p PREFIX]
#       Create two indices (default clickbench_1 / clickbench_4) and ingest the
#       same dataset into each. Index name is "<prefix>_<shards>".
#   query [-i INDEX] [-q QNUM] [-d ppl|sql]
#       POST ppl/qQNUM.ppl (or sql/qQNUM.sql) to /_plugins/_ppl (or /_plugins/_sql)
#       and pretty-print the response. Dialect defaults to ppl.
#   compare [-q QSPEC] [-l LEFT_SHARDS] [-r RIGHT_SHARDS] [-p PREFIX] [-d ppl|sql]
#       Like dual, then run each query against both and report which differ.
#       QSPEC accepts "13", "1-10", or "1,3,5"; default is every qN.{ppl,sql} present.
#
# Env:
#   HOST     target OS cluster (default localhost:9200)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOST="${HOST:-localhost:9200}"

usage() {
    sed -n '2,22p' "${BASH_SOURCE[0]}"
}

dual_setup() {
    local left="$1" right="$2" prefix="$3"
    local LEFT="${prefix}_${left}" RIGHT="${prefix}_${right}"
    create_index "$LEFT" "$left"
    bulk_ingest "$LEFT"
    create_index "$RIGHT" "$right"
    bulk_ingest "$RIGHT"
    echo "$LEFT $RIGHT"
}

create_index() {
    local index="$1" shards="$2"
    curl -fsS -XDELETE "$HOST/$index" >/dev/null 2>&1 || true
    jq --argjson s "$shards" '.settings.number_of_shards = $s' "$SCRIPT_DIR/mapping.json" \
        | curl -fsS -XPUT "$HOST/$index" \
            -H 'Content-Type: application/json' --data-binary @- >/dev/null
    echo "created $index (shards=$shards)"
}

bulk_ingest() {
    local index="$1"
    curl -fsS -XPOST "$HOST/$index/_bulk?refresh=true" \
        -H 'Content-Type: application/x-ndjson' \
        --data-binary "@$SCRIPT_DIR/bulk.json" \
        | jq -e '.errors == false' >/dev/null
    curl -fsS -XPOST "$HOST/$index/_flush?force=true" >/dev/null
    curl -fsS "$HOST/_cluster/health/$index?wait_for_status=green&timeout=30s" >/dev/null
    echo "ingested $index"
}

run_query() {
    local index="$1" qnum="$2" dialect="${3:-ppl}"
    local endpoint dir ext q
    case "$dialect" in
        ppl) endpoint="/_plugins/_ppl"; dir="ppl"; ext="ppl" ;;
        sql) endpoint="/_plugins/_sql"; dir="sql"; ext="sql" ;;
        *) echo "unknown dialect: $dialect (use ppl|sql)" >&2; return 1 ;;
    esac
    local qfile="$SCRIPT_DIR/$dir/q${qnum}.$ext"
    [ -f "$qfile" ] || { echo "missing query file: $qfile" >&2; return 1; }
    q=$(sed "s/clickbench/$index/g" "$qfile")
    curl -fsS -XPOST "$HOST$endpoint" \
        -H 'Content-Type: application/json' \
        -d "$(jq -nc --arg q "$q" '{query:$q}')"
}

available_qnums() {
    local dialect="${1:-ppl}"
    local dir ext
    case "$dialect" in
        ppl) dir="ppl"; ext="ppl" ;;
        sql) dir="sql"; ext="sql" ;;
    esac
    ls "$SCRIPT_DIR/$dir"/q*."$ext" 2>/dev/null \
        | sed -E "s@.*/q([0-9]+)\.${ext}@\1@" \
        | sort -n | tr '\n' ' '
}

expand_range() {
    python3 - "$1" <<'PY'
import sys
out = set()
for tok in sys.argv[1].split(','):
    if '-' in tok:
        a, b = tok.split('-', 1)
        out.update(range(int(a), int(b) + 1))
    else:
        out.add(int(tok))
print(' '.join(str(i) for i in sorted(out)))
PY
}

cmd="${1:-}"
shift || { usage; exit 1; }

case "$cmd" in
    ingest)
        INDEX="clickbench"; SHARDS="4"
        while getopts "i:s:" opt; do
            case "$opt" in
                i) INDEX="$OPTARG" ;;
                s) SHARDS="$OPTARG" ;;
                *) usage; exit 1 ;;
            esac
        done
        create_index "$INDEX" "$SHARDS"
        bulk_ingest "$INDEX"
        ;;
    query)
        INDEX="clickbench"; QNUM="1"; DIALECT="ppl"
        while getopts "i:q:d:" opt; do
            case "$opt" in
                i) INDEX="$OPTARG" ;;
                q) QNUM="$OPTARG" ;;
                d) DIALECT="$OPTARG" ;;
                *) usage; exit 1 ;;
            esac
        done
        run_query "$INDEX" "$QNUM" "$DIALECT" | jq
        ;;
    dual)
        L=1; R=4; PREFIX="clickbench"
        while getopts "l:r:p:" opt; do
            case "$opt" in
                l) L="$OPTARG" ;;
                r) R="$OPTARG" ;;
                p) PREFIX="$OPTARG" ;;
                *) usage; exit 1 ;;
            esac
        done
        dual_setup "$L" "$R" "$PREFIX" >/dev/null
        ;;
    compare)
        QSPEC=""; L=1; R=4; PREFIX="clickbench"; DIALECT="ppl"
        while getopts "q:l:r:p:d:" opt; do
            case "$opt" in
                q) QSPEC="$OPTARG" ;;
                l) L="$OPTARG" ;;
                r) R="$OPTARG" ;;
                p) PREFIX="$OPTARG" ;;
                d) DIALECT="$OPTARG" ;;
                *) usage; exit 1 ;;
            esac
        done
        case "$DIALECT" in ppl) EXT=ppl;; sql) EXT=sql;; *) echo "unknown dialect: $DIALECT" >&2; exit 1;; esac
        read -r LEFT RIGHT < <(dual_setup "$L" "$R" "$PREFIX" | tail -1)

        if [ -z "$QSPEC" ]; then
            QNUMS=$(available_qnums "$DIALECT")
        else
            QNUMS=$(expand_range "$QSPEC")
        fi

        diffs=0
        for q in $QNUMS; do
            [ -f "$SCRIPT_DIR/$DIALECT/q${q}.$EXT" ] || continue
            l_out=$(run_query "$LEFT" "$q" "$DIALECT" | jq -c '.rows // .data // .')
            r_out=$(run_query "$RIGHT" "$q" "$DIALECT" | jq -c '.rows // .data // .')
            if [ "$l_out" = "$r_out" ]; then
                echo "q$q: match"
            else
                diffs=$((diffs + 1))
                echo "q$q: DIFFER"
                diff <(printf '%s\n' "$l_out" | jq .) <(printf '%s\n' "$r_out" | jq .) | head -40
            fi
        done
        echo "===== $diffs queries differ ====="
        ;;
    ""|-h|--help) usage ;;
    *) usage; exit 1 ;;
esac
