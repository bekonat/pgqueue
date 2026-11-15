#!/usr/bin/env bash
# Enqueues jobs through the API and waits until the demo workers process
# them: emails succeed (some after retries), the duplicate report is
# deduplicated, and the job without a recipient ends up dead.
set -euo pipefail

API=${API:-http://localhost:8080}
N=${N:-50}
QUEUE=${QUEUE:-default}

post() { curl -fsS -X POST "$API$1" -H 'Content-Type: application/json' -d "$2"; }
field() { sed -nE "s/.*\"$1\":\"?([^,\"}]*).*/\1/p"; }

ids=()
for i in $(seq 1 "$N"); do
  ids+=("$(post /v1/jobs "{\"queue\":\"$QUEUE\",\"kind\":\"email.send\",\"payload\":{\"to\":\"user$i@example.com\"}}" | field id)")
done

key="report-$(date +%s%N)"
first=$(post /v1/jobs "{\"queue\":\"$QUEUE\",\"kind\":\"report.build\",\"payload\":{\"seconds\":1},\"unique_key\":\"$key\"}")
again=$(post /v1/jobs "{\"queue\":\"$QUEUE\",\"kind\":\"report.build\",\"payload\":{\"seconds\":1},\"unique_key\":\"$key\"}")
[[ $(echo "$again" | field duplicate) == true && $(echo "$first" | field id) == $(echo "$again" | field id) ]] \
  || { echo "FAIL: unique key did not deduplicate: $first / $again"; exit 1; }
report=$(echo "$first" | field id)
bad=$(post /v1/jobs "{\"queue\":\"$QUEUE\",\"kind\":\"email.send\",\"payload\":{}}" | field id)
echo "enqueued $N emails, report $report (deduplicated), bad job $bad"

state() { curl -fsS "$API/v1/jobs/$1" | field state; }
wait_state() {
  for _ in $(seq 1 120); do
    [[ $(state "$1") == "$2" ]] && return 0
    sleep 0.5
  done
  echo "FAIL: job $1 is $(state "$1"), want $2"; curl -fsS "$API/v1/jobs/$1"; echo; exit 1
}

for id in "${ids[@]}" "$report"; do wait_state "$id" succeeded; done
wait_state "$bad" dead
echo "all emails and the report succeeded, bad job is dead"

retried=0
for id in "${ids[@]}"; do
  [[ $(curl -fsS "$API/v1/jobs/$id" | field attempts) -gt 1 ]] && retried=$((retried + 1))
done
echo "$retried of $N emails needed more than one attempt"
curl -fsS "$API/v1/stats"; echo
echo OK
