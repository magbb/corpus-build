#/bin/sh

# concatenate all jsonl files into one big file and sort this file by dhlabid
# c = compact mode (not pretty print)
# s = slurp, read stream into one array for sorting
# .[] compact filter exploding the array to lines
cat corpus/*.jsonl | jq -c -s 'sort_by(.dhlabid) | .[]' > db/sorted.jsonl

# split this file into chunks of N lines (e.g. 250 000)
cd db
split -l 250000 sorted.jsonl
cd ..
