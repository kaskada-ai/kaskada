#!/bin/bash

# This script shows a simple run-through of using Kaskada to query some event data.

# Fire up the Kaskada engine and manager services
kaskada-manager &
kaskada-engine serve &

# Create a spec file describing the Kaskada resources you need
cat <<EOT > spec.yaml
tables:
  - table_name: GamePlay
    entity_key_column_name: entity_id
    time_column_name: event_at
    grouping_id: User
  - table_name: Purchase
    entity_key_column_name: entity_id
    time_column_name: event_at
    grouping_id: User
EOT

# Sync the spec with the Kaskada process
cli sync apply -f spec.yaml

# Create some sample 'GamePlay' data and load it
cat <<EOT > game_play.csv
event_at,entity_id,duration,won
2022-01-01 02:30:00+00:00,Alice,10,true
2022-01-01 02:35:00+00:00,Bob,3,false
2022-01-01 03:46:00+00:00,Bob,8,false
2022-01-01 03:58:00+00:00,Bob,23,true
2022-01-01 04:25:00+00:00,Bob,8,true
2022-01-01 05:05:00+00:00,Alice,53,true
2022-01-01 05:36:00+00:00,Alice,2,false
2022-01-01 07:22:00+00:00,Bob,7,false
2022-01-01 08:35:00+00:00,Alice,5,false
2022-01-01 10:01:00+00:00,Alice,43,true
EOT
cli load --table GamePlay --file-type csv --file-path file://$(pwd)/game_play.csv

# Create some sample 'Purchase' data and load it
cat <<EOT > purchase.csv
event_at,entity_id
2022-01-01 01:02:00+00:00,Alice
2022-01-01 01:35:00+00:00,Alice
2022-01-01 03:51:00+00:00,Bob
EOT
cli load --table Purchase --file-type csv --file-path file://$(pwd)/purchase.csv

# Simple query to make sure it's loaded correctly 
cli query run --response-as csv --stdout -- GamePlay
# > _time,_subsort,_key_hash,_key,event_at,entity_id,duration,won
# > 2022-01-01T02:30:00.000000000,12714731162780208561,5902814233694669492,Alice,2022-01-01 02:30:00+00:00,Alice,10,true
# > 2022-01-01T02:35:00.000000000,12714731162780208562,17054345325612802246,Bob,2022-01-01 02:35:00+00:00,Bob,3,false
# > ...

# Queries can be passed in from STDIN or a command argument
cli query run --response-as csv --stdout <<EOF                                                                            Thursday March 09 09:22:36 AM
let GameDefeat = GamePlay | when(not(GamePlay.won))

let features = {
    loss_duration: sum(GameDefeat.duration),
    purchase_count: count(Purchase) }

let is_prediction_time = not(GamePlay.won) and (count(GameDefeat, window=since(GamePlay.won)) == 2)

let example = features | when(is_prediction_time)
    | shift_to(time_of($input) | add_time(seconds(60*10)))

let target = count(Purchase) > example.purchase_count

in extend(example, {target}) | when(is_valid(example.purchase_count))
EOF
# > _time,_subsort,_key_hash,_key,loss_duration,purchase_count,target
# > 2022-01-01T08:45:00.000000000,0,5902814233694669492,Alice,7,2,false