
## Training Data Prep

Using DuckDB, run the following query to get all the historical data into a single file:

```sql
copy(
    select
      -- type,
      -- subtype           ,
      cast(ts as double) as ts,
      -- to_timestamp(cast(ts as bigint)) as ts_timedate,
      user                 ,
      text                 ,
      -- client_msg_id     ,
      -- blocks            ,
      -- team              ,
      -- user_team         ,
      -- source_team       ,
      -- user_profile      ,
      -- inviter           ,
      -- edited            ,
      reactions            ,
      cast(thread_ts as double) as thread_ts,
      -- thread_ts,
      -- to_timestamp(cast(thread_ts as bigint)) as thread_ts_timedate,
      -- reply_count       ,
      -- reply_users_count ,
      -- latest_reply      ,
      reply_users          ,
      -- replies           ,
      -- is_locked         ,
      -- subscribed        ,
      -- last_read         ,
      -- parent_user_id    ,
      regexp_extract(filename, 'slack-export/(.+)/\d{4}-\d{2}-\d{2}.json', 1) as channel
    from (
        select * from read_json_auto('slack-export/*/*.json',
            format='array',
            filename=true,
            union_by_name=true)
    )
    where subtype is null
    order by channel, ts
) to 'messages.jsonl' (FORMAT JSON);
```