# SlackBot Example

## Data Prep

```sql
copy(
    select
      type              ,
      subtype           ,
      to_timestamp(cast(ts as bigint)) as ts,
      user              ,
      text              ,
      -- client_msg_id     ,
      -- blocks            ,
      team              ,
      user_team         ,
      source_team       ,
      user_profile      ,
      inviter           ,
      edited            ,
      reactions         ,
      thread_ts         ,
      reply_count       ,
      reply_users_count ,
      latest_reply      ,
      -- reply_users       ,
      -- replies           ,
      is_locked         ,
      subscribed        ,
      last_read         ,
      parent_user_id    ,
      regexp_extract(filename, 'data/iloveai-initial-export/(.+)/\d{4}-\d{2}-\d{2}.json', 1) as channel
    from (
        select * from read_json_auto('data/iloveai-initial-export/*/*.json',
            format='array',
            filename=true,
            union_by_name=true)
    )
) to 'messages.parquet' (FORMAT PARQUET)
;

    select
    from (
        select * from read_json_auto('data/iloveai-initial-export/*/*.json',
            format='array',
            filename=true,
            union_by_name=true)
    )
    limit 10
;
```