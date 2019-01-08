-record(riak_action_log_record, {
    action=""::string(),
    details=""::list(), % unicode-encoded characters list
    user_name=""::string(),
    tenant_name=""::string(),
    timestamp=undefined
}).

-type riak_action_log_record() :: #{
    action => string(),
    details => list(),
    user_name => string(),
    tenant_name => string(),
    timestamp => integer()
}.
