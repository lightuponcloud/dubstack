-type proplist() :: proplists:proplist().

-record(riak_api_config, {
          s3_scheme="http://"::string(),
          s3_host="s3.amazonaws.com"::string(),
          s3_port=80::non_neg_integer(),
	  s3_proxy_host="127.0.0.1"::string(),
	  s3_proxy_port=8080::non_neg_integer(),
          s3_follow_redirect=false::boolean(),
          s3_follow_redirect_count=2::non_neg_integer(),
	  %% Riak's access key and secret
          access_key_id="<SECRET>"::string(),
          secret_access_key="<SECRET>"::string(),
          %% Network request timeout; if not specifed, the default timeout will be used:
          timeout=undefined::timeout()|undefined
         }).
-type(riak_api_config() :: #riak_api_config{}).

-define(FILE_UPLOAD_CHUNK_SIZE, 2000000).  % 2 MB
-define(FILE_MAXIMUM_SIZE, 5368709122).
-define(PUBLIC_BUCKET_SUFFIX, "public").
-define(PRIVATE_BUCKET_SUFFIX, "private").
-define(RIAK_INDEX_FILENAME, ".riak_index.html").
%% Riak is configured with the following option
%% {multi_backend_prefix_list, [{<<"the-">>, be_blocks}]}
-define(RIAK_BACKEND_PREFIX, "the").
