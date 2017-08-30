-record(keystone_api_config, {
          keystone_url="http://localhost:35357"::string(),
	  keystone_admin_username="admin"::string(),
	  keystone_admin_tenant="admin"::string(),
	  keystone_admin_password="<SECRET>"::string(),
          %% Network request timeout; if not specifed, the default timeout will be used:
          timeout=undefined::timeout()|undefined
         }).
-type(keystone_api_config() :: #keystone_api_config{}).
