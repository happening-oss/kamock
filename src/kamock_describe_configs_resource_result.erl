-module(kamock_describe_configs_resource_result).
-export([make_describe_configs_resource_result/3]).

% From .../apache/kafka/clients/src/main/java/org/apache/kafka/common/requests/DescribeConfigsResponse.java
% -define(CONFIG_SOURCE_UNKNOWN, 0).
% -define(CONFIG_SOURCE_TOPIC, 1).
% -define(CONFIG_SOURCE_DYNAMIC_BROKER, 2).
% -define(CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER, 3).
% -define(CONFIG_SOURCE_STATIS_BROKER, 4).
-define(CONFIG_SOURCE_DEFAULT, 5).
% -define(CONFIG_SOURCE_DYNAMIC_BROKER_LOGGER, 6).
% -define(CONFIG_SOURCE_CLIENT_METRICS, 7).

make_describe_configs_resource_result(_TopicName, Name, Value) ->
    #{
        name => Name,
        value => Value,
        read_only => false,
        is_sensitive => false,
        is_default => false,
        % config_source and synonyms were added in v1; earlier encoders will drop them.
        config_source => ?CONFIG_SOURCE_DEFAULT,
        synonyms => []
    }.
