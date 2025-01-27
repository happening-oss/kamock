-module(kamock_describe_configs_resource_result).
-export([make_describe_configs_resource_result/3]).

-define(CONFIG_SOURCE_DEFAULT, 5).

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
