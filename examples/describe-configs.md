# Example: DescribeConfigs

This example uses kafta; see https://github.com/electric-saw/kafta. You'll need at least v0.1.8 to use
`DescribeConfigs`.

## Configure kafta

```
kafta config set-context kamock \
    --server localhost:9292 \
    --sasl false \
    --tls false \
    --schema-registry ''
```

## List topics

```
kafta --context kamock topic list
```

## DescribeConfigs

```erlang
meck:expect(
    kamock_describe_configs_resource_result,
    make_describe_configs_resource_result,
    fun
        (TopicName = <<"cats">>, Name = <<"max.message.bytes">>, _Value) ->
            meck:passthrough([TopicName, Name, <<"2097176">>]);
        (TopicName, Name, Value) ->
            meck:passthrough([TopicName, Name, Value])
    end
).
```

```
$ kafta --context kamock topic list-configs dogs | grep message.bytes | cut -d'|' -f3
 1048588
$ kafta --context kamock topic list-configs cats | grep message.bytes | cut -d'|' -f3
 2097176
```
