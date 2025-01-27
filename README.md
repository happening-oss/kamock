# kamock

Mock Kafka broker.

When writing unit tests for Kafine, we found it useful to have an easily-configurable mock broker. It's also useful for
ad-hoc manual testing.

## As a dependency

### erlang.mk

```makefile
TEST_DEPS += kamock
dep_kamock = git https://github.com/happening-oss/kamock
```

### rebar3

```erlang
{profiles, [
    {test, [
        {deps, [
            {kamock, {git, "https://github.com/happening-oss/kamock", {branch, "main"}}}
        ]}
    ]}
]}.
```

## Running it in ad-hoc mode

```sh
# assumes you've got rebar3_auto installed; see https://github.com/vans163/rebar3_auto
rebar3 as test auto

# otherwise:
rebar3 as test shell
```

We use `rebar3 as test ...` to use the `test` profile. This makes sure that `meck` is available; see `rebar.config`.

## Starting the broker

```erlang
{ok, Broker} = kamock_broker:start().
```

`Broker` is a map containing `host`, `port`, `node_id` keys.

By default, the broker listens on a randomly-assigned port number. If you want more control, you can use `start/2`, as
follows:

```erlang
Ref = make_ref().
Options = #{port => 9292}.
{ok, Broker} = kamock_broker:start(Ref, Options).
```

To stop the mock broker:

```erlang
kamock_broker:stop(Broker).
```

## Defaults

By default, the mock broker:

- Responds to `Fetch` requests for any topic.
- Responds as if every topic has 4 partitions and is new and empty.
- Responds to `Produce` requests as if you produced to `/dev/null`; you'll get a success result, but the message will
  vanish into the void.

The mock broker supports consumer groups but, by default, it:

- Supports only one group member, which is the leader.
- Ignores `OffsetCommit` requests.

To change the mock broker's behaviour, it's expected that you'll use `meck`; see below for more details. There are
various helper functions to make this easier.  Examples are in the `examples` directory and in the mock broker's own
unit tests.

## Starting a cluster

```erlang
{ok, Cluster, [Broker | _]} = kamock_cluster:start(),
```

This starts 3 brokers. By default, they're not really "clustered" in any sense.

- Metadata requests for any topic spread the partitions over the available brokers, as if they were all in-sync
  replicas.

By default, all of the brokers in the cluster listen on randomly-assigned ports. If you want to specify a port
number for the "bootstrap" broker:

```erlang
{ok, Cluster, [Broker | _]} = kamock_cluster:start(Ref, [101, 102, 103], #{port => 9292}),
```

To stop the cluster and all of the brokers:

```erlang
kamock_cluster:stop(Cluster).
```

## Mocking

### Waiting for requests

To wait for a request, do something like this:

```erlang
meck:wait(kamock_fetch, handle_fetch_request, '_', ?TIMEOUT_MS).
```

### Fake responses

The mock broker is divided into fairly fine-grained modules: `kamock_fetchable_topic`, etc.. These are named after the
entities in the Kafka protocol.

It's intended that you'll use `meck` to replace the default behaviour. So, for example, if you wanted to pretend that a
topic didn't exist, you'd do something like this:

```erlang
meck:expect(kamock_partition_data, make_partition_data,
    fun(_Topic, #{partition := P}, _Env) ->
        #{
            partition_index => P,
            error_code => ?UNKNOWN_TOPIC_OR_PARTITION,
            high_watermark => -1,
            last_stable_offset => -1,
            log_start_offset => -1,
            aborted_transactions => null,
            preferred_read_replica => -1,
            records => []
        }
    end).
```

Yes, that's a bit verbose; it can be shortened to this:

```erlang
% Because we call a function that is itself defined in a module that we're intercepting,
% we need to specify 'passthrough'.
meck:new(kamock_partition_data, [passthrough]),
meck:expect(kamock_partition_data, make_partition_data,
    fun(_Topic, #{partition := P}, _Env) ->
        kamock_partition_data:make_error(
            P, ?UNKNOWN_TOPIC_OR_PARTITION
        )
    end).
```

## What's with all of the tiny modules?

1. `meck` only allows mocking exported functions, so they need to be exported.
2. Calls within a module don't go through the mock unless module-qualified. Putting them in separate modules forces them
   to be module-qualified.
3. At some point, you'll forget to do that.

So the tiny modules are a hint to future you: follow the pattern when implementing more of the mock broker, and it'll
probably work.
