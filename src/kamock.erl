-module(kamock).
-export([
    start/0,
    quick_start/0
]).

-export_type([
    ref/0,
    broker/0,
    node_id/0
]).

-type ref() :: any().
-type hostname() :: binary().
-type node_id() :: non_neg_integer().
-type rack_id() :: binary() | null.

-type broker() :: #{
    host := hostname(),
    port := non_neg_integer(),
    node_id => node_id(),
    rack => rack_id(),
    ref := ref()
}.

%% Helper, so that `erl -s kamock` works.
start() ->
    application:ensure_all_started(kamock).

%% For ad-hoc testing using `rebar3 shell` or `rebar3 as test shell`, use this one:
quick_start() ->
    {ok, _Cluster, _Brokers = [Bootstrap | _]} = kamock_cluster:start(
        make_ref(), [101, 102, 103], #{port => 9990}
    ),
    {ok, Coordinator} = kamock_coordinator:start(make_ref()),
    meck:new(kamock_join_group, [passthrough]),
    meck:new(kamock_sync_group, [passthrough]),
    meck:new(kamock_leave_group, [passthrough]),
    meck:new(kamock_heartbeat, [passthrough]),
    meck:expect(
        kamock_join_group, handle_join_group_request, kamock_coordinator:join_group(Coordinator)
    ),
    meck:expect(
        kamock_sync_group, handle_sync_group_request, kamock_coordinator:sync_group(Coordinator)
    ),
    meck:expect(
        kamock_leave_group, handle_leave_group_request, kamock_coordinator:leave_group(Coordinator)
    ),
    meck:expect(
        kamock_heartbeat, handle_heartbeat_request, kamock_coordinator:heartbeat(Coordinator)
    ),

    Table = ets:new(kamock_messages, [public, ordered_set, named_table]),
    meck:expect(kamock_produce, handle_produce_request, kamock_ets:produce_to_ets(Table)),
    meck:expect(kamock_fetch, handle_fetch_request, kamock_ets:fetch_from_ets(Table)),
    meck:expect(kamock_list_offsets, handle_list_offsets_request, kamock_ets:offsets_in_ets(Table)),

    ets:new(kamock_committed_offsets, [public, set, named_table]),
    meck:expect(
        kamock_offset_commit,
        handle_offset_commit_request,
        kamock_offset_commit:to_ets(kamock_committed_offsets)
    ),
    meck:expect(
        kamock_offset_fetch,
        handle_offset_fetch_request,
        kamock_offset_fetch:from_ets(kamock_committed_offsets)
    ),
    {ok, Bootstrap}.
