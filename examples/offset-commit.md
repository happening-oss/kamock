# Example: OffsetCommit

By default, the mock broker silently ignores `OffsetCommit` requests, so if you restart a consumer group member, it will
restart from offset zero (unless you've mocked `OffsetFetch`; see [offset-fetch.md](offset-fetch.md)).

## Using ETS

The mock broker provides helper functions for storing committed offsets in ETS. These can be used together for both
`OffsetFetch` and `OffsetCommit`.

```erlang
% If your shell process dies, the ETS table will be deleted; wrap in spawn() if you don't want that.
ets:new(committed_offsets, [named_table, public]).
```

```erlang
meck:new(kamock_offset_commit, [passthrough]).

meck:expect(
    kamock_offset_commit,
    handle_offset_commit_request,
    kamock_offset_commit:to_ets(committed_offsets)
).
```

```erlang
meck:new(kamock_offset_fetch, [passthrough]).

meck:expect(
    kamock_offset_fetch,
    handle_offset_fetch_request,
    kamock_offset_fetch:from_ets(committed_offsets)
).
```

The committed offsets are stored as `{_Key = {Group, Topic, Partition}, _Value = {Offset, Metadata}}`.

You can see the committed offsets with `ets:tab2list(committed_offsets)`, and you can update them as follows:

```erlang
lists:foreach(fun(P) ->
    kamock_offset_commit:insert(committed_offsets, <<"group">>, <<"cars">>, P, 65431)
end, [0, 1, 2, 3]).
```
