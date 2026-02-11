-module(kamock_coordinator_election).
-export([elect/1]).

-export([
    choose/1,
    reject/1
]).

elect(Joiners) ->
    % Naive election: pick the first joiner as leader.
    % Note that a real broker would remember the previous leader and use them again, if they're joining again.
    % * We assume that all members have provided _exactly_ the same list of protocols. *
    [Leader | Followers] = Joiners,
    {Leader, Followers}.

-type member_id() :: binary().

-type joiner_ref() :: gen_statem:from().
-type joiner() :: {joiner_ref(), join_group_request:join_group_request_7()}.
-type election_fun() :: fun(([joiner()]) -> {Leader :: joiner(), Followers :: [joiner()]}).

-spec choose(Preferred :: member_id()) -> election_fun().

choose(Preferred) ->
    fun(Joiners) ->
        Pred = fun({_, #{member_id := MemberId} = _Req} = _Joiner) ->
            MemberId == Preferred
        end,
        {value, Leader} = lists:search(Pred, Joiners),
        Followers = Joiners -- [Leader],
        {Leader, Followers}
    end.

-spec reject(Rejected :: member_id()) -> election_fun().

reject(Rejected) ->
    fun(Joiners) ->
        Pred = fun({_, #{member_id := MemberId} = _Req} = _Joiner) ->
            MemberId /= Rejected
        end,
        {value, Leader} = lists:search(Pred, Joiners),
        Followers = Joiners -- [Leader],
        {Leader, Followers}
    end.
