-module(kamock_batch_locator).
-export([
    fixed_size/1,
    round_robin/1
]).

fixed_size(BatchSize) ->
    % Implements fixed-size batches.
    fun(FirstOffset, LastOffset, FetchOffset) when
        FetchOffset >= FirstOffset, FetchOffset < LastOffset
    ->
        % Find the start of the batch.
        BaseOffset = round_down(FetchOffset, BatchSize),

        % We don't have to worry about going past the end of the partition; batches() will do that for us.
        {BaseOffset, BatchSize}
    end.

round_down(Value, To) ->
    (Value div To) * To.

round_robin(Sizes) when is_list(Sizes) ->
    fun(FirstOffset, LastOffset, FetchOffset) when
        FetchOffset >= FirstOffset, FetchOffset < LastOffset
    ->
        round_robin_search(FetchOffset, Sizes)
    end.

round_robin_search(FetchOffset, Sizes) ->
    round_robin_search(FetchOffset, 0, Sizes, Sizes).

round_robin_search(FetchOffset, CurrentOffset, [Size | _Rest], _Sizes) when
    CurrentOffset + Size > FetchOffset
->
    {CurrentOffset, Size};
round_robin_search(FetchOffset, CurrentOffset, [Size | Rest], Sizes) ->
    round_robin_search(FetchOffset, CurrentOffset + Size, Rest, Sizes);
round_robin_search(FetchOffset, CurrentOffset, [], Sizes) ->
    round_robin_search(FetchOffset, CurrentOffset, Sizes, Sizes).
