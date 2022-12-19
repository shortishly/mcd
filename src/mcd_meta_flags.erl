%% Copyright (c) 2022 Peter Morgan <peter.james.morgan@gmail.com>
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.


-module(mcd_meta_flags).


-export([as_map/2]).
-export([decode/1]).
-export([encode/1]).
-export_type([flag/0]).


-type flag() :: base64
              | cas
              | {cas_expected, mcd:uint32()}
              | flags
              | {flags, mcd:uint32()}
              | hit
              | invalidate
              | key_as_token
              | last_accessed
              | {mode, binary()}
              | {vivify, mcd:uint32()}
              | {opaque, binary()}
              | noreply
              | {remaining, mcd:uint32()}
              | size
              | ttl
              | {ttl, mcd:uint32()}
              | {set_ttl, mcd:uint32()}
              | stale
              | dont_bump_lru
              | value
              | already_winning
              | won.


-spec decode(binary()) -> [flag()].


decode(<<>>) ->
    [];

decode(Encoded) ->
    lists:map(
      fun demarshal/1,
      string:split(
        string:trim(Encoded),
        whitespace(),
        all)).


-spec encode([flag()]) -> iolist().

encode(Decoded) ->
    lists:join(
      " ",
      lists:map(fun marshal/1, Decoded)).


whitespace() ->
    " ".


demarshal(<<"C", Token/bytes>>) -> ?FUNCTION_NAME(cas_expected, Token);
demarshal(<<"D", Token/bytes>>) -> ?FUNCTION_NAME(delta, Token);
demarshal(<<"F", Token/bytes>>) -> ?FUNCTION_NAME(flags, Token);
demarshal(<<"I">>) -> invalidate;
demarshal(<<"J", Token/bytes>>) -> ?FUNCTION_NAME(initial, Token);
demarshal(<<"M", Token/bytes>>) -> {mode, Token};
demarshal(<<"N", Token/bytes>>) -> ?FUNCTION_NAME(vivify, Token);
demarshal(<<"O", Token/bytes>>) -> {opaque, Token};
demarshal(<<"R", Token/bytes>>) -> ?FUNCTION_NAME(remaining, Token);
demarshal(<<"T", Token/bytes>>) -> ?FUNCTION_NAME(set_ttl, Token);
demarshal(<<"W">>) -> won;
demarshal(<<"X">>) -> stale;
demarshal(<<"Z">>) -> already_winning;
demarshal(<<"b">>) -> base64;
demarshal(<<"c">>) -> cas;
demarshal(<<"c", Token/bytes>>) -> ?FUNCTION_NAME(cas, Token);
demarshal(<<"f">>) -> flags;
demarshal(<<"h">>) -> hit;
demarshal(<<"h", Token/bytes>>) -> ?FUNCTION_NAME(hit, Token);
demarshal(<<"k", Token/bytes>>) -> {key, Token};
demarshal(<<"k">>) -> key;
demarshal(<<"l">>) -> last_accessed;
demarshal(<<"l", Token/bytes>>) -> ?FUNCTION_NAME(last_accessed, Token);
demarshal(<<"q">>) -> noreply;
demarshal(<<"s">>) -> size;
demarshal(<<"s", Size/bytes>>) -> {size, binary_to_integer(Size)};
demarshal(<<"t">>) -> ttl;
demarshal(<<"t", Token/bytes>>) -> ?FUNCTION_NAME(ttl, Token);
demarshal(<<"u">>) -> dont_bump_lru;
demarshal(<<"v">>) -> value.


demarshal(Flag, Token) -> {Flag, binary_to_integer(Token)}.


marshal(base64) -> "b";
marshal(cas) -> "c";
marshal(dont_bump_lru) -> "u";
marshal(flags) -> "f";
marshal(hit) -> "h";
marshal(invalidate) -> "I";
marshal(key) -> "k";
marshal(last_accessed) -> "l";
marshal(noreply) -> "q";
marshal(size) -> "s";
marshal(ttl) -> "t";
marshal(value) -> "v";
marshal({cas, Token}) -> ?FUNCTION_NAME("c", Token);
marshal({cas_expected, Token}) -> ?FUNCTION_NAME("C", Token);
marshal({delta, Token}) -> ?FUNCTION_NAME("D", Token);
marshal({flags, Token}) -> ?FUNCTION_NAME("F", Token);
marshal({hit, Token}) ->  ?FUNCTION_NAME("h", Token);
marshal({initial, Token}) -> ?FUNCTION_NAME("J", Token);
marshal({key, Token}) -> ["k", Token];
marshal({last_accessed, Token}) ->  ?FUNCTION_NAME("l", Token);
marshal({mode, Token}) -> ["M", Token];
marshal({opaque, Token}) -> ["O", Token];
marshal({remaining, Token}) -> ?FUNCTION_NAME("R", Token);
marshal({size, Token}) -> ?FUNCTION_NAME("s", Token);
marshal({set_ttl, Token}) -> ?FUNCTION_NAME("T", Token);
marshal({ttl, Token}) -> ?FUNCTION_NAME("t", Token);
marshal({vivify, Token}) -> ?FUNCTION_NAME("N", Token).


marshal(Flag, Token) -> [Flag, integer_to_binary(Token)].


as_map(Meta, Flags) ->
    maps:merge(
      defaults(Meta),
      lists:foldl(
        fold(Meta),
        #{},
        Flags)).


defaults(set) ->
   #{mode => set};

defaults(arithmetic) ->
    #{initial => 0, delta => 1, mode => increment};

defaults(_) ->
    #{}.


fold(set) ->
    fun
        ({mode = K, <<"E">>}, A) ->
            A#{K => add};

        ({mode = K, <<"A">>}, A) ->
            A#{K => append};

        ({mode = K, <<"P">>}, A) ->
            A#{K => prepend};

        ({mode = K, <<"R">>}, A) ->
            A#{K => replace};

        ({mode = K, <<"S">>}, A) ->
            A#{K => set};

        ({mode, _} = Arg, _) ->
            error(badarg, [Arg]);

        ({K, V}, A) ->
            A#{K => V};

        (K, A) ->
            A#{K => true}
    end;

fold(arithmetic) ->
    fun
        ({mode = K, V}, A) when V == <<"I">>; V == <<"+">> ->
            A#{K => increment};

        ({mode = K, V}, A) when V == <<"D">>; V == <<"-">> ->
            A#{K => decrement};

        ({K, V}, A) ->
            A#{K => V};

        (K, A) ->
            A#{K => true}
    end;

fold(_) ->
    fun
        ({K, V}, A) ->
            A#{K => V};

        (K, A) ->
            A#{K => true}
    end.
