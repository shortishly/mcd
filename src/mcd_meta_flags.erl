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


-export([decode/1]).
-export([encode/1]).
-export_type([flag/0]).


-type flag() :: base64
              | cas
              | {cas, mcd:uint32()}
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
              | dont_bump_lru
              | value.


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


demarshal(<<"b">>) -> base64;
demarshal(<<"c">>) -> cas;
demarshal(<<"C", Token/bytes>>) -> ?FUNCTION_NAME(cas, Token);
demarshal(<<"f">>) -> flags;
demarshal(<<"F", Token/bytes>>) -> ?FUNCTION_NAME(flags, Token);
demarshal(<<"h">>) -> hit;
demarshal(<<"h", Token/bytes>>) -> ?FUNCTION_NAME(hit, Token);
demarshal(<<"I">>) -> invalidate;
demarshal(<<"k">>) -> key_as_token;
demarshal(<<"l">>) -> last_accessed;
demarshal(<<"l", Token/bytes>>) -> ?FUNCTION_NAME(last_accessed, Token);
demarshal(<<"M", Mode/bytes>>) -> {mode, Mode};
demarshal(<<"N", Token/bytes>>) -> ?FUNCTION_NAME(vivify, Token);
demarshal(<<"O", Token/bytes>>) -> {opaque, Token};
demarshal(<<"q">>) -> noreply;
demarshal(<<"R", Token/bytes>>) -> ?FUNCTION_NAME(remaining, Token);
demarshal(<<"s">>) -> size;
demarshal(<<"s", Size/bytes>>) -> {size, binary_to_integer(Size)};
demarshal(<<"t">>) -> ttl;
demarshal(<<"T", Token/bytes>>) -> ?FUNCTION_NAME(ttl, Token);
demarshal(<<"u">>) -> dont_bump_lru;
demarshal(<<"v">>) -> value.


demarshal(Flag, Token) -> {Flag, binary_to_integer(Token)}.


marshal(base64) -> "b";
marshal(cas) -> "c";
marshal({cas, Token}) -> ?FUNCTION_NAME("C", Token);
marshal(flags) -> "f";
marshal({flags, Token}) -> ?FUNCTION_NAME("F", Token);
marshal(hit) -> "h";
marshal({hit, Token}) ->  ?FUNCTION_NAME("h", Token);
marshal(invalidate) -> "I";
marshal(key_as_token) -> "k";
marshal(last_accessed) -> "l";
marshal({last_accessed, Token}) ->  ?FUNCTION_NAME("l", Token);
marshal({mode, Token}) -> ["M", Token];
marshal({vivify, Token}) -> ?FUNCTION_NAME("N", Token);
marshal({opaque, Token}) -> ["O", Token];
marshal(noreply) -> "q";
marshal({remaining, Token}) -> ?FUNCTION_NAME("R", Token);
marshal({size, Token}) -> ?FUNCTION_NAME("s", Token);
marshal(size) -> "s";
marshal(ttl) -> "t";
marshal({ttl, Token}) -> ?FUNCTION_NAME("T", Token);
marshal(dont_bump_lru) -> "u";
marshal(value) -> "v".


marshal(Flag, Token) -> [Flag, integer_to_binary(Token)].
