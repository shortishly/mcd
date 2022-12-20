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


-module(mcd_protocol).


-export([decode/1]).
-export([encode/1]).
-export([expected_reply_count/1]).
-export([reply_expected/1]).
-include("mcd.hrl").
-include_lib("kernel/include/logger.hrl").


-callback init([]) -> {ok, callback_data()}.

-type callback_data() :: any().

-callback recv(recv_request()) -> recv_response().

-type recv_request() :: #{message := mcd:protocol(),
                          data := callback_data()}.


-type recv_response() :: {continue, continue_response()}
                       | {continue, [continue_response()]}
                       | {stop, any()}
                       | stop.

-type continue_response() :: {encode, mcd:protocol()}
                           | {expire, #{key := binary(), seconds := integer()}}.

-callback expire(expire_request()) -> expire_response().

-type expire_request() :: #{key := binary(), data := callback_data()}.

-type expire_response() :: ok
                         | {stop, any()}
                         | stop.

-callback flush_all(flush_all_request()) -> flush_all_response().

-type flush_all_request() :: #{data := callback_data()}.

-type flush_all_response() :: ok
                            | {stop, any()}
                            | stop.

-spec decode(binary()) -> {mcd:protocol(), binary()} | partial | {error, atom()}.

decode(<<"STAT ", Remainder/bytes>>) ->
    mcd_protocol_text:decode(stat, Remainder);

decode(<<"stats", Remainder/bytes>>) ->
    mcd_protocol_text:decode(stats, Remainder);

decode(<<"stat", Remainder/bytes>>) ->
    mcd_protocol_text:decode(stat, Remainder);

decode(<<"quit", Remainder/bytes>>) ->
    mcd_protocol_text:decode(quit, Remainder);

decode(<<"flush_all", Remainder/bytes>>) ->
    mcd_protocol_text:decode(flush_all, Remainder);

decode(<<"verbosity ", Remainder/bytes>>) ->
    mcd_protocol_text:decode(verbosity, Remainder);

decode(<<"set ", Remainder/bytes>>) ->
    mcd_protocol_text:decode(set, Remainder);

decode(<<"cas ", Remainder/bytes>>) ->
    mcd_protocol_text:decode(cas, Remainder);

decode(<<"add ", Remainder/bytes>>) ->
    mcd_protocol_text:decode(add, Remainder);

decode(<<"replace ", Remainder/bytes>>) ->
    mcd_protocol_text:decode(replace, Remainder);

decode(<<"append ", Remainder/bytes>>) ->
    mcd_protocol_text:decode(append, Remainder);

decode(<<"prepend ", Remainder/bytes>>) ->
    mcd_protocol_text:decode(prepend, Remainder);

decode(<<"get ", Remainder/bytes>>) ->
    mcd_protocol_text:decode(get, Remainder);

decode(<<"gets ", Remainder/bytes>>) ->
    mcd_protocol_text:decode(gets, Remainder);

decode(<<"gat ", Remainder/bytes>>) ->
    mcd_protocol_text:decode(gat, Remainder);

decode(<<"gats ", Remainder/bytes>>) ->
    mcd_protocol_text:decode(gats, Remainder);

decode(<<"delete ", Remainder/bytes>>) ->
    mcd_protocol_text:decode(delete, Remainder);

decode(<<"incr ", Remainder/bytes>>) ->
    mcd_protocol_text:decode(incr, Remainder);

decode(<<"decr ", Remainder/bytes>>) ->
    mcd_protocol_text:decode(decr, Remainder);

decode(<<"touch ", Remainder/bytes>>) ->
    mcd_protocol_text:decode(touch, Remainder);

decode(<<"VALUE ", Remainder/bytes>>) ->
    mcd_protocol_text:decode(value, Remainder);

decode(<<"CLIENT_ERROR ", Remainder/bytes>>) ->
    mcd_protocol_text:decode(client_error, Remainder);

decode(<<"SERVER_ERROR ", Remainder/bytes>>) ->
    mcd_protocol_text:decode(server_error, Remainder);

decode(<<"OK\r\n", Remainder/bytes>>) ->
    {#{command => ok}, Remainder};

decode(<<"ERROR\r\n", Remainder/bytes>>) ->
    {#{command => error}, Remainder};

decode(<<"TOUCHED\r\n", Remainder/bytes>>) ->
    {#{command => touched}, Remainder};

decode(<<"STORED\r\n", Remainder/bytes>>) ->
    {#{command => stored}, Remainder};

decode(<<"NOT_STORED\r\n", Remainder/bytes>>) ->
    {#{command => not_stored}, Remainder};

decode(<<"EXISTS\r\n", Remainder/bytes>>) ->
    {#{command => exists}, Remainder};

decode(<<"NOT_FOUND\r\n", Remainder/bytes>>) ->
    {#{command => not_found}, Remainder};

decode(<<"END\r\n", Remainder/bytes>>) ->
    {#{command => 'end'}, Remainder};

decode(<<"DELETED\r\n", Remainder/bytes>>) ->
    {#{command => deleted}, Remainder};

decode(<<"ma ", Remainder/bytes>>) ->
    mcd_protocol_meta:decode(arithmetic, Remainder);

decode(<<"me ", Remainder/bytes>>) ->
    mcd_protocol_meta:decode(debug, Remainder);

decode(<<"ME ", Remainder/bytes>>) ->
    mcd_protocol_meta:decode(debug_reply, Remainder);

decode(<<"mg ", Remainder/bytes>>) ->
    mcd_protocol_meta:decode(get, Remainder);

decode(<<"mn", Remainder/bytes>>) ->
    mcd_protocol_meta:decode(no_op, Remainder);

decode(<<"ms ", Remainder/bytes>>) ->
    mcd_protocol_meta:decode(set, Remainder);

decode(<<"md ", Remainder/bytes>>) ->
    mcd_protocol_meta:decode(delete, Remainder);

decode(<<"VA ", Remainder/bytes>>) ->
    mcd_protocol_meta:decode(value, Remainder);

decode(<<"HD", Remainder/bytes>>) ->
    mcd_protocol_meta:decode(head, Remainder);

decode(<<"NS", Remainder/bytes>>) ->
    mcd_protocol_meta:decode(not_stored, Remainder);

decode(<<"EX", Remainder/bytes>>) ->
    mcd_protocol_meta:decode(exists, Remainder);

decode(<<"NF", Remainder/bytes>>) ->
    mcd_protocol_meta:decode(not_found, Remainder);

decode(<<"MN", Remainder/bytes>>) ->
    mcd_protocol_meta:decode(no_op_reply, Remainder);

decode(<<"EN\r\n", Remainder/bytes>>) ->
    {#{meta => miss}, Remainder};

decode(<<Magic:8,
         _:8,
         _:16,
         _:8,
         ?RAW:8,
         _:16,
         TotalBodyLength:32,
         _:32,
         _:64,
         _:TotalBodyLength/bytes,
         _/bytes>> = Arg) when Magic == ?REQUEST; Magic == ?RESPONSE ->
    mcd_protocol_binary:decode(Arg);

decode(Arg) ->
    ?LOG_ERROR(#{arg => Arg}),
    error(client_error).


encode(L) when is_list(L) ->
    lists:map(fun encode/1, L);

encode(#{meta := _} = Arg) ->
    mcd_protocol_meta:encode(Arg);

encode(#{command := _} = Arg) ->
    mcd_protocol_text:encode(Arg);

encode(#{header := #{opcode := _}} = Arg) ->
    mcd_protocol_binary:encode(Arg).


expected_reply_count(L) when is_list(L) ->
    lists:sum(
      lists:map(
        fun expected_reply_count/1,
        L));

expected_reply_count(#{header := _} = Arg) ->
    mcd_protocol_binary:?FUNCTION_NAME(Arg);

expected_reply_count(#{command := _} = Arg) ->
    mcd_protocol_text:?FUNCTION_NAME(Arg);

expected_reply_count(#{meta := _} = Arg) ->
    mcd_protocol_meta:?FUNCTION_NAME(Arg).


reply_expected(L) when is_list(L) ->
    lists:filter(fun ?FUNCTION_NAME/1, L);

reply_expected(#{command := _, noreply := Noreply}) ->
    not(Noreply);

reply_expected(#{meta := _, flags := Flags}) ->
    not(lists:member(noreply, Flags));

reply_expected(#{}) ->
    true.
