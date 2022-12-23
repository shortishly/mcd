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


-module(mcd_protocol_text_get_and_touch_SUITE).


-compile(export_all).
-compile(nowarn_export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").


all() ->
    common:all(?MODULE).


init_per_suite(Config) ->
    _ = application:load(mcd),
    application:set_env(mcd, protocol_callback, mcd_emulator),
    {ok, _} = mcd:start(),
    {ok, Client} = mcd_client:start(),
    [{client, Client} | Config].


end_per_suite(Config) ->
    Client = ?config(client, Config),
    ok = gen_statem:stop(Client),
    ok = application:stop(mcd).


cache_miss_test_todo(Config) ->
    Key = alpha(5),

    ?assertMatch(
       #{command := 'end'},
       send_sync(
         Config,
         #{command => gat,
           expiry => 10,
           keys => [Key]})).


set_with_expiry_get_and_touch_with_cas_test_todo(Config) ->
    K1 = alpha(5),
    V1 = alpha(5),
    Flags = 0,

    ?assertMatch(
       #{command := stored},
       send_sync(
         Config,
         #{command => set,
           key => K1,
           flags => Flags,
           expiry => 2,
           noreply => false,
           data => V1})),

    K2 = alpha(5),
    V2 = alpha(5),

    ?assertMatch(
       #{command := stored},
       send_sync(
         Config,
         #{command => set,
           key => K2,
           flags => Flags,
           expiry => 2,
           noreply => false,
           data => V2})),

    ?assertMatch(
       #{command := 'end'},
       send_sync(
         Config,
         #{command => gats,
           expiry => 10,
           keys => [K1, K2]})).


send_sync(Config, Data) ->
    Client = ?config(client, Config),

    {reply, Reply} = gen_statem:receive_response(
                       mcd_client:send(
                         #{server_ref => Client,
                           data => Data})),

    Reply.


request(Opcode) ->
    #{meta => Opcode}.


alpha(N) ->
    list_to_binary(pick(N, lists:seq($a, $z))).


pick(N, Pool) ->
    ?FUNCTION_NAME(N, Pool, []).


pick(0, _, A) ->
    A;

pick(N, Pool, A) ->
    ?FUNCTION_NAME(N - 1,
                   Pool,
                   [lists:nth(rand:uniform(length(Pool)), Pool) | A]).
