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


-module(mcd_protocol_text_cas_SUITE).


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


set_test(Config) ->
    Key = alpha(5),
    Value = alpha(5),
    Flags = 0,

    ?assertMatch(
       #{command := 'end'},
       send_sync(
         Config,
         #{command => get,
           keys => [Key]})),

    ?assertMatch(
       #{command := stored},
       send_sync(
         Config,
         #{command => set,
           key => Key,
           flags => Flags,
           expiry => 0,
           noreply => false,
           data => Value})),


    ?assertMatch(
       [#{command := value,
          key := Key,
          flags := Flags,
          data := Value},
        #{command := 'end'}],
       send_sync(
         Config,
         #{command => get,
           keys => [Key]})),

    ?assertMatch(
       #{command := exists},
       send_sync(
         Config,
         #{command => cas,
           key => Key,
           flags => Flags,
           expiry => 0,
           unique => 12321,
           noreply => false,
           data => Value})),

    [#{command := value,
       key := Key,
       cas := CAS,
       flags := Flags,
       data := Value},
     #{command := 'end'}] = send_sync(
                              Config,
                              #{command => gets,
                                keys => [Key]}),

    ?assertMatch(
       #{command := stored},
       send_sync(
         Config,
         #{command => cas,
           key => Key,
           flags => Flags,
           expiry => 0,
           unique => CAS,
           noreply => false,
           data => Value})),

    ?assertMatch(
       #{command := exists},
       send_sync(
         Config,
         #{command => cas,
           key => Key,
           flags => Flags,
           expiry => 0,
           unique => CAS,
           noreply => false,
           data => Value})),

    ?assertMatch(
       #{command := deleted},
       send_sync(
         Config,
         #{command => delete,
           key => Key,
           noreply => false})),

    ?assertMatch(
       #{command := not_found},
       send_sync(
         Config,
         #{command => cas,
           key => Key,
           flags => Flags,
           expiry => 0,
           unique => CAS,
           noreply => false,
           data => Value})).

bug15_test(Config) ->
    K = alpha(5),
    Flags = 0,

    ?assertMatch(
       #{command := stored},
       send_sync(
         Config,
         #{command => set,
           key => K,
           flags => Flags,
           expiry => 0,
           noreply => false,
           data => "1"})),

    [#{command := value,
       key := K,
       cas := CAS1,
       flags := Flags,
       data := <<"1">>},
     #{command := 'end'}] = send_sync(
                              Config,
                              #{command => gets,
                                keys => [K]}),

    ?assertMatch(
       #{command := incrdecr,
         value := 2},
       send_sync(
         Config,
         #{command => incr,
           key => K,
           noreply => false,
           value => 1})),

    [#{command := value,
       key := K,
       cas := CAS2,
       flags := Flags,
       data := <<"2">>},
     #{command := 'end'}] = send_sync(
                              Config,
                              #{command => gets,
                                keys => [K]}),
    ?assertNotEqual(CAS1, CAS2).



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
