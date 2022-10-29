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


-module(mcd_protocol_tests).


-include_lib("eunit/include/eunit.hrl").


decode_encode_test_() ->
    {setup,
     fun
         () ->
             {ok, Opcode} = mcd_opcode:start_link(),
             {ok, Status} = mcd_status:start_link(),
             [Opcode, Status]
     end,
     fun
         (Procs) ->
             lists:foreach(
               fun gen_statem:stop/1,
               Procs)
     end,
     lists:map(
       t(fun decode_encode/1),
       ["binary/add-request.terms",
        "binary/add-response.terms",
        "binary/append-request.terms",
        "binary/delete-request.terms",

        "binary/flush-request.terms",
        "binary/flush-response.terms",

        "binary/get-request.terms",
        "binary/get-response.terms",
        "binary/getk-response.terms",
        "binary/increment-request.terms",
        "binary/increment-response.terms",

        "binary/noop-request.terms",
        "binary/noop-response.terms",

        "binary/quit-request.terms",

        "binary/stat-request.terms",
        "binary/stat-response.terms",
        "binary/stat-empty-response.terms",

        "binary/version-request.terms",
        "binary/version-response.terms",

        "text/get-request.terms",
        "text/gets-request.terms",
        "text/value-response.terms",
        "text/value-cas-response.terms",

        "text/set-request.terms",
        "text/cas-request.terms",

        "text/stored-response.terms",
        "text/not-stored-response.terms",
        "text/exists-response.terms",
        "text/not-found-response.terms",
        "text/deleted-response.terms",

        "text/delete-noreply-request.terms",
        "text/delete-request.terms",

        "text/decr-request.terms",
        "text/decr-noreply-request.terms",

        "text/incr-request.terms",
        "text/incr-noreply-request.terms",

        "text/touch-request.terms",
        "text/touch-noreply-request.terms",
        "text/touched-response.terms",

        "text/gat-request.terms",
        "text/gats-request.terms",

        "text/error-response.terms",
        "text/client-error-response.terms",
        "text/server-error-response.terms",

        "text/end-response.terms",

        "text/meta-debug-request.terms",
        "text/meta-debug-flags-request.terms",
        "text/meta-debug-response.terms",

        "text/meta-en-miss-response.terms",
        "text/meta-hd-stored-response.terms",
        "text/meta-nf-not-found-response.terms",
        "text/meta-ns-not-stored-response.terms",

        "text/meta-set-request-001.terms",
        "text/meta-set-request-002.terms",
        "text/meta-delete-request.terms",
        "text/meta-get-request.terms",
        "text/meta-get-response.terms"])}.


decode_encode(Encoded) ->
    {Decoded, <<>>} = mcd_protocol:decode(
                        iolist_to_binary(Encoded)),
    mcd_protocol:encode(Decoded).


t(F) ->
    fun
        (Filename) ->
            {ok, Encoded} = file:consult(filename:join("test", Filename)),
            {nm(Filename),
             ?_assertEqual(iolist_to_binary(Encoded),
                           iolist_to_binary(F(Encoded)))}
    end.


nm(Test) ->
    iolist_to_binary(io_lib:fwrite("~p", [Test])).
