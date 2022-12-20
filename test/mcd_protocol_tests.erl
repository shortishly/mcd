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


decode_reply_expected_test_() ->
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
       t(fun decode_reply_expected/1),
       [{"binary/add-request.terms", true},
        {"binary/append-request.terms", true},
        {"binary/delete-request.terms", true},

        {"binary/flush-request.terms", true},

        {"binary/get-request.terms", true},
        {"binary/increment-request.terms", true},

        {"binary/no-op-request.terms", true},

        {"binary/quit-request.terms", true},

        {"binary/stat-request.terms", true},

        {"binary/version-request.terms", true},

        {"text/add-noreply-request.terms", false},

        {"text/get-request.terms", true},
        {"text/gets-request.terms", true},

        {"text/set-request.terms", true},
        {"text/replace-request.terms", true},
        {"text/append-request.terms", true},
        {"text/prepend-request.terms", true},

        {"text/cas-request.terms", true},

        {"text/delete-noreply-request.terms", false},
        {"text/delete-request.terms", true},

        {"text/decr-request.terms", true},
        {"text/decr-noreply-request.terms", false},

        {"text/flush-all-request.terms", true},
        {"text/flush-all-abs-expiry-request.terms", true},
        {"text/flush-all-noreply-request.terms", false},


        {"text/incr-request.terms", true},
        {"text/incr-noreply-request.terms", false},

        {"text/touch-request.terms", true},
        {"text/touch-noreply-request.terms", false},

        {"text/gat-request.terms", true},
        {"text/gats-request.terms", true},


        {"text/stats-request.terms", true},

        {"text/verbosity-request.terms", true},
        {"text/verbosity-noreply-request.terms", false},

        {"text/quit-request.terms", true},

        {"text/meta-debug-request.terms", true},
        {"text/meta-debug-flags-request.terms", true},

        {"text/meta-delete-request.terms", true},
        {"text/meta-set-add-request-001.terms", true},
        {"text/meta-set-request-001.terms", true},
        {"text/meta-set-request-002.terms", true},

        {"text/meta-no-op-request.terms", true},

        {"text/meta-arithmetic-request-001.terms", true},

        {"text/meta-get-request.terms", true},
        {"text/meta-get-flags-request.terms", true}])}.


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

        "binary/no-op-request.terms",
        "binary/no-op-response.terms",

        "binary/quit-request.terms",

        "binary/stat-request.terms",
        "binary/stat-response.terms",
        "binary/stat-empty-response.terms",

        "binary/version-request.terms",
        "binary/version-response.terms",


        "text/add-noreply-request.terms",

        "text/get-request.terms",
        "text/gets-request.terms",
        "text/value-response.terms",
        "text/value-cas-response.terms",

        "text/set-request.terms",
        "text/replace-request.terms",
        "text/append-request.terms",
        "text/prepend-request.terms",

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

        "text/flush-all-request.terms",
        "text/flush-all-abs-expiry-request.terms",
        "text/flush-all-noreply-request.terms",

        "text/ok-response.terms",

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

        "text/stat-response-float.terms",
        "text/stat-response-integer.terms",
        "text/stat-response-text.terms",
        "text/stats-request.terms",

        "text/verbosity-request.terms",
        "text/verbosity-noreply-request.terms",

        "text/quit-request.terms",

        "text/meta-debug-request.terms",
        "text/meta-debug-flags-request.terms",
        "text/meta-debug-response.terms",

        "text/meta-en-miss-response.terms",
        "text/meta-hd-stored-response.terms",
        "text/meta-nf-not-found-response.terms",
        "text/meta-ns-not-stored-response.terms",

        "text/meta-delete-request.terms",
        "text/meta-set-add-request-001.terms",
        "text/meta-set-request-001.terms",
        "text/meta-set-request-002.terms",

        "text/meta-no-op-request.terms",
        "text/meta-no-op-response.terms",

        "text/meta-arithmetic-request-001.terms",

        "text/meta-get-request.terms",
        "text/meta-get-flags-request.terms",
        "text/meta-get-response.terms"])}.


decode_encode(Encoded) ->
    {Decoded, <<>>} = mcd_protocol:decode(
                        iolist_to_binary(Encoded)),
    mcd_protocol:encode(Decoded).

decode_reply_expected(Encoded) ->
    {Decoded, <<>>} = mcd_protocol:decode(
                        iolist_to_binary(Encoded)),
    mcd_protocol:reply_expected(Decoded).


t(F) ->
    fun
        ({Filename, ExpectedResult}) ->
            {ok, Encoded} = file:consult(filename:join("test", Filename)),
            {nm(Filename), ?_assertEqual(ExpectedResult, F(Encoded))};

        (Filename) ->
            {ok, Encoded} = file:consult(filename:join("test", Filename)),
            {nm(Filename),
             ?_assertEqual(iolist_to_binary(Encoded),
                           iolist_to_binary(F(Encoded)))}
    end.


nm(Test) ->
    iolist_to_binary(io_lib:fwrite("~p", [Test])).
