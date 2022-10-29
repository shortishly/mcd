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


-module(mcd_meta_flags_tests).


-include_lib("eunit/include/eunit.hrl").


decode_encode_test_() ->
     lists:map(
       t(fun decode_encode/1),
       ["b",
        "c",
        "C1234",
        "f",
        "F5678",
        "h",
        "h1",
        "I",
        "k",
        "l",
        "l5",
        "Mabcdef",
        "N32123",
        "Opaque",
        "q",
        "R12321",
        "s",
        "s2",
        "t",
        "T56765",
        "u",
        "v"]).


decode_encode(Encoded) ->
     mcd_meta_flags:encode(
       mcd_meta_flags:decode(
         iolist_to_binary(Encoded))).


t(F) ->
    fun
        (Encoded) ->
            {nm(Encoded),
             ?_assertEqual(iolist_to_binary(Encoded),
                           iolist_to_binary(F(Encoded)))}
    end.


nm(Test) ->
    iolist_to_binary(io_lib:fwrite("~p", [Test])).
