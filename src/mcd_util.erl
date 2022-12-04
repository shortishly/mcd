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


-module(mcd_util).


-export([max/1]).
-export([min/1]).
-export([snake_case/1]).
-export([split/1]).
-include("mcd.hrl").


snake_case([_ | _] = Labels) ->
    list_to_atom(lists:concat(lists:join("_", Labels))).


split(S) ->
  string:split(S, ?RN).


min(int8) -> -128;
min(int16) -> -32_768;
min(int32) -> -2_147_483_648;
min(int64) -> -9_223_372_036_854_775_808;

min(uint8) -> 0;
min(uint16) -> 0;
min(uint32) -> 0;
min(uint64) -> 0.


max(int8) -> 127;
max(int16) -> 32_767;
max(int32) -> 2_147_483_647;
max(int64) -> 9_223_372_036_854_775_807;

max(uint8) -> 255;
max(uint16) -> 65_535;
max(uint32) -> 4_294_967_295;
max(uint64) -> 18_446_744_073_709_551_615.
