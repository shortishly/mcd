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


-module(mcd_time).


-export([monotonic_to_system/3]).
-export([system_to_monotonic/3]).


system_to_monotonic(SystemTime, FromUnit, ToUnit) ->
    erlang:convert_time_unit(
      SystemTime - erlang:time_offset(FromUnit),
      FromUnit,
      ToUnit).


monotonic_to_system(MonotonicTime, FromUnit, ToUnit) ->
    erlang:convert_time_unit(
      MonotonicTime + erlang:time_offset(FromUnit),
      FromUnit,
      ToUnit).
