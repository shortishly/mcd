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


-module(mcd_config).


-export([protocol/1]).
-export([maximum/1]).
-export([memcached/1]).
-export([socket/1]).
-import(envy, [envy/2]).
-import(envy, [envy/3]).


maximum(value_size = Name) ->
    envy(to_integer, [?FUNCTION_NAME, Name], 1024 * 1024).


memcached(port = Name) ->
    envy(to_integer, [?FUNCTION_NAME, Name], 11211);

memcached(hostname = Name) ->
    envy(to_list, [?FUNCTION_NAME, Name], "localhost").


socket(backlog = Name) ->
    envy(to_integer, [?FUNCTION_NAME, Name], 5).


protocol(callback = Name) ->
    envy(to_atom, [?FUNCTION_NAME, Name]).
