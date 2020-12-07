%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 12月 2020 14:19
%%%-------------------------------------------------------------------
-module(emqx_backend_opentsdb_sup).

-include("../include/emqx_backend_opentsdb.hrl").
-behaviour(supervisor).
-export([start_link/1]).

-export([init/1]).


start_link(Pools) ->
  supervisor:start_link({local, emqx_backend_opentsdb_sup}, emqx_backend_opentsdb_sup, [Pools]).

init([Pools]) ->
  {ok, {{one_for_all, 10, 100}, [pool_spec(Pool, Env) || {Pool, Env} <- Pools]}}.

pool_spec(Pool, Env) ->
  ecpool:pool_spec({emqx_backend_opentsdb, Pool}, emqx_backend_opentsdb:pool_name(Pool), emqx_backend_opentsdb_cli, Env).