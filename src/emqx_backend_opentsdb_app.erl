%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 12月 2020 14:18
%%%-------------------------------------------------------------------
-module(emqx_backend_opentsdb_app).

-include("../include/emqx_backend_opentsdb.hrl").


-behaviour(application).

-emqx_plugin(backend).

-export([start/2, stop/1]).


start(_Type, _Args) ->
%%  解析应用池配置
  Pools = application:get_env(emqx_backend_opentsdb, pools, []),

%%  启动应用监控进程
  {ok, Sup} = emqx_backend_opentsdb_sup:start_link(Pools),
%%  注册统计
  emqx_backend_opentsdb:register_metrics(),
%%  载入模块
  emqx_backend_opentsdb:load(),
  {ok, Sup}.

%% 卸载模块
stop(_State) -> emqx_backend_opentsdb:unload().
