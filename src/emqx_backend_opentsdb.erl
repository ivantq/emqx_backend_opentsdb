%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 12月 2020 14:18
%%%-------------------------------------------------------------------
-module(emqx_backend_opentsdb).

-export(['$logger_header'/0]).

-include("../include/emqx_backend_opentsdb.hrl").

%% -include("/emqx/include/emqx.hrl").

%% -include("emqx/include/logger.hrl").

-export([pool_name/1]).

-export([register_metrics/0, load/0, unload/0]).

-export([on_message_publish/2]).


pool_name(Pool) ->
  list_to_atom(lists:concat([emqx_backend_opentsdb, '_', Pool])).

register_metrics() ->
  [emqx_metrics:new(MetricName) || MetricName <- ['backend.opentsdb.message_publish']].

load() ->
  HookList = parse_hook(application:get_env(emqx_backend_opentsdb, hooks, [])),
  Templates = emqx_backend_opentsdb_cli:get_templates(),
  lists:foreach(fun ({Hook, Action, Pool, Filter}) ->
    load_(Hook, b2a(proplists:get_value(<<"function">>, Action)), {Filter, Pool, Templates}) end, HookList),
  io:format("~s is loaded.~n", [emqx_backend_opentsdb]),
  ok.

load_(Hook, Fun, Params) ->
  case Hook of
    'message.publish' -> emqx:hook(Hook, fun emqx_backend_opentsdb:Fun/2, [Params])
  end.

unload() ->
  HookList = parse_hook(application:get_env(emqx_backend_opentsdb, hooks, [])),
  lists:foreach(fun ({Hook, Action, _Pool, _Filter}) ->
    case proplists:get_value(<<"function">>, Action) of
      undefined -> ok;
      Fun -> unload_(Hook, b2a(Fun))
    end
                end,
    HookList),
  io:format("~s is unloaded.~n", [emqx_backend_opentsdb]),
  ok.

unload_(Hook, Fun) ->
  case Hook of
    'message.publish' -> emqx:unhook(Hook, fun emqx_backend_opentsdb:Fun/2)
  end.

on_message_publish(Message = #message{flags =
#{retain := true}, payload = <<>>}, _Params) -> {ok, Message};
on_message_publish(Message = #message{topic = Topic}, {Filter, Pool, Templates}) ->
  with_filter(fun () ->
    try emqx_backend_opentsdb_cli:build_points(Message,
      Templates)
    of
      Points ->
        emqx_metrics:inc('backend.opentsdb.message_publish'),
        emqx_backend_opentsdb_cli:put(Pool, [maps:with([<<"metric">>, <<"timestamp">>, <<"value">>, <<"tags">>], Point) || Point <- Points])
    catch
      error:no_available_template:_Stacktrace ->
        begin
          logger:log(debug, #{}, #{report_cb =>
            fun (_) ->
              {'$logger_header'()
                ++
                "Build OpenTSDB point failed: no_available_tem"
                "plate",
                []}
            end,
              mfa => {emqx_backend_opentsdb, on_message_publish, 2}, line => 79})
        end;
      error:Reason:Stacktrace ->
        begin
          logger:log(error, #{}, #{report_cb =>
            fun (_) ->
              {'$logger_header'()
                ++
                "Build OpenTSDB point failed: ~p, ~p",
                [Reason,
                  Stacktrace]}
            end,
              mfa =>
              {emqx_backend_opentsdb,
                on_message_publish,
                2},
              line => 81})
        end
    end,
    {ok, Message} end,
    Message,
    Topic,
    Filter).

parse_hook(Hooks) -> parse_hook(Hooks, []).

parse_hook([], Acc) -> Acc;
parse_hook([{Hook, Item} | Hooks], Acc) ->
  Params = emqx_json:decode(Item),
  Action = proplists:get_value(<<"action">>, Params),
  Pool = proplists:get_value(<<"pool">>, Params),
  Filter = proplists:get_value(<<"topic">>, Params),
  parse_hook(Hooks,
    [{l2a(Hook), Action, pool_name(b2a(Pool)), Filter}
      | Acc]).

with_filter(Fun, _, _, undefined) -> Fun();
with_filter(Fun, Msg, Topic, Filter) ->
  case emqx_topic:match(Topic, Filter) of
    true -> Fun();
    false -> {ok, Msg}
  end.

l2a(L) -> erlang:list_to_atom(L).

b2a(B) -> erlang:binary_to_atom(B, utf8).

'$logger_header'() -> "".
