%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 12月 2020 14:19
%%%-------------------------------------------------------------------
-file("emqx_backend_opentsdb_cli.erl", 1).

-module(emqx_backend_opentsdb_cli).

-export([logger_header/0]).
-include("../include/emqx_backend_opentsdb.hrl").

-include("../include/emqx.hrl").
-include("../include/logger.hrl").

-export([put/2, put/3, get_templates/0, build_points/2]).

-behaviour(ecpool_worker).

-export([connect/1]).


put(Pool, DataPoints) ->
  ecpool:with_client(Pool,
    fun (C) ->
      case opentsdb:put(C, DataPoints) of
        {error, Reason} ->
          begin
            logger:log(error,
              #{},
              #{report_cb =>
              fun (_) ->
                {logger_header()
                  ++
                  "Write ~p to OpenTSDB failed: ~p",
                  [DataPoints,
                    Reason]}
              end,
                mfa =>
                {emqx_backend_opentsdb_cli,
                  put,
                  2},
                line => 24})
          end;
        _ ->
          begin
            logger:log(debug,
              #{},
              #{report_cb =>
              fun (_) ->
                {logger_header()
                  ++
                  "Write point ~p to OpenTSDB successfully",
                  [DataPoints]}
              end,
                mfa =>
                {emqx_backend_opentsdb_cli,
                  put,
                  2},
                line => 25})
          end
      end
    end).

put(Pool, DataPoints, Options) ->
  ecpool:with_client(Pool,
    fun (C) ->
      case opentsdb:put(C, DataPoints, Options) of
        {error, Reason} ->
          begin
            logger:log(error,
              #{},
              #{report_cb =>
              fun (_) ->
                {logger_header()
                  ++
                  "Write ~p to OpenTSDB failed: ~p",
                  [DataPoints,
                    Reason]}
              end,
                mfa =>
                {emqx_backend_opentsdb_cli,
                  put,
                  3},
                line => 32})
          end;
        _ ->
          begin
            logger:log(debug,
              #{},
              #{report_cb =>
              fun (_) ->
                {logger_header()
                  ++
                  "Write point ~p to OpenTSDB successfully",
                  [DataPoints]}
              end,
                mfa =>
                {emqx_backend_opentsdb_cli,
                  put,
                  3},
                line => 33})
          end
      end
    end).

get_templates() ->
  FilePath = filename:join([application:get_env(emqx,
    data_dir,
    "./"),
    "templates",
    emqx_backend_opentsdb])
    ++ ".tmpl",
  case file:read_file(FilePath) of
    {error, Reason} ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Read ~p failed due to: ~p",
              [FilePath, posix_errno(Reason)]}
          end,
            mfa =>
            {emqx_backend_opentsdb_cli, get_templates, 0},
            line => 41})
      end,
      #{};
    {ok, <<>>} ->
      begin
        logger:log(debug,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++ "~p is empty",
              [FilePath]}
          end,
            mfa =>
            {emqx_backend_opentsdb_cli, get_templates, 0},
            line => 44})
      end,
      #{};
    {ok, TemplatesJson} ->
      emqx_json:decode(TemplatesJson, [return_maps])
  end.

build_points(Message = #message{topic = Topic}, Templates) ->
  case lists:foldl(fun ({Topic0, Template}, Acc) ->
    case emqx_topic:match(Topic, Topic0) of
      true -> [Template | Acc];
      false -> Acc
    end
                   end,
    [],
    maps:to_list(Templates))
  of
    [] -> error(no_available_template);
    Templates1 ->
      Data = available_fields(Message),
      lists:foldl(fun (Template, Acc) ->
        {MixedPoints, Marks} = prebuild_points(Template,
          Data,
          [],
          #{}),
        separate_points(MixedPoints, Marks) ++ Acc
                  end,
        [],
        Templates1)
  end.

available_fields(#message{id = Id, qos = QoS,
  from = ClientId, headers = Headers, topic = Topic,
  payload = Payload, timestamp = Timestamp}) ->
  #{<<"$id">> => Id, <<"$qos">> => QoS,
    <<"$clientid">> => ClientId,
    <<"$username">> =>
    maps:get(<<"$username">>, Headers, null),
    <<"$peerhost">> =>
    host_to_str(maps:get(<<"$peerhost">>, Headers, null)),
    <<"$topic">> => Topic,
    <<"$payload">> => {Payload, try_decode(Payload)},
    <<"$timestamp">> => Timestamp}.

try_decode(Data) ->
  try emqx_json:decode(Data, [return_maps]) of
    Json -> Json
  catch
    error:Reason:Stacktrace ->
      begin
        logger:log(error,
          #{},
          #{report_cb =>
          fun (_) ->
            {logger_header() ++
              "Decode ~p failed due to: ~p",
              [Data, {Reason, Stacktrace}]}
          end,
            mfa => {emqx_backend_opentsdb_cli, try_decode, 1},
            line => 87})
      end,
      #{}
  end.

prebuild_points(Template, Data, Keys, Marks)
  when is_map(Template) ->
  lists:foldl(fun (K, {Template0, Marks0}) ->
    case maps:get(K, Template) of
      V when is_map(V) ->
        {Data1, Marks1} = prebuild_points(V,
          Data,
          Keys ++ [K],
          Marks0),
        {maps:update(K, Data1, Template0), Marks1};
      V when is_binary(V); is_list(V) ->
        case get_val(V, Data) of
          V1 when is_list(V1) ->
            Marks1 = maps:put(maps:size(Marks0) + 1,
              Keys ++ [K],
              Marks0),
            {maps:update(K, V1, Template0), Marks1};
          V1 ->
            {maps:update(K, V1, Template0), Marks0}
        end;
      _ -> error(invalid_template)
    end
              end,
    {Template, Marks},
    maps:keys(Template)).

separate_points(MixedPoints, Marks)
  when Marks =:= #{} ->
  [MixedPoints];
separate_points(MixedPoints, Marks) ->
  maps:fold(fun (_, [], Acc) -> Acc;
    (_, Mark, Acc) ->
      case nested_get(Mark, MixedPoints) of
        V when is_list(V) ->
          case Acc of
            [] ->
              Acc1 = [MixedPoints
                || _ <- lists:seq(1, length(V))],
              [nested_put(Mark, V0, Acc0)
                || {Acc0, V0} <- lists:zip(Acc1, V)];
            Acc when length(Acc) =/= length(V) ->
              error({cannot_apply_template,
                different_length});
            Acc ->
              [nested_put(Mark, V0, Acc0)
                || {Acc0, V0} <- lists:zip(Acc, V)]
          end;
        _ -> error(unknown_error)
      end
            end,
    [],
    Marks).

get_val(<<"$payload">>,
    #{<<"$payload">> := {Raw, _}}) ->
  Raw;
get_val(Key = <<"$", Rest/binary>>, Data) ->
  try binary_to_integer(Rest) of
    _ -> error(invalid_template)
  catch
    error:_Reason ->
      case maps:get(Key, Data, undefined) of
        undefined ->
          error({invalid_template, invalid_placeholder});
        Val when is_map(Val) orelse is_list(Val) ->
          error({cannot_apply_template, invalid_data_type});
        Val -> Val
      end
  end;
get_val(Key, _Data) when is_binary(Key) -> Key;
get_val([], _Data) -> error(invalid_template);
get_val([<<"$payload">> | Rest],
    #{<<"$payload">> := {_, Payload}})
  when is_map(Payload) or is_list(Payload) ->
  get_val_(Rest, Payload);
get_val([<<"$payload">> | _Rest],
    #{<<"$payload">> := _}) ->
  error({cannot_apply_template, invalid_data_type});
get_val([_ | _Rest], _Data) -> error(invalid_template).

get_val_([], Data)
  when is_map(Data) orelse is_list(Data) ->
  error({invalid_template, invalid_data_type});
get_val_([], Data) -> Data;
get_val_([<<"$", N0/binary>> | Rest], Data)
  when is_list(Data) ->
  try binary_to_integer(N0) of
    0 ->
      lists:foldr(fun (D, Acc) ->
        case get_val_(Rest, D) of
          List when is_list(List) -> List ++ Acc;
          Other -> [Other | Acc]
        end
                  end,
        [],
        Data);
    N ->
      case N > length(Data) of
        true -> error({cannot_apply_template, data_shortage});
        false -> get_val_(Rest, lists:nth(N, Data))
      end
  catch
    error:_Reason ->
      error({invalid_template, invalid_placeholder})
  end;
get_val_([<<"$", _/binary>> | _Rest], _Data) ->
  error({cannot_apply_template, cannot_fetch_data});
get_val_([Key | Rest], Data)
  when is_binary(Key), is_map(Data) ->
  case maps:get(Key, Data, undefined) of
    undefined ->
      error({cannot_apply_template, cannot_fetch_data});
    Val -> get_val_(Rest, Val)
  end;
get_val_([_ | _Rest], _Data) ->
  error({cannot_apply_template, cannot_fetch_data}).

nested_get(Key, Map) when not is_list(Key) ->
  maps:get(Key, Map, undefined);
nested_get([Key], Map) -> maps:get(Key, Map, undefined);
nested_get([Key | More], Map) ->
  case maps:find(Key, Map) of
    {ok, Val} -> nested_get(More, Val);
    error -> undefined
  end;
nested_get([], Val) -> Val.

nested_put(_, undefined, Map) -> Map;
nested_put(Key, Val, Map) when not is_list(Key) ->
  maps:put(Key, Val, Map);
nested_put([Key], Val, Map) -> maps:put(Key, Val, Map);
nested_put([Key | More], Val, Map) ->
  SubMap = maps:get(Key, Map, #{}),
  maps:put(Key, nested_put(More, Val, SubMap), Map);
nested_put([], Val, _Map) -> Val.

%% 连接 opentsdb 时序数据库
connect(Options) -> opentsdb:start_link(Options).

host_to_str(null) -> null;
host_to_str(IPAddr) ->
  list_to_binary(inet:ntoa(IPAddr)).

posix_errno(enoent) -> "The file does not exist";
posix_errno(eacces) ->
  "Missing permission for reading the file, "
  "or for searching one of the parent directorie"
  "s";
posix_errno(eisdir) -> "The named file is a directory";
posix_errno(enotdir) ->
  "A component of the filename is not a "
  "directory";
posix_errno(enomem) ->
  "There is not enough memory for the contents "
  "of the file";
posix_errno(_) -> "Unknown error".

logger_header() -> "".