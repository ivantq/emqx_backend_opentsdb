{application,emqx_backend_opentsdb,
             [{description,"EMQ X OpenTSDB Backend"},
              {vsn,"4.2.1"},

              {modules,[
                emqx_backend_opentsdb,
                emqx_backend_opentsdb_app,
                emqx_backend_opentsdb_cli,
                emqx_backend_opentsdb_sup]},

              {registered,[emqx_backend_opentsdb_sup]},
              {applications,[kernel,stdlib,opentsdb,ecpool]},
              {mod,{emqx_backend_opentsdb_app,[]}}]}.

%% opentsdb 模块
%% ecpool  进程池模块


%% {ok,{_,[{abstract_code,{_,AC}}]}} = beam_lib:chunks(Beam,[abstract_code]).
%% io:fwrite("~s~n", [erl_prettypr:format(erl_syntax:form_list(AC))]).

%%{ok,{_,[{abstract_code,{_,AC}}]}} = beam_lib:chunks(emqx_bridge_rocket,[abstract_code]).
%%{ok,{_,[{abstract_code,{_,AC}}]}} = beam_lib:chunks(emqx_backend_opentsdb,[abstract_code]).