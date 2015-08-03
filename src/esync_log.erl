%%%-------------------------------------------------------------------
%%% @author thi
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. 七月 2015 下午4:18
%%%-------------------------------------------------------------------
-module(esync_log).
-author("thi").

-behaviour(application).

%% API
-export([get_config/2, make_rest_request_url/3]).

%% Application callbacks
-export([start/2,
    stop/1]).

%%%===================================================================
%%% Application callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called whenever an application is started using
%% application:start/[1,2], and should start the processes of the
%% application. If the application is structured according to the OTP
%% design principles as a supervision tree, this means starting the
%% top supervisor of the tree.
%%
%% @end
%%--------------------------------------------------------------------
-spec(start(StartType :: normal | {takeover, node()} | {failover, node()},
    StartArgs :: term()) ->
    {ok, pid()} |
    {ok, pid(), State :: term()} |
    {error, Reason :: term()}).
start(_StartType, _StartArgs) ->
    application:start(lager),
    case esync_log_sup:start_link() of
        {ok, Pid} ->
            {ok, Pid};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called whenever an application has stopped. It
%% is intended to be the opposite of Module:start/2 and should do
%% any necessary cleaning up. The return value is ignored.
%%
%% @end
%%--------------------------------------------------------------------
-spec(stop(State :: term()) -> term()).
stop(_State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_config(Field, Default) ->
    case application:get_env(esync_log, Field) of
        {ok, Value} ->
            lager:debug("~p := ~p~n", [Field, Value]),
            Value;
        _ ->
            lager:debug("~p := ~p~n", [Field, Default]),
            Default
    end.

make_rest_request_url(Host, Port, Index) ->
    RequestUrlPath = esync_log:get_config(rest_request_url_path, "/rest/oplog/"),
    %% eg. http://localhost:8765/rest/oplog/?index=...
    lists:concat(["http://", Host, ":", integer_to_list(Port), RequestUrlPath, "?index=", integer_to_list(Index)]).

