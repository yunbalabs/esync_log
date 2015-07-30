%%%-------------------------------------------------------------------
%%% @author thi
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. 七月 2015 下午8:12
%%%-------------------------------------------------------------------
-module(esync_log_rest_request_handler).
-author("thi").

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-include_lib("esync_log.erl").

-record(state, {
    rest_request_id                             ::term(),
    op_log_receiver                             ::pid() | term()
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(list()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link([Receiver]) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], [Receiver]).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([Receiver]) ->
    {ok, #state{
        op_log_receiver = Receiver,
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call({rest_sync, Host, Port}, _From, State = #state{rest_request_id = undefined}) ->
    Index =
        try
            gen_server:call(esync_log_op_logger, get_latest_index)
        catch
            E:T ->
                lager:error("gen server call get_latest_index failed [~p:~p]", [E, T]),
                ?DEFAULT_OP_LOG_START_INDEX
        end,
    handle_call({rest_sync, Host, Port, Index}, _From, State);
handle_call({rest_sync, Host, Port, Index}, _From, State = #state{rest_request_id = undefined}) ->
    Url = esync_log:make_up_rest_rul(Host, Port, Index),
    RestRequestId = httpc:request(get, {Url, []}, [], [{sync, false}, {stream, self}, {body_format, binary}]),
    lager:debug("rest sync from url [~p] requstId [~p]", [Url, RestRequestId]),
    {reply, {ok, Url, RestRequestId}, State#state{rest_request_id = RestRequestId}};
handle_call({rest_sync, Url}, _From, State = #state{rest_request_id = RestRequestId}) ->
    lager:debug("rest sync http now unfinished requstId [~p]", [RestRequestId]),
    {reply, {error, Url, RestRequestId}, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info({http, {_RequestId, stream_start, Headers}}, State = #state{op_log_receiver = Receiver, rest_request_id = RequestId}) ->
    lager:debug("stream started [~p]", [Headers]),
    try
        Receiver ! op_log_start
    catch E:T ->
        lager:error("send op_log_start to receiver [~p] failed [~p:~p]", [Receiver, E ,T])
    end,
    {noreply, State};
handle_info({http, {RequestId, stream, BinBodyPart}}, State = #state{op_log_receiver = Receiver, rest_request_id = RequestId}) ->
    lager:debug("stream body [~p]", [BinBodyPart]),
    try
        Receiver ! {op_log, BinBodyPart}
    catch E:T ->
        lager:error("send op_log [~p] to receiver [~p] failed [~p:~p]", [BinBodyPart, Receiver, E ,T])
    end,
    {noreply, State};
handle_info({http, {RequestId, stream_end, _Headers}}, State = #state{op_log_receiver = Receiver, rest_request_id = RequestId}) ->
    lager:debug("stream end", []),
    try
        Receiver ! op_log_end
    catch E:T ->
        lager:error("send op_log_end to receiver [~p] failed [~p:~p]", [Receiver, E ,T])
    end,
    {noreply, State};
handle_info({http, {RequestId, Event, _Headers}}, State = #state{op_log_receiver = Receiver, rest_request_id = RequestId}) ->
    lager:debug("stream unknown event [~p]", [Event]),
    {noreply, State};
handle_info(_Info, State) ->
    lager:debug("unknown info [~p]", [_Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
