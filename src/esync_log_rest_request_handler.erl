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
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-include("esync_log.hrl").

-record(state, {
    rest_request_id                             ::term(),
    op_log_receiver                             ::pid() | term(),
    rest_url                                    ::string(),
    rest_buf            = <<>>                  ::binary()
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
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

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
init([]) ->
   {ok, #state{}}.

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
handle_call({rest_sync, {ServerId, Index, Host, Port}}, _From, State = #state{rest_request_id = undefined}) ->
    RestUrl = esync_log:make_rest_request_url(Host, Port, ServerId, Index),
    RestRequestResult = httpc:request(get, {RestUrl, []}, [], [{sync, false}, {stream, self}, {body_format, binary}]),
    RequestId =
        case RestRequestResult of
            {ok, Id} -> Id;
            _ -> undefined
        end,
    lager:debug("rest sync to url [~p] requst result [~p]", [RestUrl, RestRequestResult]),
    {reply, {RestUrl, RestRequestResult}, State#state{rest_url = RestUrl, rest_request_id = RequestId}};
handle_call({rest_sync, Args}, _From, State = #state{rest_request_id = RestRequestId}) ->
    lager:debug("rest sync http now still unfinished requstId [~p]", [RestRequestId]),
    {reply, {error, Args, RestRequestId}, State};

handle_call(cancel_rest_sync, _From, State = #state{rest_request_id = undefined}) ->
    lager:debug("rest sync http still not started", []),
    {reply, {error, request_not_started}, State};
handle_call(cancel_rest_sync, _From, State = #state{rest_request_id = RestRequestId}) ->
    Result = httpc:cancel_request(RestRequestId),
    lager:debug("rest sync http request [~p] cancelled", [RestRequestId]),
    {reply, Result, State#state{rest_request_id = undefined}};

handle_call(_Request, _From, State) ->
    lager:debug("unknown request [~p] from [~p]", [_Request, _From]),
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
handle_info({http, {RequestId, stream_start, Headers}}, State = #state{op_log_receiver = Receiver, rest_request_id = RequestId}) ->
    lager:debug("stream started [~p]", [Headers]),
    try
        Receiver ! sync_log_start
    catch E:T ->
        lager:error("send op_log_start to receiver [~p] failed [~p:~p]", [Receiver, E ,T])
    end,
    {noreply, State#state{rest_buf = <<>>}};
handle_info({http, {RequestId, stream, BinBodyPart}}, State = #state{op_log_receiver = Receiver, rest_request_id = RequestId, rest_buf = Buf}) ->
    lager:debug("stream body [~p]", [BinBodyPart]),
    AllBody = <<Buf/binary, BinBodyPart/binary>>,
    Rest = handle_rest_response(Receiver, AllBody),
    {noreply, State#state{rest_buf = Rest}};
handle_info({http, {RequestId, stream_end, _Headers}}, State = #state{op_log_receiver = Receiver, rest_request_id = RequestId}) ->
    lager:debug("stream end", []),
    try
        Receiver ! sync_log_end
    catch E:T ->
        lager:error("send op_log_end to receiver [~p] failed [~p:~p]", [Receiver, E ,T])
    end,
    {noreply, State};
handle_info({http, {RequestId, Event, _Headers}}, State = #state{op_log_receiver = Receiver, rest_request_id = RequestId}) ->
    lager:debug("stream unknown event [~p]", [Event]),
    try
        Receiver ! {sync_log_exception, Event}
    catch E:T ->
        lager:error("send op_log_end to receiver [~p] failed [~p:~p]", [Receiver, E ,T])
    end,
    {noreply, State};

handle_info({set_sync_receiver, Receiver}, State) ->
    lager:debug("set_sync_receiver [~p]", [Receiver]),
    {noreply, State#state{op_log_receiver = Receiver}};

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
handle_rest_response(Receiver, Response) ->
    case binary:split(Response, <<"\n">>) of
        [Line, Rest] ->
            try
                [ServerId, Rest1] = binary:split(Line, ?OP_LOG_SEP),
                [Index, Rest2] = binary:split(Rest1, ?OP_LOG_SEP),
                Receiver ! {sync_log, {ServerId, binary_to_integer(Index), binary_part(Rest2, 0, byte_size(Rest2)-1)}}
            catch E:T ->
                lager:error("send op_log [~p] to receiver [~p] failed [~p:~p]", [Response, Receiver, E ,T])
            end,
            handle_rest_response(Receiver, Rest);
        _ ->
            Response
    end.