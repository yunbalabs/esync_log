%%%-------------------------------------------------------------------
%%% @author thi
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. 七月 2015 上午10:59
%%%-------------------------------------------------------------------
-module(esync_log_op_logger).
-author("thi").

-behaviour(gen_server).

%% API
-export([start_link/0]).

-export([log_command/1, rest_sync/1]).

-export([open_op_log_file_for_read/0,
    split_index_from_op_log_line/1
]).

%-export([format_command_to_op_log/2,
%    make_command_from_op_log/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-define(DEFAULT_LOG_IDX_FILE, "oplog/op_log.idx").
-define(DEFAULT_START_OP_LOG_FILE_INDEX, 0).
-define(DEFAULT_SERVER_ID, "server1").
-define(DEFAULT_OP_COUNT_PER_LOG_FILE, 1000000).

-define(DEFAULT_OP_LOG_FILE_NAME, "oplog/op_log.log").
-define(OP_LOG_SEP, <<"\\">>).
-define(DEFAULT_OP_LOG_START_INDEX, 0).

-record(state, {
    op_log_file                                 ::term(),
    op_index                                    ::integer(),
    server_id       =   <<"default_server">>    ::binary(),
    rest_request_id                             ::term(),
    client
}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Notifies an op log.
-spec log_command(binary()) -> ok.
log_command(Command) ->
    gen_server:cast(?MODULE, {oplog, Command}).

%% @doc Notifies a rest sync.
-spec rest_sync(binary()) -> ok.
rest_sync(Url) ->
    gen_event:call(?MODULE, {rest_sync, Url}).

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
    %% get start op log index from file
    StartOpIndex = get_last_op_log_index(),

    %% get server id
    ServerId = ?DEFAULT_SERVER_ID,

    %% open log file to write op in
    OpLogFile = open_op_log_file_for_write(),

    lager:debug("open log file [~p] start index [~p]", [?DEFAULT_OP_LOG_FILE_NAME, StartOpIndex]),

    Client = edis_db:process(0),

    {ok, #state{op_index = StartOpIndex, op_log_file = OpLogFile, server_id = ServerId, client = Client}}.

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
handle_call({rest_sync, Url}, _From, State = #state{}) ->
    RestRequestId = httpc:request(get, {Url, []}, [], [{sync, false}, {stream, self}, {body_format, binary}]),
    lager:debug("rest sync from url [~p] requstId [~p]", [Url, RestRequestId]),
    {reply, {ok, Url, RestRequestId}, State#state{rest_request_id = RestRequestId}};
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

handle_cast({oplog, BinOpLog}, State = #state{op_log_file = OpLogFile, op_index = LastOpIndex}) ->
    OpIndex = LastOpIndex + 1,
    %BinOpLog = format_command_to_op_log(OpIndex, Command),
    lager:debug("write OpIndex [~p]", [OpIndex]),
    write_bin_log_to_op_log_file(OpLogFile, BinOpLog),
    {noreply, State#state{op_index = OpIndex}};

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
-spec(handle_info(Info :: term(), State :: #state{}) ->
    {ok, NewState :: #state{}} |
    {ok, NewState :: #state{}, hibernate} |
    {swap_handler, Args1 :: term(), NewState :: #state{},
        Handler2 :: (atom() | {atom(), Id :: term()}), Args2 :: term()} |
    remove_handler).
handle_info({http, {_RequestId, stream_start, Headers}}, State) ->
    lager:debug("stream started [~p]", [Headers]),
    {noreply, State};
handle_info({http, {_RequestId, stream, BinBodyPart}}, State = #state{client = Client}) ->
    lager:debug("stream body [~p]", [BinBodyPart]),
    try
        {_Index, Command} = edis_op_logger:make_command_from_op_log(BinBodyPart),
        edis_db:run(Client, Command)
    catch E:T ->
        lager:error("make command or run command [~p] failed [~p:~p]", [BinBodyPart, E ,T])
    end,
    {noreply, State};
handle_info({http, {_RequestId, stream_end, _Headers}}, State) ->
    lager:debug("stream end", []),
    {noreply, State};
handle_info(_Info, State) ->
    lager:debug("info [~p]", [_Info]),
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

position_file_to_index(File, Index) when is_integer(Index) ->
    case File of
        none -> ok;
        _ ->
            file:position(File, {bof, 0}),
            position_line_by_line(File, Index)
    end,
    File.

position_line_by_line(File, Index) ->
    case file:read_line(File) of
        {ok, Line} ->
            case edis_op_logger:split_index_from_op_log_line(Line) of
                Index ->
                    lager:info("found index [~p] in Line [~p]", [Index, Line]),
                    ok;
                N when N < Index ->
                    position_line_by_line(File, Index);
                N ->
                    lager:error("splited index [~p] > specified index [~p], set back to previos line [~p]", [N, Index, Line]),
                    {ok, _} = file:position(File, {cur, -byte_size(Line)}),
                    ok
            end
    end.

%format_command_to_op_log(OpIndex, _Command = #edis_command{timestamp = TimeStamp, db = Db, cmd = Cmd, args = Args, group = Group, result_type = ResultType}) ->
%    iolist_to_binary([make_sure_binay(OpIndex)
%        , ?OP_LOG_SEP, make_sure_binay(trunc(TimeStamp))
%        , ?OP_LOG_SEP, make_sure_binay(Db)
%        , ?OP_LOG_SEP, make_sure_binay(Cmd)
%        , ?OP_LOG_SEP, make_sure_binay(Group)
%        , ?OP_LOG_SEP, make_sure_binay(ResultType)
%    ] ++ lists:map(fun(E) -> iolist_to_binary([?OP_LOG_SEP, make_sure_binay(E)]) end, Args)
%        ++ "\n"
%    ).
%
%make_command_from_op_log(BinOpLog) ->
%    [BinOpIndex, Bin1] = binary:split(BinOpLog, ?OP_LOG_SEP),
%    [BinTimeStamp, Bin2] = binary:split(Bin1, ?OP_LOG_SEP),
%    [BinDb, Bin3] = binary:split(Bin2, ?OP_LOG_SEP),
%    [BinCmd, Bin4] = binary:split(Bin3, ?OP_LOG_SEP),
%    [BinGroup, Bin5] = binary:split(Bin4, ?OP_LOG_SEP),
%    [BinResultType, Bin6] = binary:split(Bin5, ?OP_LOG_SEP),
%    Args = binary:split(Bin6, ?OP_LOG_SEP, [global]),
%
%    {binary_to_integer(BinOpIndex),
%        #edis_command{
%            timestamp = binary_to_integer(BinTimeStamp) + 0.0,
%            db = binary_to_integer(BinDb),
%            cmd = BinCmd,
%            group = binary_to_atom(BinGroup, latin1),
%            result_type = binary_to_atom(BinResultType, latin1),
%            args = Args
%        }}.
%
%
%
%make_sure_binay(Data) ->
%    if
%        is_integer(Data) ->
%            integer_to_binary(Data);
%        is_list(Data) ->
%            list_to_binary(Data);
%        is_atom(Data) ->
%            atom_to_binary(Data, latin1);
%        is_float(Data) ->
%            float_to_binary(Data);
%        true ->
%            Data
%    end.

open_op_log_file_for_write() ->
    FileName = ?DEFAULT_OP_LOG_FILE_NAME,
    case file:open(FileName, [raw, write, append, binary]) of
        {ok, File} -> File;
        Error ->
            lager:error("open op log file [~p] failed [~P]", [FileName, Error]),
            none
    end.

-spec(open_op_log_file_for_read() -> none | term()).
open_op_log_file_for_read() ->
    FileName = ?DEFAULT_OP_LOG_FILE_NAME,
    case file:open(FileName, [read, binary]) of
        {ok, File} -> File;
        {error, enoent} ->
            lager:info("no op log idx file found, deault to index 0", []),
            none;
        Error ->
            lager:error("open op log file [~p] failed [~P]", [FileName, Error]),
            none
    end.

write_bin_log_to_op_log_file(File, BinLog) ->
    case File of
        none -> lager:info("no op log file to write op [~p]", [BinLog]);
        _ -> file:write(File, BinLog)
    end.

close_op_log_file(File) ->
    file:close(File).

get_last_op_log_index() ->
%    OpLogFileIndex =
%        case file:read_file(?DEFAULT_LOG_IDX_FILE) of
%            {ok, File} ->
%                try
%                    erlang:binary_to_integer(File)
%                catch E:T ->
%                    lager:error("binary to integer [~p] failed of [~p:~p]", [File, E, T]),
%                    ?DEFAULT_START_OP_LOG_FILE_INDEX
%                end;
%            {error, enoent} ->
%                lager:info("no op log idx file found, deault to index 0", []),
%                ?DEFAULT_START_OP_LOG_FILE_INDEX;
%            Error ->
%                lager:info("read log idx file failed with error [~p]", [Error]),
%                ?DEFAULT_START_OP_LOG_FILE_INDEX
%        end,

    %% read to get current op log index first
    case open_op_log_file_for_read() of
        none ->
            lager:error("read log idx file failed", []),
            ?DEFAULT_OP_LOG_START_INDEX;
        File ->
            LastLine = read_last_line(File),
            file:close(File),
            case LastLine of
                {ok, BinLastLine} ->
                    lager:debug("get last line from current log succ: [~p]", [LastLine]),
                    split_index_from_op_log_line(BinLastLine);
                Error ->
                    lager:error("try to read last log op line failed [~p]", [Error]),
                    ?DEFAULT_OP_LOG_START_INDEX
            end
    end.

split_index_from_op_log_line(BinLastLine) ->
    case binary:split(BinLastLine, ?OP_LOG_SEP) of
        [BinIndex, _Rest] ->
            try
                erlang:binary_to_integer(BinIndex)
            catch E:T ->
                lager:error("index binary_to_integer [~p] failed [~p:~p]", [BinIndex, E, T]),
                ?DEFAULT_OP_LOG_START_INDEX
            end;
        _ ->
            lager:info("get an illegal op log line [~p], set to default index", [BinLastLine]),
            ?DEFAULT_OP_LOG_START_INDEX
    end.

-spec(read_last_line(term()) -> {ok, binary()} | {error, atom()}).  %% just as file:read_line
read_last_line(File) ->
    read_last_line(<<"">>, File).

%read_last_line(File, Number) ->
%    case file:pread(File, {eof, Number}, Number) of
%        {ok, Data} ->
%            Lines = binary:split(Data, <<"\n">>, [global]),
%            case length(Lines) of
%                N when N>=3 ->
%                    {ok, lists:nth(N-1, Lines)};
%                _ ->
%                    read_last_line(File, Number*2)
%            end;
%        {error, eof} -> file:read_line(File);
%        eof -> file:read_line(File);
%        Error ->
%            lager:error("read_last_line/2 failed with error [~p]", [Error]),
%            {ok, <<"">>}
%    end.


read_last_line(Line, File) ->
    case file:read_line(File) of
        {ok, Data} ->
            read_last_line(Data, File);
        {error, eof} -> {ok, Line};
        eof -> {ok, Line};
        Error ->
            lager:error("read_last_line/2 failed with error [~p]", [Error]),
            {ok, Line}
    end.
