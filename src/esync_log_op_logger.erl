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

-export([log_command/1, log_sync_command/3]).

-export([start_sync/2, cancel_sync/0]).

-export([
    open_read_logger/0,
    read_logger_line/1,
    position_logger_to_index/2,
    close_logger/1,
    get_line_index/1,
    is_full_line/1,
    get_line_server_id/1
]).

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
    op_log_file                                 ::term(),
    op_index                                    ::integer(),
    server_id                                   ::binary(),
    client                                      ::pid()
}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc handle an op log.
-spec log_command(binary()) -> ok.
log_command(Command) ->
    gen_server:cast(?MODULE, {oplog, Command}).

%% @doc handle an sync op log with index.
-spec log_sync_command(binary(), integer(), binary()) -> ok.
log_sync_command(ServerId, Index, Command) ->
    gen_server:cast(?MODULE, {synclog, ServerId, Index, Command}).

%% @doc handle an sync op log with index.
-spec start_sync(string(), integer()) -> ok.
start_sync(Host, Port) ->
    gen_server:call(?MODULE, {start_sync, Host, Port}).

cancel_sync() ->
    gen_server:call(?MODULE, cancel_sync).

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
    ServerId = get_server_id(),

    %% open log file to write op in
    OpLogFile = open_write_logger(),

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
handle_call(get_read_logger, _From, State) ->
    Result = open_and_ensure_read_logger(),
    lager:debug("get_or_create_read_logger [~p]", [Result]),
    {reply, Result, State};
handle_call(get_latest_index, _From, State = #state{op_index = LastOpIndex}) ->
    Result = LastOpIndex,
    lager:debug("get_latest_index [~p]", [Result]),
    {reply, Result, State};
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

handle_cast({oplog, BinOpLog}, State = #state{op_log_file = OpLogFile, op_index = LastOpIndex, server_id = ServerId}) ->
    OpIndex = LastOpIndex + 1,
    lager:debug("write OpIndex [~p]", [OpIndex]),
    Line = iolist_to_binary([
        ServerId, ?OP_LOG_SEP
        ,integer_to_binary(OpIndex), ?OP_LOG_SEP
        ,BinOpLog, "\n"
    ]),
    write_bin_log_to_op_log_file(OpLogFile, Line),
    {noreply, State#state{op_index = OpIndex}};
handle_cast({synclog, ServerId, Index, BinOpLog}, State = #state{op_log_file = OpLogFile, op_index = LastOpIndex, server_id = ServerId}) ->
    OpIndex = Index,
    case Index == LastOpIndex+1 of
        true ->
            ok;
        _ ->
            lager:error("conflict at op Index [~p] with sync Index [~p], reset index, Log [~p]", [OpIndex, Index, BinOpLog]),
            ok
    end,
    lager:debug("write sync OpIndex [~p]", [Index]),
    Line = iolist_to_binary([
        integer_to_binary(OpIndex), ?OP_LOG_SEP
        ,ServerId, ?OP_LOG_SEP
        ,BinOpLog, "\n"
    ]),
    write_bin_log_to_op_log_file(OpLogFile, Line),
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

-spec(position_logger_to_index(term(), integer()) -> none | term()).
position_logger_to_index(File, Index) when is_integer(Index) ->
    case File of
        none -> ok;
        _ ->
            file:position(File, {bof, 0}),
            position_line_by_line(File, Index)
    end.

position_line_by_line(File, Index) ->
    case read_logger_line(File) of
        {ok, Line} ->
            case get_line_index(Line) of
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

%% API call with block
-spec(open_and_ensure_read_logger() -> none | term()).
open_and_ensure_read_logger() ->
    FileName = ?DEFAULT_OP_LOG_FILE_NAME,
    case file:write_file(FileName, <<>>, [exclusive, raw, binary]) of
        ok ->
            lager:info("create log file [~p] succ", [FileName]),
            ok;
        Error ->
            lager:error("create log file [~p] failed [~P]", [FileName, Error])
    end,
    open_write_logger().

-spec(open_write_logger() -> {ok, term()} | {error, term()}).
open_write_logger() ->
    FileName = ?DEFAULT_OP_LOG_FILE_NAME,
    case file:open(FileName, [raw, write, append, binary, delayed_write]) of
        {ok, File} -> File;
        {error, Error} ->
            lager:error("open op log file [~p] failed [~P]", [FileName, Error]),
            none
    end.

-spec(open_read_logger() -> {ok, term()} | {error, term()}).
open_read_logger() ->
    FileName = ?DEFAULT_OP_LOG_FILE_NAME,
    case file:open(FileName, [read, binary]) of
        {ok, File} -> File;
        {error, enoent} ->
            lager:info("no op log file found", []),
            none;
        {error, Error} ->
            lager:error("open op log file [~p] failed [~P]", [FileName, Error]),
            none
    end.

-spec(read_logger_line(pid()) -> {ok, binary()} | eof | {error, term()}).
read_logger_line(File) when is_pid(File)->
    file:read_line(File).

write_bin_log_to_op_log_file(File, BinLog) ->
    case File of
        none -> lager:info("no op log file to write op [~p]", [BinLog]);
        _ -> file:write(File, BinLog)
    end.

-spec(close_logger(term()) -> ok).
close_logger(File) ->
    file:close(File).

get_last_op_log_index() ->
    %% read to get current op log index first
    case open_read_logger() of
        none ->
            lager:error("read log idx file failed", []),
            ?DEFAULT_OP_LOG_START_INDEX;
        File ->
            LastLine = read_last_line(File),
            file:close(File),
            case LastLine of
                {ok, BinLastLine} ->
                    lager:debug("get last line from current log succ: [~p]", [LastLine]),
                    get_line_index(BinLastLine);
                Error ->
                    lager:error("try to read last log op line failed [~p]", [Error]),
                    ?DEFAULT_OP_LOG_START_INDEX
            end
    end.

get_line_index(BinLastLine) ->
    case binary:split(BinLastLine, ?OP_LOG_SEP) of
        [_ServerId, Rest1] ->
            case binary:split(Rest1, ?OP_LOG_SEP) of
                [BinIndex, _Rest2] ->
                    try
                        erlang:binary_to_integer(BinIndex)
                    catch E:T ->
                        lager:error("index binary_to_integer [~p] failed [~p:~p]", [BinIndex, E, T]),
                        ?DEFAULT_OP_LOG_START_INDEX
                    end;
                _ ->
                    lager:info("get an illegal op log line [~p], set to default index", [BinLastLine]),
                    ?DEFAULT_OP_LOG_START_INDEX
            end;
        _ ->
            lager:info("get an illegal op log line [~p], set to default index", [BinLastLine]),
            ?DEFAULT_OP_LOG_START_INDEX
    end.

-spec(read_last_line(term()) -> {ok, binary()} | {error, atom()}).  %% just as file:read_line
read_last_line(File) ->
    read_last_line(<<"">>, File).

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

-spec(is_full_line(binary()) -> boolean()).
is_full_line(Line) when is_binary(Line) ->
    case byte_size(Line) of
        0 -> false;
        _ ->
            case binary:last(Line) of
                10 -> true;
                _ -> false
            end
    end.

-spec(get_line_server_id(binary()) -> binary()).
get_line_server_id(Line) when is_binary(Line) ->
    case binary:split(Line, ?OP_LOG_SEP) of
        [ServerId, _Rest] -> ServerId;
        _ ->
            lager:error("get server id failed [~p]", [Line]),
            ?DEFAULT_SERVER_ID
    end.

get_server_id() ->
    FileName = ?DEFAULT_SERVER_ID_FILE_NAME,
    case file:read_file(FileName) of
        {ok, ServerId} ->
            lager:debug("got server id from file [~p]", [ServerId]),
            ServerId;
        {error, Error} ->
            lager:info("open server id file [~p] failed [~p], recreate it!", [FileName, Error]),
            ServerId = gen_server_id(),
            case file:write_file(FileName, ServerId, [exclusive, raw, binary]) of
                ok ->
                    lager:info("create server id file [~p] succ", [FileName]),
                    ok;
                Error ->
                    lager:error("create server id file [~p] failed [~P]", [FileName, Error])
            end,
            ServerId
    end.

gen_server_id() ->
    %% generated server id is based on nodename & ip & timestamp
    HashInt = erlang:phash2([node(), inet:getif(), os:timestamp()]),
    integer_to_binary(HashInt).