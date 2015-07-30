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

-export([
    get_read_logger/0,
    read_logger_line/1,
    position_logger_to_index/2,
    close_logger/1,
    get_latest_index/0,
    split_index_from_op_log_line/1
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
    server_id       =   <<"default_server">>    ::binary(),
    client                                      ::pid()
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
handle_call(get_latest_index, _From, State) ->
    Result = get_last_op_log_index(),
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
    end,
    File.

position_line_by_line(File, Index) ->
    case read_logger_line(File) of
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

%% API call with block
-spec(open_and_ensure_read_logger() -> none | term()).
open_and_ensure_read_logger() ->
    FileName = ?DEFAULT_OP_LOG_FILE_NAME,
    case file:write_file(FileName, <<>>, [exclusive, raw, binary]) of
        ok ->
            lager:info("create file [~p] succ", [FileName]),
            ok;
        Error ->
            lager:error("create file [~p] failed [~P]", [FileName, Error])
    end,
    open_write_logger().

-spec(open_write_logger() -> {ok, term()} | {error, term()}).
open_write_logger() ->
    FileName = ?DEFAULT_OP_LOG_FILE_NAME,
    case file:open(FileName, [raw, write, append, binary]) of
        {ok, File} -> {ok, File};
        {error, Error} ->
            lager:error("open op log file [~p] failed [~P]", [FileName, Error]),
            {error, Error}
    end.

-spec(open_read_logger() -> {ok, term()} | {error, term()}).
open_read_logger() ->
    FileName = ?DEFAULT_OP_LOG_FILE_NAME,
    case file:open(FileName, [read, binary]) of
        {ok, File} -> {ok, File};
        {error, enoent} ->
            lager:info("no op log idx file found", []),
            {error, enoent};
        {error, Error} ->
            lager:error("open op log file [~p] failed [~P]", [FileName, Error]),
            {error, Error}
    end.

-spec(read_logger_line(File) -> {ok, Data} | eof | {error, term()}).
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
