%%%-------------------------------------------------------------------
%%% @author thi
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. 七月 2015 下午4:52
%%%-------------------------------------------------------------------
-module(esync_log_rest_handler).
-author("thi").

-include("esync_log.hrl").

-behaviour(cowboy_loop_handler).

-export([init/3]).
-export([info/3]).
-export([terminate/3]).

-record(state, {
    logger                  ::  term(),
    buf         = <<>>      ::  binary(),
    server_id   = undefined :: binary()
}).

init(_, Req, _) ->
    self() ! response,
    {loop, Req, #state{}, hibernate}.

info(response, Req, State) ->
    {Index, ServerId} = get_req_index_serverId(Req),
    Logger = esync_log_op_logger:open_read_logger(),
    case esync_log_op_logger:position_logger_to_index(Logger, Index) of
        ok ->
            Req2 = cowboy_req:set([{resp_state, waiting_stream}], Req),
            {ok, Req3} = cowboy_req:chunked_reply(200, Req2),
            trigger_next_line(),
            {loop, Req3, State#state{logger = Logger, server_id = ServerId}};
        error ->
            {ok, Req2} = cowboy_req:reply(413, [], <<"Index too big">>, Req),
            {ok, Req2, State#state{logger = none, server_id = ServerId}}
    end;

info(send_line, Req, State = #state{logger = Logger, buf = Buf, server_id = ServerId}) ->
    case esync_log_op_logger:read_logger_line(Logger) of
        {ok, Line} ->
            AllBuf = <<Buf/binary, Line/binary>>,
            case esync_log_op_logger:is_full_line(AllBuf) of
                true ->
                    case esync_log_op_logger:get_line_server_id(AllBuf) == ServerId of
                        true ->
                            lager:debug("request server id equal with log server id [~p], ignore this line [~p]", [ServerId, AllBuf]);
                        false ->
                            send_line(Req, AllBuf)
                    end,
                    trigger_next_line(),
                    {loop, Req, State#state{buf = <<>>}};
                _ ->
                    trigger_next_line(),
                    {loop, Req, State#state{buf = AllBuf}}
            end;
        eof ->
            trigger_next_line(),
            {loop, Req, State};
        {error, eof} ->
            trigger_next_line(),
            {loop, Req, State};
        _ ->
            esync_log_op_logger:close_logger(Logger),
            {ok, Req, State#state{logger = none}}
    end.

terminate(Reason, _, _State = #state{logger = Logger}) ->
    lager:debug("Req terminate by [~p]", [Reason]),
    esync_log_op_logger:close_logger(Logger),
    ok.

trigger_next_line() ->
    %%timer:sleep(500),
    self() ! send_line.

send_line(Req, Line) ->
    cowboy_req:chunk(Line, Req),
    ok.

get_req_index_serverId(Req) ->
    {_Method, Req2} = cowboy_req:method(Req),
    {ServerId, Req3} = cowboy_req:qs_val(<<"serverId">>, Req2, ?DEFAULT_SERVER_ID),
    {BinIndex, _Req4} = cowboy_req:qs_val(<<"index">>, Req3, integer_to_binary(?DEFAULT_REQ_INDEX)),
    lager:debug("Index [~p], ServerId [~p]", [BinIndex, ServerId]),
    {binary_to_integer(BinIndex), ServerId}.
