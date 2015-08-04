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
-define(DEFAULT_REQ_INDEX, 0).

-behaviour(cowboy_loop_handler).

-export([init/3]).
-export([info/3]).
-export([terminate/3]).

-record(state, {
    logger              ::  term(),
    buf     = <<>>      ::  binary()
}).

init(_, Req, _) ->
    self() ! response,
    {loop, Req, #state{}, hibernate}.

info(response, Req, State) ->
    Index = get_req_index(Req),
    Logger = esync_log_op_logger:open_read_logger(),
    case esync_log_op_logger:position_logger_to_index(Logger, Index) of
        ok ->
            Req2 = cowboy_req:set([{resp_state, waiting_stream}], Req),
            {ok, Req3} = cowboy_req:chunked_reply(200, Req2),
            trigger_next_line(),
            {loop, Req3, State#state{logger = Logger}};
        error ->
            {ok, Req2} = cowboy_req:reply(413, [], <<"Index too big">>, Req),
            {ok, Req2, State#state{logger = none}}
    end;

info(send_line, Req, State = #state{logger = Logger, buf = Buf}) ->
    case esync_log_op_logger:get_line(Logger) of
        {ok, Line} ->
            AllBuf = <<Buf, Line>>,
            case esync_log_op_logger:is_full_line(AllBuf) of
                true ->
                    send_line(Req, AllBuf),
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

get_req_index(Req) ->
    {_Method, Req2} = cowboy_req:method(Req),
    {BinIndex, _Req3} = cowboy_req:qs_val(<<"index">>, Req2),
    lager:debug("Index [~p]", [BinIndex]),
    case BinIndex of
        <<"undefined">> ->
            lager:info("index argument not found, set to default index [~p]", [?DEFAULT_REQ_INDEX]),
            ?DEFAULT_REQ_INDEX;
        _ ->
            try
                binary_to_integer(BinIndex)
            catch E:T ->
                lager:error("parge binary index [~p] failed [~p:~p], set default index [~p]", [BinIndex, E, T, ?DEFAULT_REQ_INDEX]),
                ?DEFAULT_REQ_INDEX
            end
    end.
