%%%-------------------------------------------------------------------
%%% @author thi
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. 七月 2015 下午3:18
%%%-------------------------------------------------------------------
-author("thi").

-define(DEFAULT_LOG_IDX_FILE, "oplog/op_log.idx").
-define(DEFAULT_START_OP_LOG_FILE_INDEX, 0).
-define(DEFAULT_SERVER_ID, "server1").
-define(DEFAULT_OP_COUNT_PER_LOG_FILE, 1000000).

-define(DEFAULT_OP_LOG_FILE_NAME, "oplog/op_log.log").
-define(OP_LOG_SEP, <<"\\">>).
-define(DEFAULT_OP_LOG_START_INDEX, 0).