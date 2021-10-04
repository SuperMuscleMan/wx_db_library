-module(wx_db_client).
%%%=======================STATEMENT====================
-description("wx_db_client").
-copyright('').
-author("wmh, SuperMuscleMan@outlook.com").
%%%=======================EXPORT=======================
-export([read/2, read/5]).
-export([delete/5]).
-export([transaction/6]).
-export([update/8]).

-export([check_result/3]).
%%%=======================INCLUDE======================
-include_lib("wx_log_library/include/wx_log.hrl").
%%%=======================RECORD=======================

%%%=======================DEFINE=======================
-define(Multi_True, 1).%% 多个查询
-define(Multi_False, 0).%% 单个查询
-define(Lock_Try_Sleep_Time, 18).%% 尝试获取单个keys
-define(Lock_Try_Sleep_Time_Max, 28).%% 尝试获取单个keys
-define(Lock_Sleep_Time, 18).%% 尝试获取所有tab_keys
-define(Lock_Sleep_Time_Max, 40).%% 尝试获取所有tab_keys
-define(Read_Simple_TimeOut, 3000).

-define(WaitData, '$0$').        %% 等待数据标识

-define(TransactionOnlyOne, '$TOO$').        %% 进程同一时间只能处理一个事务
%%%=================EXPORTED FUNCTIONS=================
%% -----------------------------------------------------------------
%% Description:单表更新（单表事务
%% Inputs:
%% Returns:
%% -----------------------------------------------------------------
update(Tab, Key, Default, Fun, Lock0, LockTime, TimeOut, Args) ->
	case check_transaction_only_one() of
		ok ->
			Pid = wx_db_server:tab_to_pid(Tab),
			Lock = make_lock(Lock0),
			try
				D1 =
					case read(Pid, Tab, Key, Lock, LockTime, TimeOut) of
						{ok, none} ->
							Default;
						{ok, D} ->
							D
					end,
				case Fun(Args, D1) of
					{ok, R} ->
						unlock(Pid, Key, Lock),
						R;
					{ok, R, delete} ->
						ok = delete_(Pid, Tab, Key, Lock),
						R;
					{ok, R, D1} ->
						unlock(Pid, Key, Lock),
						R;
					{ok, R, Update} ->
						ok = write(Pid, Tab, Key, Update, Lock),
						R
				end
			catch
				_E1:E2:E3 ->
					unlock(Pid, Key, Lock),
					?ErrDb([update_err, {reason, E2}, {tab, Tab}, {key, Key}, {lock, Lock},
						{lock_time, LockTime}, {lock_timeout, TimeOut}, E3]),
					{err, E2}
			after
				erase_transaction_only_one()
			end;
		Reason ->
			Reason
	end.
%% -----------------------------------------------------------------
%% Description:事务处理（多表处理
%% Inputs:
%% Returns:
%% -----------------------------------------------------------------
transaction(TabKeys, Fun, Lock0, LockTime, TimeOut, Args) ->
	case check_transaction_only_one() of
		ok ->
			Lock = make_lock(Lock0),
			{TabKeys1, Defaults} = parse_tab_key(TabKeys, 1, [], []),
			try
				Ref = erlang:make_ref(),
				TabKeys2 = reads(TabKeys1, Ref, Lock, LockTime, TimeOut),
				?DEBUG([{tab_key_1, TabKeys1}, {default, Defaults},
					{tab_key_2, TabKeys2}, {add_default, add_default(Defaults, size(TabKeys2), TabKeys2, [])}]),
				case Fun(Args, add_default(Defaults, size(TabKeys2), TabKeys2, [])) of
					{ok, R} ->
						unlocks(TabKeys2, Lock),
						R;
					{ok, R, Update} ->
						TabKeys3 = handle_result(Update, TabKeys2),
						writes(TabKeys3, Ref, Lock),
						R;
					{ok, R, Update, Insert} ->
						TabKeys3 = handle_result(Update, TabKeys2),
						NewTabKeys = handle_insert_result(Insert, 1, []),
						writes(TabKeys3, Ref, Lock),
						writes(NewTabKeys, Ref, Lock),
						R
%%			Err ->
%%				unlocks(TabKeys2, Lock),
%%				Err
				end
			catch
				_E1:E2:E3 ->
					unlocks(TabKeys1, Lock),
					?ErrDb([transaction_err, {reason, E2}, {stack_trace, E3},{tab_keys, TabKeys}, {lock, Lock},
						{lockTime, LockTime}, {timeout, TimeOut}]),
					{err, E2}
			after
				erase_transaction_only_one()
			end;
		Reason ->
			Reason
	end.

%% -----------------------------------------------------------------
%% Description:获取数据
%% Returns:{err, E} | {ok, D}
%% -----------------------------------------------------------------
read(Tab, Key) ->
	read(Tab, Key, 0, 0, ?Read_Simple_TimeOut).
read(Tab, Key, Lock0, LockTime, TimeOut) ->
	Pid = wx_db_server:tab_to_pid(Tab),
	read(Pid, Tab, Key, Lock0, LockTime, TimeOut).
read(Pid, Tab, Key, Lock0, LockTime, TimeOut) ->
	Lock = make_lock(Lock0),
	read_(Pid, Tab, Key, Lock, LockTime, TimeOut, wx_time:now_millisec()).
read_(Pid, Tab, Key, Lock, LockTime, TimeOut, Now) when TimeOut > 0 ->
	try
		case gen_server:call(Pid, {r, Key, Lock, LockTime, ?Multi_False}, TimeOut) of
			{ok, _} = R ->
				R;
			{err_lock, OldLock, OldCode, OldFromPid, OldMulti} ->
				if
					TimeOut > ?Lock_Sleep_Time ->
						timer:sleep(?Lock_Sleep_Time),
						Now1 = wx_time:now_millisec(),
						read_(Pid, Tab, Key, Lock, LockTime, Now + TimeOut - Now1, Now1);
					true ->
						erlang:error([read_timeout, {tab, Tab}, {key, Key}, {old_lock, OldLock},
							{old_code, OldCode}, {pid, OldFromPid}, {oldMulti, OldMulti}])
				end
		end
	catch
		_E1:E2:E3 ->
			unlock(Pid, Key, Lock),
			?ErrDb([read_err, {reason, E2}, {tab, Tab}, {key, Key},
				{lock, Lock}, {lock_time, LockTime}, {timeout, TimeOut},
				E3]),
			{err, E2}
	end;
read_(_Pid, Tab, Key, _Lock, _LockTime, _TimeOut, _Now) ->
	erlang:error([read_timeout, {tab, Tab}, {key, Key}]).

%% -----------------------------------------------------------------
%% Description:删除单条数据（带事务
%% Inputs:
%% Returns:
%% -----------------------------------------------------------------
delete(Tab, Key, Lock0, LockTime, TimeOut) ->
	Lock = make_lock(Lock0),
	Pid = wx_db_server:tab_to_pid(Tab),
	delete(Pid, Tab, Key, Lock, LockTime, TimeOut).
delete(Pid, Tab, Key, Lock, LockTime, TimeOut) ->
	try
		case read(Pid, Tab, Key, Lock, LockTime, TimeOut) of
			{ok, none} ->
				ok;
			{ok, _} ->
				delete_(Pid, Tab, Key, Lock)
		end
	catch
		_E:E2:E3 ->
			unlock(Pid, Key, Lock),
			?ErrDb([delete_err, {reason, E2}, {tab, Tab}, {key, Key},
				{lock, Lock}, E3]),
			{err, E2}
	end.

delete_(Pid, Tab, Key, Lock) ->
	case gen_server:call(Pid, {d, Key, Lock}) of
		{ok, ok} ->
			ok;
		{err_lock, OldLock, OldCode, OldFromPid} ->
			erlang:error([lock_timeout, {tab, Tab}, {key, Key}, {old_lock, OldLock},
				{old_code, OldCode}, {pid, OldFromPid}])
	end.


%==========================DEFINE=======================
make_lock({_, _} = Lock) ->
	Lock;
make_lock(Lock) ->
	{Lock, self()}.

set_result(Local, Val, Result) ->
	setelement(Local, Result, setelement(2, element(Local, Result), Val)).

check_result(Left, Right, Result) ->
	case check_result_(Left, Right, Result, ok) of
		wait ->
			wait;
		ok ->
			check_result_(1, Left, Result, ok);
		Status ->
			Status
	end.

check_result_(Left, Right, Result, Status) when Left =< Right ->
	case element(2, element(Right, Result)) of
		?WaitData ->
			wait;
		lock ->
			check_result_(Left, Right - 1, Result, lock);
		_ ->
			check_result_(Left, Right - 1, Result, Status)
	end;
check_result_(_, _, _, Status) ->
	Status.

%%get_ref({_, {_, OldRef}}) ->
%%	OldRef;
%%get_ref({_, OldRef}) ->
%%	OldRef.

%% 处理业务逻辑执行完后的修改数据
handle_result([{Index, Data} | T], TabKeys) ->
	{_, _, {Pid, Tab, Key}} = element(Index, TabKeys),
	TabKeys1 = setelement(Index, TabKeys, {Index, {Pid, Tab, Key, Data}}),
	handle_result(T, TabKeys1);
handle_result([], Result) ->
	Result.

%% 处理业务逻辑执行完后的新增数据
handle_insert_result([{Tab, Key, Val} | T], Index, Result) ->
	Pid = wx_db_server:tab_to_pid(Tab),
	handle_insert_result(T, Index + 1, [{Index, {Pid, Tab, Key, Val}} | Result]);
handle_insert_result([], _, R) ->
	list_to_tuple(R).

%% 解析tab key
parse_tab_key([{Tab, Key, Default} | T], Index, TabKeyResult, DefaultResult) ->
	Pid = wx_db_server:tab_to_pid(Tab),
	parse_tab_key(T, Index + 1, [{Index, ?WaitData, {Pid, Tab, Key}} | TabKeyResult], [Default | DefaultResult]);
parse_tab_key([{Tab, Key} | T], Index, TabKeyResult, DefaultResult) ->
	Pid = wx_db_server:tab_to_pid(Tab),
	parse_tab_key(T, Index + 1, [{Index, ?WaitData, {Pid, Tab, Key}} | TabKeyResult], [none | DefaultResult]);
parse_tab_key([], _Index, TabKeyResult, DefaultResult) ->
	{lists:reverse(TabKeyResult), DefaultResult}.

add_default([H | T], Index, TabKeys, Result) ->
	Result1 =
		case element(Index, TabKeys) of
			{_, none, _} ->
				[{Index, H} | Result];
			{_, D, _} ->
				[{Index, D} | Result]
		end,
	add_default(T, Index - 1, TabKeys, Result1);
add_default([], _, _, Result) ->
	Result.


%% -----------------------------------------------------------------
%% Description:获取多个tab_keys
%% Returns:
%% -----------------------------------------------------------------
reads(TabKeys, Ref, Lock, LockTime, TimeOut) ->
	reads(TabKeys, Ref, Lock, LockTime, TimeOut, wx_time:now_millisec()).
reads(TabKeys, Ref, Lock, LockTime, TimeOut, Now) when LockTime > TimeOut, TimeOut > 0 ->
	TabKeys1 =
		case
			reads_send(TabKeys, Ref, Lock, LockTime) of
			ok ->
				list_to_tuple(TabKeys);
			TabKeysTmp ->
				TabKeysTmp
		end,
	case reads_receive(TabKeys1, Ref, Lock, LockTime) of
		{ok, TabKeys2} ->
			?DEBUG([reads_receive_total_ok, {ref, Ref}]),
			TabKeys2;
		{retry_simple, TabKeys2} ->
			if
				TimeOut > (?Lock_Try_Sleep_Time + ?Lock_Try_Sleep_Time) ->
					SleepTime = wx_lib:rand(?Lock_Try_Sleep_Time, ?Lock_Try_Sleep_Time_Max),
					?DEBUG([reads_retry_simple, {ref, Ref}, {sleep, SleepTime}]),
					timer:sleep(SleepTime),
					Now1 = wx_time:now_millisec(),
					Diff = Now1 - Now,
					reads(TabKeys2, Ref, Lock, LockTime - Diff, TimeOut - Diff, Now1);
				true ->
					erlang:error([read_timeout, [{tab_keys, TabKeys2}, {lock, Lock},
						{lock_time, LockTime}, {timeout, TimeOut}]])
			end;
		{retry_all, TabKeys2} ->
			TabKeys3 = unlocks(TabKeys2, Lock),
			transaction_sleep(tuple_to_list(TabKeys3), Ref, Lock, LockTime, TimeOut, ?Lock_Sleep_Time, Now)
	end;
reads(TabKeys, Ref, Lock, LockTime, TimeOut, _Now) ->
	?ErrDb([reads_timeout, {ref, Ref}, {tab_keys, TabKeys}, {lock, Lock},
		{lockTime, LockTime}, {timeout, TimeOut}]),
	erlang:error({reads_timeout, {ref, Ref}, {tab_keys, TabKeys}, {lock, Lock},
		{lockTime, LockTime}, {timeout, TimeOut}}).
transaction_sleep(TabKeys2, Ref, Lock, LockTime, TimeOut, SleepTime, Now) ->
	if
		TimeOut > (SleepTime + SleepTime) ->
			SleepTime1 = wx_lib:rand(SleepTime, ?Lock_Sleep_Time_Max),
			?DEBUG([reads_retry_all, {ref, Ref}, {sleep, SleepTime1}]),
			timer:sleep(SleepTime1),
			Now1 = wx_time:now_millisec(),
			reads(TabKeys2, Ref, Lock, LockTime, Now + TimeOut - Now1, Now1);
		true ->
			erlang:error([transaction_timeout_try_read, [{tab_keys, TabKeys2}, {lock, Lock},
				{lock_time, LockTime}, {timeout, TimeOut}]])
	end.
reads_receive(TabKeys, Ref, Lock, LockTime) ->
	receive
		{{Local, Ref}, {ok, Val}} ->
			?DEBUG([reads_recive_ok, {ref, Ref}, {local, Local}, {tab_keys, TabKeys}, {lockTime, LockTime}]),
			TabKeys1 = set_result(Local, Val, TabKeys),
			case check_result(Local, size(TabKeys1), TabKeys1) of
				ok ->
					{ok, TabKeys1};
				wait ->
					reads_receive(TabKeys1, Ref, Lock, LockTime);
				lock ->
					{retry_simple, TabKeys1}
			end;
	%% 等待锁超时
		{{Local, Ref}, {err_lock, _OldLock, _OldCode, _OldFrom, IsMulti}} ->
			?DEBUG([reads_recive_err_lock, {ref, Ref}, {local, Local}, {tab_keys, TabKeys}, {lockTime, LockTime}]),
			TabKeys1 = set_result(Local, lock, TabKeys),
			case check_result(Local, size(TabKeys1), TabKeys1) of
				ok ->
					{ok, TabKeys1};
				wait ->
					reads_receive(TabKeys1, Ref, Lock, LockTime);
				lock when IsMulti =:= 0 ->%% 为单key锁，则再次尝试
					{retry_simple, TabKeys1};
				lock ->
%%					OldRef = get_ref(OldFrom),
%%					if
%%						Ref < OldRef ->%% 如果当前锁是先来的，则再次尝试
%%							{retry_simple, TabKeys1};
%%						true ->
%%							{retry_all, TabKeys1}
%%					end
					%% 直接放弃、整体响应更快、
					{retry_all, TabKeys1}
			end
	end.

%% -----------------------------------------------------------------
%% Description:解锁多keys
%% Returns:
%% -----------------------------------------------------------------
unlocks(TabKeys, Lock) when is_tuple(TabKeys) ->
	unlock_tuple(TabKeys, 1, size(TabKeys), Lock);
unlocks(TabKeys, Lock) when is_list(TabKeys) ->
	unlock_list(TabKeys, Lock).
unlock_list([{_Index, _, {Pid, _Tab, Key}} | T], Lock) ->
	erlang:send(Pid, {'$gen_call', {self(), unlock}, {un, Key, Lock}}),
	unlock_list(T, Lock);
unlock_list([], _) ->
	ok.
unlock_tuple(TabKeys, Left, Right, Lock) when Left =< Right ->
	TabKeys1 = set_result(Left, ?WaitData, TabKeys),
	case element(Left, TabKeys) of
		{_, lock, _} ->
			unlock_tuple(TabKeys1, Left + 1, Right, Lock);
		{_, _, {Pid, _Tab, Key}} ->
			erlang:send(Pid, {'$gen_call', {self(), unlock}, {un, Key, Lock}}),
			unlock_tuple(TabKeys1, Left + 1, Right, Lock)
	end;
unlock_tuple(TabKeys, _, _, _) ->
	TabKeys.
%% -----------------------------------------------------------------
%% Description:解锁单key
%% Returns:
%% -----------------------------------------------------------------
unlock(Pid, Key, Lock) ->
	erlang:send(Pid, {'$gen_call', {self(), unlock}, {un, Key, Lock}}).

write(Pid, Tab, Key, Val, Lock0) ->
	Lock = make_lock(Lock0),
	?DEBUG([write, [{pid, Pid}, {tab, Tab}, {key, Key}, {val, Val}, {lock, Lock}]]),
	try
		case gen_server:call(Pid, {w, Key, Val, Lock}) of
			{ok, ok} ->
				ok;
			{err_lock, OldLock, OldCode, OldFromPid} ->
				erlang:error([lock_timeout, {tab, Tab}, {key, Key}, {old_lock, OldLock},
					{old_code, OldCode}, {pid, OldFromPid}])
		end
	catch
		_E:E2:E3 ->
			unlock(Pid, Key, Lock),
			?ErrDb([write_err, {reason, E2}, {tab, Tab}, {key, Key},
				{lock, Lock}, E3]),
			{err, E2}
	end.

writes(TabKeys, Ref, Lock) ->
	writes_send(TabKeys, 1, size(TabKeys), Ref, Lock),
	writes_receive(TabKeys, Ref, Lock).
writes_receive(TabKeys, Ref, Lock) ->
	receive
		{{Local, Ref}, {ok, ok}} ->
			TabKeys1 = set_result(Local, ok, TabKeys),
			case check_result(Local, size(TabKeys1), TabKeys1) of
				ok ->
					ok;
				wait ->
					writes_receive(TabKeys1, Ref, Lock)
			end;
	%% Lock锁超时了
		{{_Local, Ref}, {err_lock, OldLock, OldCode, OldFrom, IsMulti}} ->
			erlang:error([transaction_timeout_write, [{tab_keys, TabKeys}, {lock, Lock},
				{oldLock, OldLock}, {oldCode, OldCode},
				{oldFrom, OldFrom}, {isMulti, IsMulti}]])
	end.
writes_send(TabKeys, Left, Right, Ref, Lock) when Left =< Right ->
	case element(Left, TabKeys) of
		{_, {Pid, _Tab, Key, Val}} ->
			erlang:send(Pid, {'$gen_call', {self(), {Left, Ref}}, {w, Key, Val, Lock}});
		{_, {Pid, _Tab, Key}} ->
			erlang:send(Pid, {'$gen_call', {self(), {Left, Ref}}, {un, Key, Lock}})
	end,
	writes_send(TabKeys, Left + 1, Right, Ref, Lock);
writes_send(_, _, _, _, _) ->
	ok.
%%writes_send([{Index, {Pid, _Tab, Key, Val}} | T], Ref, Lock) ->
%%	erlang:send(Pid, {'$gen_call', {self(), {Index, Ref}}, {w, Key, Val, Lock}}),
%%	writes_send(T, Ref, Lock);
%%writes_send([], _Ref, _Lock) ->
%%	ok.

reads_send(TabKeys, Ref, Lock, LockTime) when is_list(TabKeys) ->
	?DEBUG([reads_send_list, {tab_keys, TabKeys}, {ref, Ref}, {lock, Lock}, {lock, LockTime}]),
	reads_send_list(TabKeys, Ref, Lock, LockTime);
reads_send(TabKeys, Ref, Lock, LockTime) when is_tuple(TabKeys) ->
	?DEBUG([reads_send_tuple, {tab_keys, TabKeys}, {ref, Ref}, {lock, Lock}, {lock, LockTime}]),
	reads_send_tuple(TabKeys, 1, size(TabKeys), Ref, Lock, LockTime).
reads_send_list([{Index, _, {Pid, _Tab, Key}} | T], Ref, Lock, LockTime) ->
	erlang:send(Pid, {'$gen_call', {self(), {Index, Ref}}, {r, Key, Lock, LockTime, ?Multi_True}}),
	reads_send_list(T, Ref, Lock, LockTime);
reads_send_list([], _, _, _) ->
	ok.
reads_send_tuple(TabKeys, Left, Right, Ref, Lock, LockTime) when Left =< Right ->
	case element(Left, TabKeys) of
		{_, lock, {Pid, _Tab, Key}} ->
			TabKeys1 = set_result(Left, Left, TabKeys),
			erlang:send(Pid, {'$gen_call', {self(), {Left, Ref}}, {r, Key, Lock, LockTime, ?Multi_True}}),
			reads_send_tuple(TabKeys1, Left + 1, Right, Ref, Lock, LockTime);
		_ ->
			reads_send_tuple(TabKeys, Left + 1, Right, Ref, Lock, LockTime)
	end;
reads_send_tuple(TabKeys, _, _, _, _, _) ->
	TabKeys.

%% 检测唯一事务
check_transaction_only_one() ->
	case get(?TransactionOnlyOne) of
		undefined ->
			put(?TransactionOnlyOne, 1), ok;
		_ ->
			{err, "Transactions cannot be nested."}
	end.
%% 擦掉唯一事务标识
erase_transaction_only_one() ->
	erase(?TransactionOnlyOne).