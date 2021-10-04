-module(wx_db_table).
%%%=======================STATEMENT====================
-description("wx_db_table").
-copyright('').
-author("wmh, SuperMuscleMan@outlook.com").
-behaviour(gen_server).

%% API
-export([start_link/2]).
-export([get_statistics/1]).

%% gen_server callbacks
-export([init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3]).

-define(SERVER, ?MODULE).
-define(Hibernate_TimeOut, 10000). %%休眠超时时间(毫秒)
-include_lib("wx_log_library/include/wx_log.hrl").

-define(StoreModule, "wx_db_store_").
-define(Handle_Write, w). %% 写
-define(Handle_Read, r).%% 读
-define(Handle_Delete, d).%% 删
-define(Handle_Lock, l).%% 锁
-define(Handle_UnLock, un).%% 解锁

-define(DelayTimer, delay_timer).                %% 延迟dick
-define(DelayTimerMilliSecond, 3000).            %% 延迟3s
-define(DelayStoreTree, 'delay_up_tree').        %% 延迟存储树dick
-define(DelayUp, 1).        %% 延迟更新
-define(DelayDel, 0).        %% 延迟删除

-record(state, {store_m, tab, pid, parent}).
-define(Statistics, 'statistics_db').
-record(statistics, {
	%% 锁次数、解锁次数、锁实际总计时间、锁超时次数、锁最短时长、锁最大时长、
	lock = 0, unlock = 0, lock_time = 0, lock_timeout = 0, lock_min_time = 0, lock_max_time = 0,
	%% 读次数（无锁、读次数（有锁、
	read = 0, read_lock = 0,
	%% 写次数、删次数、
	write = 0, delete = 0
}).


%%%===================================================================
%%% API
%%%===================================================================
get_statistics(Pid) ->
	gen_server:call(Pid, statistics).
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(Parent :: pid(), Args :: term()) ->
	{ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Parent, Args) ->
	WordSize = erlang:system_info(wordsize),
	gen_server:start_link(?MODULE, {Parent, Args},
		[{spawn_opt, [{min_heap_size, 256 * 1024 div WordSize}, {fullsweep_after, 4096}]}]).

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
init({Parent, {Tab, Opts}}) ->
	erlang:register(Tab, self()),
	put(?Statistics, #statistics{}),
	Type = wx_lib:get_value(Opts, type),
	StoreM = list_to_atom(lists:concat([?StoreModule, Type])),
	{ok, Pid} = apply(StoreM, start_link, [Tab, self()]),
	{ok, #state{store_m = StoreM, tab = Tab, pid = Pid, parent = Parent}, ?Hibernate_TimeOut}.

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
handle_call({Func, Key, Lock, LockTime, IsMulti}, From, State) when Func =:= ?Handle_Read ->
	R = handle(Func, Key, 0, Lock, LockTime, IsMulti, From, State),
	gen_server:reply(From, R),
	{noreply, State, ?Hibernate_TimeOut};
handle_call({Func, Key, Val, Lock}, From, State) when Func =:= ?Handle_Write ->
	R = handle(Func, Key, Val, Lock, 0, 0, From, State),
	gen_server:reply(From, R),
	{noreply, State, ?Hibernate_TimeOut};
handle_call({Func, Key, Lock}, From, State) when Func =:= ?Handle_Delete ->
	R = handle(Func, Key, 0, Lock, 0, 0, From, State),
	gen_server:reply(From, R),
	{noreply, State, ?Hibernate_TimeOut};
handle_call({Func, Key, Lock, LockTime}, From, State) when Func =:= ?Handle_Lock ->
	R = handle(Func, Key, 0, Lock, LockTime, 0, From, State),
	gen_server:reply(From, R),
	{noreply, State, ?Hibernate_TimeOut};
handle_call({Func, Key, Lock}, From, State) when Func =:= ?Handle_UnLock ->
	handle(Func, Key, 0, Lock, 0, 0, From, State),
%%	gen_server:reply(From, R),
	{noreply, State, ?Hibernate_TimeOut};
handle_call(statistics, _From, #state{tab = Tab} = State) ->
	{reply, {Tab, get(?Statistics)}, State, ?Hibernate_TimeOut}.
%%handle_call({Func, Key, Val, Lock, LockTime}, From, State) ->
%%	R = handle(Func, Key, Val, Lock, LockTime, 0, From, State),
%%	gen_server:reply(From, R),
%%	{noreply, State}.

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
handle_cast(load_ok, #state{tab = Tab, parent = Parent} = State) ->
	ets:new(Tab, [named_table, set, {keypos, 1}, {read_concurrency, true}, {write_concurrency, false}]),
	%% 同步到内存
	dets:to_ets(Tab, Tab),
	erlang:send(Parent, {load_ok, self()}),
	{noreply, State, ?Hibernate_TimeOut};
handle_cast(_Request, State) ->
	{noreply, State, ?Hibernate_TimeOut}.

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
handle_info(delay_store, State) ->
	erase(?DelayTimer),
	do_delay_store(State),
	{noreply, State, ?Hibernate_TimeOut};
handle_info(timeout, State) ->
	{noreply, State, hibernate};
handle_info(_Info, State) ->
	{noreply, State, ?Hibernate_TimeOut}.

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
handle(Code, Key, Info, Lock, LockTime, IsMulti, FromPid, #state{tab = Tab} = State) ->
	?DEBUG([Code, Key, Info, Lock, LockTime, IsMulti, FromPid, State]),
	Now = wx_time:now_millisec(),
	case get(Key) of
		undefined ->
			if
				(LockTime > 0) ->
					handle_statistics(Code, 1, 0, 0, 0),
					put(Key, {Lock, Code, FromPid, IsMulti, Now, Now + LockTime});
				true ->
					handle_statistics(Code, 0, 0, 0, 0),
					ok
			end,
			D = handle_(Code, Key, Info, State),
			{ok, D};
		{Lock, OldCode, _, _, OldNow, OldLockEndTime} = OldLockInfo when OldCode =/= Code ->
			erase(Key),
			D = handle_(Code, Key, Info, State),
			if (Now > OldLockEndTime) ->
				err_lock_timeout(Tab, Now, OldLockInfo),
				handle_statistics(Code, 0, 1, 1, Now - OldNow);
				true ->
					handle_statistics(Code, 0, 1, 0, Now - OldNow)
			end,
			{ok, D};
		{_OldLock, _OldCode, _OldFromPid, _OldIsMulti, OldNow, OldLockEndTime} = OldLockInfo when Now > OldLockEndTime ->
			err_lock_timeout(Tab, Now, OldLockInfo),
			if
				(LockTime > 0) ->
					handle_statistics(Code, 1, 1, 1, Now - OldNow),
					put(Key, {Lock, Code, FromPid, IsMulti, Now, Now + LockTime});
				true ->
					handle_statistics(Code, 0, 1, 1, Now - OldNow),
					erase(Key)
			end,
			D = handle_(Code, Key, Info, State),
			{ok, D};
		{OldLock, OldCode, OldFromPid, OldIsMulti, _OldNow, _OldLockEndTime} ->
			{err_lock, OldLock, OldCode, OldFromPid, OldIsMulti}
	end.
handle_(?Handle_Read, Key, _Info, #state{tab = Tab}) ->
	get_data(Tab, Key);
handle_(?Handle_Write, Key, Info, #state{tab = Tab, store_m = StoreM, pid = StorePid}) ->
	set_data(Tab, StoreM, StorePid, Key, Info),
	ok;
handle_(?Handle_Delete, Key, _, #state{tab = Tab, store_m = StoreM, pid = StorePid}) ->
	del_data(Tab, StoreM, StorePid, Key),
	ok;
handle_(?Handle_Lock, _Key, _Info, _State) ->
	ok;
handle_(?Handle_UnLock, _Key, _Info, _State) ->
	ok.

%% 统计信息
handle_statistics(?Handle_Read, IsLock, IsUnLock, IsTimeOut, RealLockTime) ->
	#statistics{read = Read, read_lock = ReadLock, lock = Lock, unlock = UnLock,
		lock_time = LockTime, lock_timeout = TimeOut,
		lock_min_time = MinTime, lock_max_time = MaxTime} = S = get(?Statistics),
	put(?Statistics, S#statistics{read = Read + (1 - IsLock),
		read_lock = ReadLock + IsLock, lock = Lock + IsLock,
		lock_time = LockTime + RealLockTime, unlock = UnLock + IsUnLock,
		lock_timeout = TimeOut + IsTimeOut,
		lock_min_time = min(MinTime, RealLockTime),
		lock_max_time = max(MaxTime, RealLockTime)});
handle_statistics(?Handle_Write, IsLock, IsUnLock, IsTimeOut, RealLockTime) ->
	#statistics{write = Write, lock = Lock, lock_time = LockTime,
		unlock = UnLock, lock_timeout = TimeOut,
		lock_min_time = MinTime, lock_max_time = MaxTime} = S = get(?Statistics),
	put(?Statistics, S#statistics{write = Write + 1, lock = Lock + IsLock,
		lock_time = LockTime + RealLockTime, unlock = UnLock + IsUnLock,
		lock_timeout = TimeOut + IsTimeOut,
		lock_min_time = min(MinTime, RealLockTime),
		lock_max_time = max(MaxTime, RealLockTime)});
handle_statistics(?Handle_Lock, IsLock, IsUnLock, IsTimeOut, RealLockTime) ->
	#statistics{lock = Lock, lock_time = LockTime, unlock = UnLock,
		lock_timeout = TimeOut,
		lock_min_time = MinTime, lock_max_time = MaxTime} = S = get(?Statistics),
	put(?Statistics, S#statistics{lock = Lock + IsLock,
		lock_time = LockTime + RealLockTime, unlock = UnLock + IsUnLock,
		lock_timeout = TimeOut + IsTimeOut,
		lock_min_time = min(MinTime, RealLockTime),
		lock_max_time = max(MaxTime, RealLockTime)});
handle_statistics(?Handle_UnLock, _IsLock, IsUnLock, IsTimeOut, RealLockTime) ->
	#statistics{unlock = UnLock, lock_time = LockTime,
		lock_timeout = TimeOut,
		lock_min_time = MinTime, lock_max_time = MaxTime} = S = get(?Statistics),
	put(?Statistics, S#statistics{unlock = UnLock + IsUnLock,
		lock_time = LockTime + RealLockTime, lock_timeout = TimeOut + IsTimeOut,
		lock_min_time = min(MinTime, RealLockTime),
		lock_max_time = max(MaxTime, RealLockTime)});
handle_statistics(?Handle_Delete, IsLock, IsUnLock, IsTimeOut, RealLockTime) ->
	#statistics{delete = Delete, lock = Lock, lock_time = LockTime,
		unlock = UnLock, lock_timeout = TimeOut,
		lock_min_time = MinTime, lock_max_time = MaxTime} = S = get(?Statistics),
	put(?Statistics, S#statistics{delete = Delete + 1, lock = Lock + IsLock,
		lock_time = LockTime + RealLockTime, unlock = UnLock + IsUnLock,
		lock_timeout = TimeOut + IsTimeOut,
		lock_min_time = min(MinTime, RealLockTime),
		lock_max_time = max(MaxTime, RealLockTime)}).

%%write(Key, Val, #state{tab = Tab,
%%	store_m = StoreM, pid = StorePid}) ->
%%	Now = wx_time:now_millisec(),
%%	case get(Key) of
%%		undefined ->
%%			ets:insert(Tab, {Key, Val}),
%%			StoreM:write(StorePid, Key, Val),
%%			ok;
%%		{_OldLock, _OldFromPid, _OldNow, OldLockEndTime} = OldLockInfo
%%			when Now > OldLockEndTime ->
%%			ets:insert(Tab, {Key, Val}),
%%			StoreM:write(StorePid, Key, Val),
%%%%			put(Key, {Lock, FromPid, Now, Now + LockTime}),
%%			err_lock_timeout(Tab, Now, OldLockInfo),
%%			ok;
%%		{_OldLock, _OldFromPid, _OldNow, OldLockEndTime} ->
%%			{err_lock, OldLockEndTime - Now}
%%	end.
%%read(Key, Lock0, LockTime, FromPid, #state{tab = Tab}) ->
%%	Now = wx_time:now_millisec(),
%%	Lock = make_lock(Lock0, FromPid),
%%	case get(Key) of
%%		undefined when LockTime > 0 ->
%%			put(Key, {Lock, FromPid, Now, Now + LockTime}),
%%			D = get_data(Tab, Key),
%%			{ok, D};
%%		undefined ->
%%			D = get_data(Tab, Key),
%%			{ok, D};
%%		{_OldLock, _OldFromPid, _OldNow, OldLockEndTime} = OldLockInfo when Now > OldLockEndTime ->
%%			err_lock_timeout(Tab, Now, OldLockInfo),
%%			if
%%				(LockTime > 0) ->
%%					put(Key, {Lock, FromPid, Now, Now + LockTime});
%%				true ->
%%					ok
%%			end,
%%			D = get_data(Tab, Key),
%%			{ok, D};
%%		{_OldLock, _OldFromPid, _OldNow, OldLockEndTime} ->
%%			{err_lock, OldLockEndTime - Now}
%%	end.

get_data(Tab, Key) ->
	case ets:lookup(Tab, Key) of
		[{_, D}] ->
			D;
		[] ->
			none
	end.
set_data(Tab, StoreM, StorePid, Key, Val) ->
	true = ets:insert(Tab, {Key, Val}),
	delay_store(Key, ?DelayUp).
%%	StoreM:write(StorePid, Key, Val).
del_data(Tab, StoreM, StorePid, Key) ->
	true = ets:delete(Tab, Key),
	delay_store(Key, ?DelayDel).
%%	StoreM:delete(StorePid, Key).

err_lock_timeout(Tab, Now, {OldLock, OldCode, OldFromPid, OldMulti, OldNow, OldLockEndTime}) ->
	?ErrDb([db_timeout, {tab, Tab}, {oldlock, OldLock}, {oldcode, OldCode}, {oldfrom_pid, OldFromPid},
		{oldMulti, OldMulti},
		{oldlock_timeout, OldLockEndTime - OldNow}, {oldlock_time_real, Now - OldNow}]).

%% 延迟树
delay_store(Key, Type) ->
	Tree = get_delay_store(),
	case gb_trees:lookup(Key, Tree) of
		{value, Type} ->
			ok;
		{value, _} ->
			set_delay_store(gb_trees:update(Key, Type, Tree)),
			delay_timer();
		none ->
			set_delay_store(gb_trees:insert(Key, Type, Tree)),
			delay_timer()
	end.
get_delay_store() ->
	case get(?DelayStoreTree) of
		undefined ->
			gb_trees:empty();
		V ->
			V
	end.
set_delay_store(Tree) ->
	put(?DelayStoreTree, Tree).
erase_delay_store() ->
	erase(?DelayStoreTree).
delay_timer() ->
	case get(?DelayTimer) of
		undefined ->
			put(?DelayTimer, erlang:send_after(?DelayTimerMilliSecond, self(), delay_store));
		Timer ->
			case erlang:read_timer(Timer) of
				false ->
					put(?DelayTimer, erlang:send_after(?DelayTimerMilliSecond, self(), delay_store));
				_ ->
					ok
			end
	end.

do_delay_store(#state{store_m = StoreM, pid = StorePid}) ->
	Tree = get_delay_store(),
	erase_delay_store(),
	Iterator = gb_trees:iterator(Tree),
	List = split_tree(gb_trees:next(Iterator), [], []),
	StoreM:store_batch(StorePid, List).
split_tree({K, ?DelayUp, Iterator}, UpList, DelList) ->
	split_tree(gb_trees:next(Iterator), [K | UpList], DelList);
split_tree({K, ?DelayDel, Iterator}, UpList, DelList) ->
	split_tree(gb_trees:next(Iterator), UpList, [K | DelList]);
split_tree(none, UpList, DelList) ->
	{UpList, DelList}.
