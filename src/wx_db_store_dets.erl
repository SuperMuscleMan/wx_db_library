-module(wx_db_store_dets).
%%%=======================STATEMENT====================
-description("wx_db_store_file_dets").
-copyright('').
-author("wmh, SuperMuscleMan@outlook.com").
-behaviour(gen_server).

%% API
-export([start_link/2]).
-export([write/3, delete/2, store_batch/2]).

%% gen_server callbacks
-export([init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3]).

-define(SERVER, ?MODULE).
-define(HIBERNATE_TIMEOUT, 10000).
-define(Status_Close, 0).
-define(Status_Run, 1).



-record(state, {status = ?Status_Close, tab, parent}).

-include_lib("wx_log_library/include/wx_log.hrl").

%%%===================================================================
%%% API
%%%===================================================================
write(Pid, K, V) ->
	gen_server:cast(Pid, {w, K, V}).
delete(Pid, K) ->
	gen_server:cast(Pid, {d, K}).
store_batch(Pid, {UpList, DelList} = List) when is_list(UpList), is_list(DelList) ->
	gen_server:cast(Pid, {store_batch, List}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(Tab :: atom(), Parent :: pid()) ->
	{ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Tab, Parent) ->
	gen_server:start_link(?MODULE, {Tab, Parent}, []).

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
init({Tab, Parent}) ->
	process_flag(trap_exit, true),
	TabOpts = wx_db:get_cfg(Tab),
	Args = wx_lib:get_value(TabOpts, args),
	?SOUT([Tab, Parent, Args, wx_lib:get_value(Args, file)]),
	ok = filelib:ensure_dir(wx_lib:get_value(Args, file)),
	{ok, _} = dets:open_file(Tab, Args),
	gen_server:cast(Parent, load_ok),
	{ok, #state{tab = Tab, parent = Parent, status = ?Status_Run}, ?HIBERNATE_TIMEOUT}.

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
handle_call(_Request, _From, State) ->
	{reply, ok, State, ?HIBERNATE_TIMEOUT}.

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
handle_cast({store_batch, ListInfo}, #state{tab = Tab} = State) ->
	do_store_batch(Tab, ListInfo),
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_cast({w, K, V}, #state{tab = Tab} = State) ->
	ok = dets:insert(Tab, {K, V}),
	{noreply, State, ?HIBERNATE_TIMEOUT};
handle_cast({d, K}, #state{tab = Tab} = State) ->
	ok = dets:delete(Tab, K),
	{noreply, State, ?HIBERNATE_TIMEOUT}.

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
handle_info(timeout, State) ->
	{noreply, State, hibernate};
handle_info(_Info, State) ->
	{noreply, State, ?HIBERNATE_TIMEOUT}.

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
terminate(_Reason, #state{tab = Tab} = _State) ->
	?SOUT([_Reason, _State]),
	close(Tab),
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
close(Tab) ->
	dets:sync(Tab),
	dets:close(Tab).

do_store_batch(Tab, {UpList, DelList}) ->
	do_store_batch_up(Tab, UpList),
	do_store_batch_del(Tab, DelList).
do_store_batch_up(Tab, UpList) ->
	List = [lists:nth(1, ets:lookup(Tab, E)) || E <- UpList],
	dets:insert(Tab, List).
do_store_batch_del(Tab, DelList) ->
	dets_delete(Tab, DelList).
%%%-----------------------------------------------------------------
%%% The following functions are from the dets module
%%%-----------------------------------------------------------------
dets_delete(Tab, DelList) ->
	badarg(treq(Tab, {delete_key, DelList}), [Tab, DelList]).
-define(DETS_CALL(Pid, Req), {'$dets_call', Pid, Req}).
badarg(badarg, A) ->
	erlang:error(badarg, A);
badarg(Reply, _A) ->
	Reply.
treq(Tab, R) ->
	case catch dets_server:get_pid(Tab) of
		Pid when is_pid(Pid) ->
			req(Pid, R);
		_ ->
			badarg
	end.
req(Proc, R) ->
	Ref = erlang:monitor(process, Proc),
	Proc ! ?DETS_CALL(self(), R),
	receive
		{'DOWN', Ref, process, Proc, _Info} ->
			badarg;
		{Proc, Reply} ->
			erlang:demonitor(Ref, [flush]),
			Reply
	end.
%%%-----------------------------------------------------------------
%%% end
%%%-----------------------------------------------------------------