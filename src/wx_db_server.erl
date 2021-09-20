-module(wx_db_server).
%%%=======================STATEMENT====================
-description("wx_db_server").
-copyright('').
-author("wmh, SuperMuscleMan@outlook.com").
-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([load/1, tab_to_pid/1, get_statistics/0, get_statistics/1]).

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

-define(Tab_To_Pid, t2p).
-define(Pid_To_Tab, p2t).

-record(state, {}).

-define(Load_TimeOut, 30000).%% 加载等待时长30秒
%%%===================================================================
%%% API
%%%===================================================================
get_statistics() ->
	Ref = make_ref(),
	Self = self(),
	List = get_all_tab_pid(),
	[receive {Ref, S} -> S end || _ <-
		[erlang:send(Pid, {'$gen_call', {Self, Ref}, statistics}) || {_, Pid} <- List]].
get_statistics(Tab) ->
	Pid = tab_to_pid(Tab),
	wx_db_table:get_statistics(Pid).

load(Mf) ->
	gen_server:call(?MODULE, {load, Mf}, ?Load_TimeOut).
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
	process_flag(trap_exit, true),
	ets:new(?Tab_To_Pid, [set, protected, named_table, {keypos, 1}]),
	ets:new(?Pid_To_Tab, [set, protected, named_table, {keypos, 1}]),
	{ok, #state{}, ?Hibernate_TimeOut}.

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
handle_call({load, Mf}, _From, #state{} = State) ->
	ok = on_load(Mf),
	{reply, ok, State, ?Hibernate_TimeOut};
handle_call(_Request, _From, State) ->
	{reply, ok, State, ?Hibernate_TimeOut}.

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
handle_info({'EXIT', From, Reason}, State) ->
	Tab = pid_to_tab(From),
	?ErrDb([{db_exit, From}, {tab, Tab}, {reason, Reason}]),
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
on_load({M, F}) ->
%%	TablePath = wx_cfg:get('var_g', 'TablePath'),
%%	CurProject = wx_cfg:get('var_g', 'CurProject'),
%%	case filelib:is_dir(TablePath) of
%%		false ->
%%			ok = file:make_dir(TablePath);
%%		_ ->
%%			ok
%%	end,
	
	List =
		case wx_db:get_all() of
			none ->
				[];
			V ->
				V
		end,
	Set = on_load_apply(List, M, F, gb_sets:empty()),
	on_load_receive(Set).
on_load_receive(Set) ->
	case gb_sets:is_empty(Set) of
		true ->
			ok;
		_ ->
			receive
				{load_ok, From} ->
					Set1 = gb_sets:delete(From, Set),
					on_load_receive(Set1);
				Err ->
					error(Err)
			end
	end.
on_load_apply([{Tab, _} = E | T], M, F, Set) ->
	{ok, Pid} = erlang:apply(M, F, [[self(), E]]),
	ets:insert(?Tab_To_Pid, {Tab, Pid}),
	ets:insert(?Pid_To_Tab, {Pid, Tab}),
	on_load_apply(T, M, F, gb_sets:insert(Pid, Set));
on_load_apply([], _, _, Set) ->
	Set.

get_all_tab_pid() ->
	ets:tab2list(?Tab_To_Pid).

pid_to_tab(Pid) ->
	case ets:lookup(?Pid_To_Tab, Pid) of
		[{_, Tab}] -> Tab;
		_ ->
			erlang:error([not_tab, {pid, Pid}])
	end.

tab_to_pid(Tab) ->
	case ets:lookup(?Tab_To_Pid, Tab) of
		[{_, Pid}] -> Pid;
		_ ->
			erlang:error([not_tab, {tab, Tab}])
	end.