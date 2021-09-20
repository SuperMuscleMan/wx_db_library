%%%-------------------------------------------------------------------
%%% @author WeiMengHuan
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. 9月 2021 16:43
%%%-------------------------------------------------------------------
-module(wx_db_table_sup).
%%%=======================STATEMENT====================
-description("wx_db_table_sup").
-copyright('').
-author("wmh, SuperMuscleMan@outlook.com").

-behaviour(supervisor).

%% API
-export([start_link/0, start_child/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
	{ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
	supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
	{ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
		MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
		[ChildSpec :: supervisor:child_spec()]
	}} |
	ignore |
	{error, Reason :: term()}).
init(_) ->
	SupFlags = #{strategy => simple_one_for_one,
		intensity => 3,
		period => 180},
	ChildSpecs = [
		#{id => ?SERVER,
			start => {wx_db_table, start_link, []},
			restart => permanent,
			shutdown => infinity,
			type => worker,
			modules => [wx_db_table]}
	],
	{ok, {SupFlags, ChildSpecs}}.

start_child(Args)->
	supervisor:start_child(?SERVER, Args).
%%%===================================================================
%%% Internal functions
%%%===================================================================
