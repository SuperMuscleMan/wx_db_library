%%%-------------------------------------------------------------------
%% @doc wx_db_library top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(wx_db_library_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
	supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init(_) ->
	SupFlags = #{strategy => one_for_all,
		intensity => 0,
		period => 1},
	ChildSpecs = [
		#{id => wx_db_server,
			start => {wx_db_server, start_link, []},
			restart => permanent,
			shutdown => infinity,
			type => worker,
			modules => [wx_db_server]},
		#{id => wx_db_table_sup,
			start => {wx_db_table_sup, start_link, []},
			restart => permanent,
			shutdown => infinity,
			type => supervisor,
			modules => [wx_db_table_sup]}
	],
	{ok, {SupFlags, ChildSpecs}}.


%% internal functions
