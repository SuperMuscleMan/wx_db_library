%%%-------------------------------------------------------------------
%%% @author WeiMengHuan
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%% 数据库API
%%% @end
%%% Created : 17. 三月 2021 21:16
%%%-------------------------------------------------------------------
-module(wx_db_lib).
%%%=======================STATEMENT====================
-description("wx_db_lib").
-copyright('').
-author("wmh, SuperMuscleMan@outlook.com").
%%%=======================EXPORT=======================
-compile({inline, [{get_table_name, 2}]}).
-export([get/2, get/3, delete/3, update/5, update/6, transaction/4, get_table_name/2]).
%%%=======================INCLUDE======================

%%%=======================RECORD=======================

%%%=======================DEFINE=======================
-define(Transaction_Lock_Time, 8000).%% 事务锁时间
-define(Transaction_TimeOut, 3000).%% 事务等待锁时间

-define(Update_Lock_Time, 8000).%% 单表事务锁时间
-define(Update_Lock_TimeOut, 3000).%% 单表事务锁等待时间

-define(Delete_Lock_Time, 3000).%% 删除锁时间
-define(Delete_Lock_TimeOut, 3000).%% 删除等待时间
%%%=================EXPORTED FUNCTIONS=================

%% 获取表名
get_table_name(Src, Name) when is_list(Src) ->
	get_table_name(wx_lib:get_value(Src, tab_src), Name);
get_table_name(TabSrc, Name) ->
	case wx_cfg:get(TabSrc, Name) of
		none ->
			error([{not_table_name, Name}, {tab_src, TabSrc}]);
		V ->
			V
	end.
%% -----------------------------------------------------------------
%% Description: 获取数据
%% Returns: 
%% -----------------------------------------------------------------
get(Tab, Key) ->
	get(Tab, Key, none).
get(Tab, Key, Default) ->
	case wx_db_client:read(Tab, Key) of
		{ok, none} ->
			Default;
		{ok, V} ->
			V;
		{err, _} = Reason->
			Reason
	end.

%% -----------------------------------------------------------------
%% Description:删除数据
%% Returns:
%% -----------------------------------------------------------------
delete(Lock, Tab, Key) ->
	wx_db_client:delete(Tab, Key, Lock, ?Delete_Lock_Time, ?Delete_Lock_TimeOut).

%% -----------------------------------------------------------------
%% Description:更新数据（单表操作
%% Returns:
%% -----------------------------------------------------------------
update(Lock, Fun, Tab, Key, Args) ->
	update(Lock, Fun, Tab, Key, none, Args).
update(Lock, Fun, Tab, Key, Default, Args) ->
	wx_db_client:update(Tab, Key, Default, Fun, Lock, ?Update_Lock_Time, ?Update_Lock_TimeOut, Args).
%% -----------------------------------------------------------------
%% Description:事务处理
%% Returns:
%% -----------------------------------------------------------------
transaction(Lock, Fun, TabKeys, Args) ->
	wx_db_client:transaction(TabKeys, Fun, Lock, ?Transaction_Lock_Time, ?Transaction_TimeOut, Args).


%==========================DEFINE=======================
