# wx\_db_library #

## 简介 ##
数据库模块，缓存用的ETS、持久化用的DETS。目前的缓存过于原始，是在启服时将所有数据载入ETS，后期会加入智能缓存（惰性加载+超时清理）。

Build
-----

    $ rebar3 compile
