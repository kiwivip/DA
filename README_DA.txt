项目工程说明文档

项目下文件功能说明
    - config.ini                    ＃项目配置文件，定义数据库连接信息等
    - DA_config_start.pl            ＃项目部署至新机器时需要且仅需要运行一次，作用是自动安装依赖包
                                    ＃运行前，请确保Linux已经安装有cpanm工具，如果没有：curl -L http://cpanmin.us | perl - --sudo App::cpanminus
    - scpLogs.pl                    ＃拷贝14/69上的所有项目的日志到本地 目前已经几乎没多少key是在这边生成的了但依然不是0还有少许的需要日志
    - IP2LOCATION.pl                ＃扫所有项目的日志解析ip存进redis供ip分析时直接提取
    - DA_CS_day.pl                  ＃床上项目，日key，大部分扫mysql
    - DA_CS_month.pl                ＃床上项目，月key，基本只扫redis 
    - DA_CS_log.pl                  ＃床上项目扫日志
    - DA_CBS_day.pl                 ＃大赢家（猜比赛）项目
    - DA_DS_day.pl                  ＃设计师项目
    - ds_24h_0619.pl                ＃设计师项目
    - DA_TY_day.pl                  ＃体育头条项目
    - ft_DA_0721.pl                 ＃第一次项目
    - DA_ft_analysis_0721.pl        ＃第一次项目
    - l99_DA_0721.pl                ＃立方网，读日志生成pv，uv等
    - DA_l99_analysis_1028.pl       ＃立方网，月key
    - l99_24h_0318.pl               ＃立方网，日key
    - Recharge_and_Pay_0318_temp.pl ＃床上/L99的充值付费统计
    - *.lock                        ＃各项目统计运算期的文件锁，为保证单任务单实例运行，每个周期运算完毕会自动消失
                                    ＃有特殊情况（如断电）导致计划任务启动不了统计脚本的话（目前还未发生），可以尝试手动删除项目的.lock文件
                                    
    - GeoLite2-City.mmdb            ＃这个是maxmind的免费ip库文件，借助Geoip进行本地化的ip解析，但事实上这种解析对中国内地的ip并不十分准确，如果需要更精确的分布请优化者改用第三方的API来解析ip
    - 
    
    
    
其它说明：

＃备份线上crontab，这些时间的设置是根据201.94的实际运算性能和状态调整的结果，如果迁移至新机器请自行根据实际情况调整，但大致遵循依次往后的时间安排
30 1 * * * perl /home/DA/DataAnalysis/scpLogs.pl >>/home/DA/DataAnalysis/scp.log 2>>/home/DA/DataAnalysis/scp.log &
50 1 * * * perl /home/DA/DataAnalysis/IP2LOCATION.pl >>/home/DA/DataAnalysis/ip2location.log &
20 3 * * * perl /home/DA/DataAnalysis/DA_CS_log.pl            >>/home/DA/DataAnalysis/cs.log &
*/5 * * * * perl /home/DA/DataAnalysis/DA_CS_day.pl           >>/home/DA/DataAnalysis/cs_day.log &
15 * * * * perl /home/DA/DataAnalysis/DA_CS_month.pl          >>/home/DA/DataAnalysis/cs_month.log &
15 2 * * * perl /home/DA/DataAnalysis/DA_DS_day.pl            >>/home/DA/DataAnalysis/ds.log &
*/30 * * * * perl /home/DA/DataAnalysis/ds_24h_0619.pl        >>/home/DA/DataAnalysis/ds_24h.log &
*/20 * * * * perl /home/DA/DataAnalysis/DA_TY_day.pl          >>/home/DA/DataAnalysis/ty.log &
*/10 * * * * perl /home/DA/DataAnalysis/DA_CBS_day.pl         >>/home/DA/DataAnalysis/cbs_day.log &
55 2 * * * perl /home/DA/DataAnalysis/ft_DA_0721.pl             >>/home/DA/DataAnalysis/ft.log &
10 3 * * * perl /home/DA/DataAnalysis/DA_ft_analysis_0721.pl    >>/home/DA/DataAnalysis/ft_analysis.log &
35 3 * * * perl /home/DA/DataAnalysis/l99_DA_0721.pl            >>/home/DA/DataAnalysis/l99.log &
35 4 * * * export LANG="en_US.UTF-8"; perl /home/DA/DataAnalysis/DA_l99_analysis_1028.pl        >>/home/DA/DataAnalysis/l99_analysis.log &
*/30 * * * * perl /home/DA/DataAnalysis/l99_24h_0318.pl         >>/home/DA/DataAnalysis/l99_24h.log &
*/2 * * * *  perl /home/DA/DataAnalysis/Recharge_and_Pay_0318_temp.pl                           >>/home/DA/DataAnalysis/recharge.log &


