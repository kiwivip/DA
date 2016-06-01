
＃项目下文件说明
    - config.ini                    ＃项目配置文件，定义数据库连接信息等
    - DA_config_start.pl            ＃项目部署至新机器时需要且仅需要运行一次，作用是自动安装依赖包
                                    ＃运行前，请确保Linux已经安装有cpanm工具，如果没有：curl -L http://cpanmin.us | perl - --sudo App::cpanminus
    - *.lock                        ＃统计运算期的文件锁，为保证单任务单实例运行，每个周期运算完毕会自动消失
    
＃备份线上crontab，这些时间的设置是根据201.94的实际性能和状态长期调整的结果，如果迁移至新机器请自行根据情况调整
30 1 * * * perl /home/DA/DataAnalysis/scpLogs.pl >>/home/DA/DataAnalysis/scp.log 2>>/home/DA/DataAnalysis/scp.log &
50 1 * * * perl /home/DA/DataAnalysis/IP2LOCATION.pl >>/home/DA/DataAnalysis/ip2location.log &
20 3 * * * perl /home/DA/DataAnalysis/cs_log_1202.pl            >>/home/DA/DataAnalysis/cs.log &
*/5 * * * * perl /home/DA/DataAnalysis/CS_day_0405.pl           >>/home/DA/DataAnalysis/cs_day.log &
15 * * * * perl /home/DA/DataAnalysis/CS_month_1214.pl          >>/home/DA/DataAnalysis/cs_month.log &
15 2 * * * perl /home/DA/DataAnalysis/ds_DA.pl                  >>/home/DA/DataAnalysis/ds.log &
20 2 * * * perl /home/DA/DataAnalysis/DA_ds_analysis.pl         >>/home/DA/DataAnalysis/ds.log &
*/30 * * * * perl /home/DA/DataAnalysis/ds_24h_0619.pl          >>/home/DA/DataAnalysis/ds_24h.log &
*/20 * * * * perl /home/DA/DataAnalysis/TY_day_0918.pl          >>/home/DA/DataAnalysis/ty.log &
*/10 * * * * perl /home/DA/DataAnalysis/CBS_0519.pl             >>/home/DA/DataAnalysis/cbs_day.log &
55 2 * * * perl /home/DA/DataAnalysis/ft_DA_0721.pl             >>/home/DA/DataAnalysis/ft.log &
10 3 * * * perl /home/DA/DataAnalysis/DA_ft_analysis_0721.pl    >>/home/DA/DataAnalysis/ft_analysis.log &
35 3 * * * perl /home/DA/DataAnalysis/l99_DA_0721.pl            >>/home/DA/DataAnalysis/l99.log &
35 4 * * * export LANG="en_US.UTF-8"; perl /home/DA/DataAnalysis/DA_l99_analysis_1028.pl        >>/home/DA/DataAnalysis/l99_analysis.log &
*/30 * * * * perl /home/DA/DataAnalysis/l99_24h_0318.pl         >>/home/DA/DataAnalysis/l99_24h.log &
*/2 * * * *  perl /home/DA/DataAnalysis/Recharge_and_Pay_0318_temp.pl                           >>/home/DA/DataAnalysis/recharge.log &


