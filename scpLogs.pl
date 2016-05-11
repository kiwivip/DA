#!/bin/env perl
# ==============================================================================
# function:	备份各项目的日志文件
#               床上API/猜比赛/第一次/在哪API/立方网...
# Author: 	kiwi
# createTime:	2014.6.22
# ==============================================================================
use 5.10.1;
use POSIX qw(strftime);
use Net::SSH::Perl ;                 # libmath-gmp-perl
use Net::SCP::Expect ; 
# -------------------------------------------------------------------------------
my $LOGDIR = '/logback/tongji_data/LOG' ;

#=pod
# -------------------------------------------------------------------------------
# 拷贝14/69上的nginx日志
# -------------------------------------------------------------------------------
my @hosts = ('192.168.199.14','192.168.199.69') ;
my ($port,$userName,$password) = ('3609','skstsuperadmin','@asetygag,.i78argrhje4p@@Ot.....MWEHS') ;
my @dirs = (
            'api.nyx.l99.com',                  # 床上
            'api.wwere.l99.com',                # 在哪
            'cbs.l99.com',                      # 猜比赛
            'firsttime.l99.com',                # 第一次
            'dbapi.l99.com',                    # 立方飞鸽
            'www.l99.com'                       # 立方网
) ;

for (1 .. 1)
{ 
    my $log_day = strftime("%Y%m%d",localtime(time() - 86400 * $_));
    say $log_day ;

    my $scpe = Net::SCP::Expect->new(
                        user => $userName , password => $password , port => $port ,
                        timeout => '2000' , privileged => 0
    );

    for(@hosts)
    {
        my $host = $_ ;                             # e.g.  192.168.199.14
        my ($dir_num) = $host =~ /\.(\d+)$/ ;       # 14 / 69
        # access.www.l99.com.log.20160322
        
        my $ssh = Net::SSH::Perl->new($host,port => $port);
        $ssh->login($userName, $password);
        
        for(@dirs)
        {
            my $project = $_ ; 
            # 判断本地是否存在压缩日志文件
            my $dir_log_local  = $LOGDIR . "/$project/$dir_num/" ;
            
            my $file_log_local = 'access.'.$project.'.log.'.$log_day ;          # 这个地方根据运维对日志的命名规则的调整而调整
            my $file_log_gz_local = 'access.'.$project.'_'.$log_day.'.log.gz' ;
            say "file : $host : $file_log_gz_local exists !" and next if -e $dir_log_local . $file_log_gz_local ;
            
            # 登录日志服务器压缩目标文件
            say "start to gzip $host:$dir_log_local" ;
            $ssh->cmd("cp /usr/local/nginx/logs/$file_log_local /tmp/$file_log_local");
            $ssh->cmd("gzip /tmp/$file_log_local") ;
            $ssh->cmd("mv /tmp/$file_log_local".".gz /tmp/$file_log_gz_local") ;
            #say "gzip $host:$file_log_gz_local success" ;
            
            # 拷贝压缩日志文件
            say "start to scp $host:$file_log_gz_local ... " ;
            $scpe->scp("$host:/tmp/$file_log_gz_local" , $dir_log_local);
            #say "scp $host:$file_log_gz_local success" ;
            
            # 毁尸灭迹
            $ssh->cmd("rm -f /tmp/$file_log_local") ;
            $ssh->cmd("rm -f /tmp/$file_log_gz_local") ;
        }
    }
}



