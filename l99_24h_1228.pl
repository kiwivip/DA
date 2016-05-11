#!/usr/bin/env perl 
# ==============================================================================
# function:	*
# Author: 	kiwi
# createTime:	2015.4.8
# ==============================================================================

use 5.10.1 ;
BEGIN {
    # 如果程序迁移到新机器，需要 Linux 预配好 cpanm ，然后解掉注释
    my @PMs = (
            #'Config::Tiny',
            #'Unicode::UTF8'
	) ;
    foreach(@PMs){
            my $pm = $_ ;
            eval {require $pm;};
            if ($@ =~ /^Can't locate/) {
                    print "install module $pm";
                    `cpanm $pm`;
            }
    }
}

use utf8 ;
use autodie ;
use Data::Dumper ;
use DBI ;
use Redis;
use MongoDB ;
use Time::Local ;
use JSON::XS ;
use LWP::Simple;
use Config::Tiny ;
use POSIX qw(strftime);
use File::Lockfile ; 
use Time::Local ;
use Unicode::UTF8 qw[decode_utf8 encode_utf8];
binmode(STDIN, ':encoding(utf8)');
binmode(STDOUT, ':encoding(utf8)');
binmode(STDERR, ':encoding(utf8)');

# ----------------------------------------------------------------------------------

my $lockfile = File::Lockfile->new('l99.lock' , '/home/DA/DataAnalysis');
if ( my $pid = $lockfile->check ) {
        say "Seems that program is already running with PID: $pid";
        exit;
}
$lockfile->write;

# -----------------------------------------------------------------------
# read config.ini
# -----------------------------------------------------------------------
my $Config = Config::Tiny->new;
$Config = Config::Tiny->read( '/home/DA/DataAnalysis/config.ini', 'utf8' );
my $version = $Config -> {_} -> {version};

my $redis_host_market = $Config -> {REDIS} -> {host_market};
my $redis_host = $Config -> {REDIS} -> {host};
my $redis_port = $Config -> {REDIS} -> {port};

my $L99_log_dir         = $Config -> {L99_LOG} -> {dir} ;
my $L99_os_json         = $Config -> {L99_LOG} -> {file_os} ;
my $L99_browser_json    = $Config -> {L99_LOG} -> {file_browser} ;
my $source_topN         = $Config -> {L99_LOG} -> {source_topN} ;		# 立方网流量来源网站排行
my $weixin_article_topN = $Config -> {L99_LOG} -> {weixin_topN} ;		# 微信阅读文章 排行
my $l99_zone_topN       = $Config -> {L99_LOG} -> {l99_zone_topN} ;		# 立方网用户个人空间访问 排行
my $l99_keywords_topN   = $Config -> {L99_LOG} -> {l99_keywords_topN} ;		# 立方网最火的文章内容的关键字标签个数
my $article_topN        = $Config -> {L99_LOG} -> {article_topN} ;


my $L06_host     = $Config -> {L06_DB} -> {host};
my $L06_db       = $Config -> {L06_DB} -> {database} ;
my $L06_usr      = $Config -> {L06_DB} -> {username};
my $L06_password = $Config -> {L06_DB} -> {password};

#my $time_step = 15 ;
my $time_step = $Config -> {time} -> {step} ;					        # 设置为往前推 N天 统计，默认 N = 1 ;
my $day_today = strftime( "%Y-%m-%d", localtime(time()) );			    # 今天
my $months_ago = $time_step / 30 + 1;

# -------------------------------------------------------------------
# connect to mysql
# -------------------------------------------------------------------
my $dsn_v506 = "DBI:mysql:database=$L06_db;host=$L06_host" ;
my $dbh_v506 = DBI -> connect($dsn_v506, $L06_usr, $L06_password, {'RaiseError' => 1} ) ;
$dbh_v506 -> do ("SET NAMES UTF8");

# ---------------------------------------------------------------------
# connect to Redis & mongoDB
# ---------------------------------------------------------------------
my $redis = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
my $redis_market = Redis->new(server => "$redis_host_market:$redis_port",reconnect => 10, every => 2000);
$redis_market -> select(6) ;

my $mongo_comment = MongoDB::MongoClient->new(host => '192.168.199.102', port => 27017 , query_timeout => 1000000);
my $db_comment = $mongo_comment -> get_database( 'l_comment' ) ;
my $collection_comment = $db_comment -> get_collection( 'comment' );


#=pod
# -----------------------------------------------------
# 用户龙币排行
# -----------------------------------------------------
my $top = 1;
my $sth_money = $dbh_v506 -> prepare(" SELECT * FROM pay_account order by userMoney desc LIMIT 100 ");
$sth_money -> execute();
while (my $ref = $sth_money -> fetchrow_hashref())
{
    my %user_temp ;			                            # hash_json of redis-value
    my $accountId = $ref -> {accountId} ;
    my $money     = $ref -> {userMoney} ;
    my $frozen    = $ref -> {frozenMoney} ;
    my $balance   = $money - $frozen ;
    
    my $ref_account = get_user_from_accountId($dbh_v506,$accountId) ;
    my ($l99NO,$name) = ($ref_account->{l99NO} , $ref_account->{name}) ;
    
    $user_temp{l99NO} = $l99NO ;
    $user_temp{name}  = decode_utf8 $name ;
    $user_temp{money} = $money ;
    $user_temp{frozenMoney} = $frozen ;
    $user_temp{balance} = $balance ;
    #say join "\t" , ($top,$l99NO,$name,$money,$frozen,$balance) ;
    my $user_info = encode_json \%user_temp;
    insert_redis_scalar('L99::A::user::money::top'.$top.'_'.$day_today  , $user_info );
    
    $top++ ;
}
$sth_money -> finish();
#=cut

#=pod
# --------------------------------------------------------------------------------------------------
# 充值(分渠道)
# --------------------------------------------------------------------------------------------------

#=pod
for ( 1 .. $months_ago)	# 往前取几个月
{
	my $month_ago = $_ - 1 ;
	my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	
	my %pay ;			
	my %pay_user ;			
	my %pay_user_day ;
	my %pay_user_channel ;
	my %times_month ;
	
    # 苹果抽成的月统计
	my $apple_take_money_month ;
	foreach( $redis->keys( 'L99::A::payin::recharge::appletake_'.$month.'-*' ) )
	{
	    my $m = $redis->get($_);
	    $apple_take_money_month += $m ;
	}
	insert_redis_scalar('L99::A::payin::recharge::appletake_'.$month  , $apple_take_money_month ) if $apple_take_money_month;
	
    
	foreach($redis->keys( 'L99::payin::recharge::[^u]*'.'_'.$month.'-*' ) )
	{
		my $key = $_ ;		
		my $pay_type ;
		my $id ;
		my $date ;
		my $hour ;
		
		if ($key =~ /^L99::payin::(recharge.*?)::user::(\d+)::uuid::\d+_([-\d]+)_(\d+):/ )
		{
            my $pay = $redis->get($key);
            $times_month{$pay} ++ ;
            
			$pay_type = $1 ;
			$id = $2 ;
			$date = $3 ;
			$hour = $4 ;
			
			$pay{'L99::A::payin::'.$pay_type.'_'.$month } += $pay ;
			$pay{'L99::A::payin::recharge_'.$month } += $pay ;
			
			$pay_user{'L99::A::payin::'.$pay_type.'::user::'.$id.'_'.$date } += $pay ;
			$pay_user{'L99::A::payin::'.$pay_type.'::user::'.$id.'_'.$month} += $pay ;
            
			$pay_user_channel{$pay_type} += $pay ;
			
			$redis -> sadd( 'L99::payin::recharge::uv_'.$month , $id ) ;
		}
	}
	
	# 充值次数 月度统计
	foreach ($redis->keys( 'L99::A::payin::recharge::times::*_'.$month.'-*') )
	{
	    my $key = $_ ;		
		my $n = $redis->get($key);
		next unless $n ;
		if ($key =~ /recharge::(times::.*?)_/) {
		    my $type = $1 ;
		    $pay{'L99::A::payin::recharge::'.$type.'_'.$month } += $n ;
		}
	}
	my $times_info = encode_json \%times_month;
	insert_redis_scalar('L99::A::payin::recharge::times_'.$month  , $times_info ) if $times_info ;
	
	my $channel_info = decode_utf8 encode_json \%pay_user_channel ;
	insert_redis_scalar('L99::A::payin::recharge::channel_' . $month  , $channel_info ) if $channel_info ;

	foreach( keys %pay_user )
	{
		my $key = $_ ;
		my $pay = $pay_user{$key} ;
        my ($pay_type,$accountId,$time) = $key =~ /payin::(.*?)::user::(\d+)_(.+)$/ ;
		my $redis_key = 'L99::A::payin::'.$pay_type.'::user_'.$time ;
        
        $redis->zadd($redis_key , $pay , $accountId) ;
	}
	
	my $num_payin_uv_month = $redis -> scard('L99::payin::recharge::uv_'.$month) ;
	insert_redis_scalar('L99::A::payin::recharge::uv_'.$month , $num_payin_uv_month) if $num_payin_uv_month;
	
	insert_redis(\%pay) ;
}

#=cut

#=pod
# -------------------------------------------------
# 用户消费排行(不含充值，仅统计消费)
# -------------------------------------------------
for ( 1 .. $time_step )
{
    my $days_step = $_ - 1 ;		
    my (%userpay_day,%userinfo) ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    my ($sec,$min,$hour,$day,$mon,$year,$wday,$yday,$isdst) = localtime(time - 86400 * $days_step);    
    $year += 1900;    
    #$mon++;
    my $time_yes_start = timelocal(0,  0,  0 , $day , $mon, $year);
    my $time_yes_end   = timelocal(59, 59, 23, $day , $mon, $year);
    
    my $sth_pay = $dbh_v506 -> prepare("
                                        SELECT * FROM
                                        pay_account_log
                                        where
                                        changeType > 1 and
                                        changeTime between $time_yes_start and $time_yes_end
                                    ");
    $sth_pay -> execute();
    while (my $ref = $sth_pay -> fetchrow_hashref())
    {
        my $accountId = $ref -> {accountId} ;
        my $money     = $ref -> {userMoney} ;
	
	my $ref_account = get_user_from_accountId($dbh_v506,$accountId) ;
	my ($l99NO,$name) = ($ref_account->{l99NO} , $ref_account->{name}) ;
	$userinfo{$accountId}{l99NO} = $l99NO ;
	$userinfo{$accountId}{name}  = decode_utf8 $name  ;
	
        $userpay_day{$accountId} += 0 - $money ;		# 数据库里存的是负数，转一下
    }
    $sth_pay -> finish() ;
    
    my $top = 1 ;
    foreach (sort {$userpay_day{$b} <=> $userpay_day{$a}} keys %userpay_day)	# 从大到小取top20
    {
	last if $top > 20 ;
	my %user_temp ;
	my $accountId = $_ ;
	
	$user_temp{l99NO} = $userinfo{$accountId}{l99NO} ;
	$user_temp{name}  = $userinfo{$accountId}{name} ;
	$user_temp{pay}   = $userpay_day{$accountId} ;
	
	my $user_info = encode_json \%user_temp;
	insert_redis_scalar('L99::A::user::pay::top'.$top.'_'.$key_day  , $user_info );
	
	$top ++ ;
    }
    
}
#=cut


#=pod
# ---------------------------------------------
# Alexa 排行 
# ---------------------------------------------
say "-> Redis.L99::A::user::alexa" ;
my $ref_alexa = get_alexa() ;
my $rank_global = $ref_alexa -> {rank_global} ;
my $rank_china  = $ref_alexa -> {rank_china}  ;

my $rediskey_l99_alexa_global = 'L99::A::user::alexa::world_'.$day_today ;
my $rediskey_l99_alexa_china  = 'L99::A::user::alexa::china_'.$day_today ;
insert_redis_scalar($rediskey_l99_alexa_global,$rank_global) if $rank_global;
insert_redis_scalar($rediskey_l99_alexa_china ,$rank_china) if $rank_china;

#=cut

#=pod
# -------------------------------------------------------------------
# 新增用户 & 登录用户
# -------------------------------------------------------------------
my $day_start = strftime( "%Y-%m-%d 00:00:00",localtime(time() - 86400 * ($time_step - 1)) );
my $day_end   = strftime( "%Y-%m-%d 23:59:59",localtime(time() - 86400 * ($time_step - 1)) );

say "-> Redis.L99::A::user::new_TIME" ;

my %key_user ;
my $sth_sign = $dbh_v506 -> prepare("
                                    SELECT createTime FROM account_log
                                    WHERE
                                    createTime > '$day_start'
                                    ");
$sth_sign -> execute();
while (my $ref = $sth_sign -> fetchrow_hashref())
{
    my $time = $ref -> {createTime} ;
    $time =~ s/ .+// ;
    my $redis_key_sign = "L99::A::user::new_".$time ;
    $key_user{$redis_key_sign} ++ ;
}
$sth_sign -> finish();

say "-> Redis.L99::A::user::login_TIME" ;
my $sth_login = $dbh_v506 -> prepare(" SELECT lastLogin FROM account_log
					WHERE
					lastLogin between '$day_start' and '$day_end'");
$sth_login -> execute();
while (my $ref = $sth_login -> fetchrow_hashref())
{
    my $time = $ref -> {lastLogin} ;
    $time =~ s/ .+// ;
    my $redis_key_login = "L99::A::user::login_".$time ;
    $key_user{$redis_key_login} ++ ;
}
$sth_login -> finish();

insert_redis(\%key_user) ;
#=cut


#=pod
# ----------------------------------------------------------------------------
# 发帖量，发帖渠道
# ----------------------------------------------------------------------------
say "-> Redis.L99::A::content::article_TIME " ;
for ( 1 .. $time_step )
{
    my $days_step = $_ - 1 ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;

    my $article_num ;						# 发帖量(不含重飞)
    my %accountIds ;						# 某用户发贴量
    my %clients ;						# 各渠道(app)发帖量
    my $sth_article = $dbh_v506 -> prepare("
					SELECT dashboardId,accountId,sourceClient
					FROM
					dashboard_basic
					WHERE
					dashboardFlag = 1 and blockFlag = 0 and rebolgFlag = 0 and
					dashboardTime between '$key_day 00:00:00' and '$key_day 23:59:59'
				    ");
    $sth_article -> execute();
    while (my $ref = $sth_article -> fetchrow_hashref())
    {
	my $dashboardId  = $ref -> {dashboardId} ;
	my $accountId    = $ref -> {accountId} ;
	my $sourceClient = $ref -> {sourceClient} ;
	
	$article_num ++ ;
	$accountIds{$accountId} ++ ;
	$clients{$sourceClient} ++ if $sourceClient ;
	
    }
    $sth_article -> finish() ;
    
    # 发帖用户数
    my $nums_user = scalar keys %accountIds ;
    insert_redis_scalar('L99::A::user::article_'.$key_day , $nums_user) if $nums_user ;
    
    # 用户发帖数
    insert_redis_scalar('L99::A::content::article_'.$key_day , $article_num) if $article_num;
    
    # 各渠道(app)发帖数
    my $source_info = encode_json \%clients;
    insert_redis_scalar('L99::A::content::article::source_'.$key_day  , $source_info );	
    
    # 按发帖量对用户排名 
    my $top = 1 ;
    foreach (sort {$accountIds{$b} <=> $accountIds{$a}} keys %accountIds)	# 从大到小取top20
    {
	last if $top > 20 ;
	my %user_temp ;
	my $accountId = $_ ;
	
	my $ref_account = get_user_from_accountId($dbh_v506,$accountId) ;
	
	# ----------------------------
	# status
	# 0 - 初始状态，不允许登录
	# 1 - 黑名单
	# 2 - 未激活
	# 3 - 需要改密码
	# 4 - 正常
	# ----------------------------
	my ($l99NO,$name,$status) = ($ref_account->{l99NO} , $ref_account->{name} , $ref_account->{status}) ;
	next unless $status == 4 ;
	
	$user_temp{l99NO} = $l99NO ;
	$user_temp{name}  = decode_utf8 $name ;
	$user_temp{articleNums} = $accountIds{$accountId} ;
	my $user_info = encode_json \%user_temp;
	insert_redis_scalar('L99::A::user::article::top'.$top.'_'.$key_day  , $user_info );
	
	$top ++ ;
    }
}
#=cut

#=pod
my $num_month_ago = $time_step / 30 + 1;
for ( 1 .. $num_month_ago)				# 往前取几个月
{
    my $month_ago = $_ - 1 ;
    my $month      = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
    my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
    my $sqltime_start = $month.'-01 00:00:00' ;
    my $sqltime_end   = $month_next.'-01 00:00:00' ;
    
    # 月 发帖量
    my $article_num_month ;
    foreach($redis->keys( 'L99::A::content::article_'.$month.'-*' ) )
    {
	my $key = $_ ;
	my $value = $redis->get($key);
	$article_num_month += $value ;
    }
    insert_redis_scalar('L99::A::content::article_'.$month , $article_num_month) if $article_num_month ;
    
    # 月 各渠道(app)发帖量
    my %clients_month ;
    foreach($redis -> keys('L99::A::content::article::source_'.$month.'-*') )
    {
	my $key = $_ ;
	my $value = $redis->get($key);
	my $ref_clients = decode_json $value ;
	foreach (keys %$ref_clients){
	    my $client = $_ ;
	    my $num = $$ref_clients{$client} ;
	    $clients_month{$client} += $num ;
	}
    }
    my $source_info_month = encode_json \%clients_month ;
    insert_redis_scalar('L99::A::content::article::source_'.$month  , $source_info_month );	
}
#=cut
    
# ----------------------------------------------------------------------------
# 用户评论量回帖量
# ----------------------------------------------------------------------------
say "-> Redis.L99::A::content::article::reply" ;
for ( 1 .. $time_step)
{
    my $days_step = $_ - 1;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    $redis -> exists('L99::A::content::article::reply_'.$key_day) && next ;
    
    my $num = $collection_comment -> count({source => 'WEB' , createTime => qr/^$key_day/}) ;
    insert_redis_scalar('L99::A::content::article::reply_'.$key_day  , $num );	
}

for ( 1 .. $months_ago)
{
    my $month_ago = $_ - 1 ;
    my $month_last = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago + 1) )) ;
    my $month_now  = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
    my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
    
    my $num_month ;
    foreach($redis -> keys('L99::A::content::article::reply_'.$month_now.'-*') )
    {
	my $key = $_ ;
	my $value = $redis->get($key);
	$num_month += $value ;
    }
    insert_redis_scalar('L99::A::content::article::reply_'.$month_now  , $num_month );	
    
}
#=cut

$lockfile->remove;

# ====================================  function  ======================================

sub insert_redis
{
    my ($ref) = @_ ;
    foreach (keys %$ref){
        my $key = $_ ;
        my $value = $ref->{$key} ;
	next unless $value ;
        say "$key => $value" ;
        $redis->set($key,$value);
    }
}

sub insert_redis_scalar
{
    my ($rediskey,$redisvalue) = @_ ;
    return unless $redisvalue ;
    $redis->set($rediskey,$redisvalue);
    say "$rediskey => $redisvalue" ;
}


# -----------------------------------------------------------------------------------------
# get user_info.[userl99No,username,status] from accountId -- TABLE:account
# -----------------------------------------------------------------------------------------
sub get_user_from_accountId
{
	my ($dbh,$accountId) = @_ ;
	my $ref_accountId ;
	my $sth_account = $dbh -> prepare(" SELECT accountId,l99NO,name,status FROM account WHERE accountId = $accountId ") ;
	$sth_account -> execute();
	while (my $ref_account = $sth_account -> fetchrow_hashref())
	{
		my $accountId = $ref_account -> {accountId};
		my $l99NO     = $ref_account -> {l99NO};
		my $name      = $ref_account -> {name};
		my $status    = $ref_account -> {status} ;
		$ref_accountId -> {l99NO}  = $l99NO ;
		$ref_accountId -> {name}   = $name ;
		$ref_accountId -> {status} = $status ;
	}
	$sth_account -> finish ;
	return $ref_accountId ;
}


sub get_alexa
{
    my $ref ;
    my $html = get("http://www.alexa.com/siteinfo/www.l99.com");
    my ($rank_global) = $html =~ /alt='Global rank icon'[\s\S]*?>([,0-9]+)/ ;
    my ($rank_china)  = $html =~ /alt='China Flag'[\s\S]*?>([,0-9]+)</ ;
    s/,//g for ($rank_global,$rank_china) ;
    $ref->{rank_global} = $rank_global ;
    $ref->{rank_china}  = $rank_china ;
    $ref ;
}


sub unix2local
{
    my ($time) = @_ ;
    my ($sec,$min,$hour,$day,$mon,$year,$wday,$yday,$isdst) = localtime $time ;
    $year += 1900;    
    $mon ++;
    foreach($mon,$day,$hour,$min,$sec){ $_ = sprintf("%02d", $_); }
    my $local = "$year-$mon-$day $hour:$min:$sec" ;
    return $local ;
}

sub local2unix
{
	my ($time) = @_ ;
	my ($yyear,$m,$dday,$h,$mi,$s) = $time =~ /^(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)$/ ;
	my $mmon = $m - 1 ;
	my $timestamp = timelocal($s,  $mi,  $h , $dday , $mmon, $yyear);
	#my $timestamp = timegm($s,  $mi,  $h , $dday , $mmon, $yyear);		# timelocal + 3600 * 8
	return $timestamp ;
}
