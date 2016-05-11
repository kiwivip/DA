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

my $lockfile = File::Lockfile->new('recharge.lock' , '/home/DA/DataAnalysis');
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

my $L06_host     = $Config -> {L06_DB} -> {host};
my $L06_db       = $Config -> {L06_DB} -> {database} ;
my $L06_usr      = $Config -> {L06_DB} -> {username};
my $L06_password = $Config -> {L06_DB} -> {password};

my $CS_host      = $Config -> {CS_DB}  -> {host};
my $CS_user_host = $Config -> {CS_DB}  -> {host_user};
my $CS_db        = $Config -> {CS_DB}  -> {database};
my $CS_usr       = $Config -> {CS_DB}  -> {username};
my $CS_password  = $Config -> {CS_DB}  -> {password};
my $CS_user_dbs  = $Config -> {CS_DB}  -> {database_account} ;
my $CS_db_comment= $Config -> {CS_DB}  -> {database_comment} ;
my $CS_log_dir   = $Config -> {CS_LOG} -> {dir} ;
my $article_topN = $Config -> {CS_LOG} -> {topN} ;

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

my $dsn_v506_user = "DBI:mysql:database=$L06_db;host=$CS_user_host" ;
my $dbh_v506_user = DBI -> connect($dsn_v506_user , $L06_usr, $L06_password, {'RaiseError' => 1} ) ;
$dbh_v506_user -> do ("SET NAMES UTF8");

# ---------------------------------------------------------------------
# connect to Redis & mongoDB
# ---------------------------------------------------------------------
my $redis = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
my $redis_db2 = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
$redis_db2 -> select(2) ;
my $redis_market = Redis->new(server => "$redis_host_market:$redis_port",reconnect => 10, every => 2000);
$redis_market -> select(6) ;
my $redis_active = Redis->new(server => "192.168.201.57:6379",reconnect => 10, every => 2000);

# --------------------------------------------------------------------------------------------------
# 充值(分渠道)
# --------------------------------------------------------------------------------------------------
say "-> Redis.L99::payin::recharge " ;

for ( 1 .. $time_step + 1 )
{
    my $days_step = $_ - 1 ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
	say $key_day ;
	my $timestamp_l = local2unix($key_day.' 00:00:00')  ;
	my $timestamp_r = local2unix($key_day.' 23:59:59')  ;

    my $sth_pay = $dbh_v506 -> prepare("
                                        SELECT logId,accountId,userMoney,changeType,changeDesc,changeTime
                                        FROM
                                        pay_account_log
                                        WHERE
                                        changeType = 1 and changeTime between $timestamp_l and $timestamp_r
                                        ") ;
    $sth_pay -> execute();
    while (my $ref = $sth_pay -> fetchrow_hashref())
    {
    	my $logId     = $ref -> {logId} ;
    	my $accountId = $ref -> {accountId} ;
    	my $userMoney = $ref -> {userMoney} ;
    	my $changeType = $ref -> {changeType} ;
    	my $changeDesc = decode_utf8 $ref -> {changeDesc} ;
    	my $changeTime = $ref -> {changeTime} ;
    	my $time = unix2local($changeTime) ;
    	$time =~ s/ /_/ ;
    	my $type_recharge ;
    	
    	if ($changeDesc =~ /类型：(.+) 订单号：(.+)\)/) {
    	    $type_recharge = encode_utf8 $1 ;
    	}
    	elsif($changeDesc =~ /苹果账户支付/){
    	    $type_recharge = 'apple' ;
    	}
    	elsif($changeDesc =~ /微信支付/){
    	    $type_recharge = 'weixin' ;
    	}
    	else{
    	    $type_recharge = '#' ;
    	}
    	if ($type_recharge eq 'apple' && $userMoney =~ /\.[48]/)
        {
            $userMoney *= 1.25 ;
        }
        if ($type_recharge eq 'apple' && $userMoney =~ /\.[26]/)
        {
            $userMoney /= 0.7 ;
        }
        
    	my $redis_key = 'L99::payin::recharge::'. $type_recharge .'::user::'.$accountId . '::uuid::' . $logId . '_' .$time ;
    	$redis -> exists( $redis_key ) && next ;
    	insert_redis_scalar( $redis_key => $userMoney ) ;
    	
    }   
    $sth_pay -> finish ;
}
#=cut

#=pod
for ( 1 .. $time_step + 1)
{
	my $days_step = $_ - 1 ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    my ($month) = $key_day =~ /^(\d+-\d+)-\d+$/ ;
	my %times ;
	my %pay ;
    my %pay_user_channel ;
    my %pay_user_market  ;
    my %pay_user_channel_market ;
	
	foreach($redis->keys( 'L99::payin::recharge*_'.$key_day.'_*' ) )
	{
		my $key = $_ ;
		if ($key =~ /^L99::payin::(recharge.*?)::user::(\d+)::uuid::\d+_[-\d]+_(\d+):/ )
		{
            my $pay = $redis->get($key);
			my $pay_type = $1 ;
			my $id = $2 ;
			my $hour = $3 ;
            
            # 各种金额的充值次数
            $times{$pay} ++ ;
            
            # 获取用户渠道
            #my $ref_account_l99NO = get_user_from_accountId($dbh_v506,$id) ;
			#my $l99NO = $ref_account_l99NO->{l99NO} ;
            my $ref_temp = get_user_market2_from_accountId($dbh_v506,$id) ;
            my $market = $ref_temp -> {market} ;
			#my $market = $redis_market->get('STORM::CS::user::market::'.$l99NO) ;
            
            $pay{ 'L99::A::payin::'.$pay_type.'_'.$key_day  } += $pay ;
			$pay{ 'L99::A::payin::recharge_'.$key_day } += $pay ;
			$pay{ 'L99::A::payin::recharge::appletake_'.$key_day } += $pay * 0.3 if $pay_type =~ /recharge::apple/ ;
			$pay{ 'L99::A::payin::recharge::times::hour'.$hour.'_'.$key_day } ++ ;
			$pay{ 'L99::A::payin::recharge::times::market::'.$market.'_'.$key_day } ++ if $market;
			
            $pay_user_channel{$pay_type} += $pay ;
            $pay_user_market{$market}    += $pay ;
            $pay_user_channel_market{$pay_type.'::'.$market} += $pay ;
            
            $redis -> sadd( 'L99::payin::recharge::uv_'.$key_day , $id ) ;
            $redis -> sadd( 'L99::payin::recharge::uv_'.$month   , $id ) ;
		}
		
	}
	
	insert_redis(\%pay) ;
	
    my $channel_info = decode_utf8 encode_json \%pay_user_channel;
    insert_redis_scalar('L99::A::payin::recharge::channel_'.$key_day , $channel_info );
    
    my $market_info = encode_json \%pay_user_market ;
    insert_redis_scalar('L99::A::payin::recharge::market_'.$key_day , $market_info );
    
    my $channel_market_info = decode_utf8 encode_json \%pay_user_channel_market ;
    insert_redis_scalar('L99::A::payin::recharge::channel::market_'.$key_day , $channel_market_info );
    
	my $times_info = encode_json \%times;
	insert_redis_scalar('L99::A::payin::recharge::times_'.$key_day  , $times_info );
    
    my $uv_day = $redis -> scard('L99::payin::recharge::uv_'.$key_day) ;
    insert_redis_scalar('L99::A::payin::recharge::uv_'.$key_day  , $uv_day ) if $uv_day;
}
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
	
	insert_redis_hash(\%pay) ;
}


#=pod
# ---------------------------------------------------------------------------------------------------
# 用户消费统计  CS::A::payin
# ---------------------------------------------------------------------------------------------------
say "-> Redis.CS::A::payin*_DAY" ;
for ( 1 .. $time_step + 1 )
{
	my $days_step = $_ - 1 ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
	my ($month) = $key_day =~ /^(\d+-\d+)-\d+$/ ;
	
	my %pay ;			# 每天的消费3
	my %pay_user ;
	
	my $ref_user_version = get_user_version($redis_active,$key_day) ;
	say "\t Get all user-version on $key_day ... " ;
	
	foreach($redis_db2 -> keys( 'CS::payin*::user::*_'.$key_day.'*' ) )
	{
		my $key = $_ ;		
		my $pay ;
		
		my $pay_type ;
		my $id ;
		my $hour ;
		
		if ($key =~ /^CS::payin::(.+)::user::(\d+)::uuid::\d+_[-\d]+_(\d+):/ )
		{
			$pay = $redis_db2 -> get($key);
			my $pay_type_s = $1 ;
			$id = $2 ;
			$hour = $3 ;
			if ($pay_type_s =~ /point::item::(\d+)/)
			{
                my $itemId = $1 ;
				if ( $itemId == 18 ){
					$pay_type = 'point::chuangdian' ;
				}elsif( $itemId == 19 ){
					$pay_type = 'point::chuangbi' ;
				}else{
					$pay_type = 'point::shiwu' ;
				}
            }
			else{
				$pay_type = $pay_type_s ;
			}
            
		}
		else
		{
			next ;
		}
		
		# 获取用户的渠道
		my $ref_account_l99NO = get_user_from_accountId($dbh_v506_user,$id) ;
		my $l99NO = $ref_account_l99NO->{l99NO} ;
		#my $market = $redis_market->get('STORM::CS::user::market::'.$l99NO) ;	
		my $ref_temp = get_user_market2_from_accountId($dbh_v506_user,$id) ;
		my $market = $ref_temp -> {market} ;
		
		my $version = $ref_user_version -> {$l99NO} ;
		
		# 获取用户的系统
		my $ref_account = get_user_market_from_accountId($dbh_v506_user,$id) ;
        my $os = $ref_account -> {market} ;
		
		$pay{'CS::A::payin::'.$pay_type.'_'.$key_day  } += $pay ;
		$pay{'CS::A::payin::'.$pay_type.'::hour'.$hour.'_'.$key_day  } += $pay ;
		$pay{'CS::A::payin::'.$pay_type.'::'.$os.'_'.$key_day  } += $pay if $os ;
		$pay{'CS::A::payin::'.$pay_type.'::market::' . $market .'_'.$key_day  } += $pay if $market ;
		$pay{'CS::A::payin::'.$pay_type.'::version::'.$version .'_'.$key_day  } += $pay if $version ;
		$pay{'CS::A::payin::'.$pay_type.'::market::'.$market.'::version::'.$version.'_'.$key_day  } += $pay if $market && $version ;
		
		
		if ($pay_type !~ /point::/)
		{
			if ($pay_type =~ /mora/)
			{
                $pay{'CS::A::payin::'.$pay_type.'::times_'.$key_day } ++ ;
				$redis_db2 -> sadd( 'CS::payin::'.$pay_type.'::uv_'.$key_day , $id ) ;
				$redis_db2 -> sadd( 'CS::payin::'.$pay_type.'::uv_'.$month   , $id ) ;
            }
            
			if ($pay_type =~ /packs/)
			{
				$pay{'CS::A::payin::'.$pay_type.'::times_'.$key_day } ++ ;
				$redis_db2 -> sadd( 'CS::payin::'.$pay_type.'::uv_'.$key_day , $id ) ;
				$redis_db2 -> sadd( 'CS::payin::'.$pay_type.'::uv_'.$month   , $id ) ;
			}
			
			if ($pay_type =~ /gamelife/)
			{
				$pay{'CS::A::payin::'.$pay_type.'::times_'.$key_day } ++ ;
				$redis_db2 -> sadd( 'CS::payin::'.$pay_type.'::uv_'.$key_day , $id ) ;
				$redis_db2 -> sadd( 'CS::payin::'.$pay_type.'::uv_'.$month   , $id ) ;
			}
			
			$redis_db2 -> sadd( 'CS::payin::uv::hour'.$hour.'_'.$key_day  , $id ) ;
			$redis_db2 -> sadd( 'CS::payin::uv::hour'.$hour.'_'.$month    , $id ) ;
			
			$redis_db2 -> sadd( 'CS::payin::uv_'.$key_day  , $id ) ;
			$redis_db2 -> sadd( 'CS::payin::uv_'.$month    , $id ) ;
			$redis_db2 -> sadd( 'CS::payin::uv' , $id ) ;
			
			$redis_db2 -> sadd( 'CS::payin::uv::'.$os.'_'.$key_day  , $id ) if $os ;
			$redis_db2 -> sadd( 'CS::payin::uv::'.$os.'_'.$month    , $id ) if $os ;
			$redis_db2 -> sadd( 'CS::payin::uv::market::'.$market.'_'.$key_day    , $id ) if $market ;
			$redis_db2 -> sadd( 'CS::payin::uv::market::'.$market.'_'.$month      , $id ) if $market ;
			$redis_db2 -> sadd( 'CS::payin::uv::version::'.$version.'_'.$key_day  , $id ) if $version ;
			$redis_db2 -> sadd( 'CS::payin::uv::version::'.$version.'_'.$month    , $id ) if $version ;
			$redis_db2 -> sadd( 'CS::payin::uv::market::'.$market.'::version::'.$version.'_'.$key_day , $id ) if $market && $version ;
			$redis_db2 -> sadd( 'CS::payin::uv::market::'.$market.'::version::'.$version.'_'.$month   , $id ) if $market && $version ;
			
			$redis_db2 -> sadd( 'CS::payin::uv::'.$os , $id ) if $os ;
			$redis_db2 -> sadd( 'CS::payin::uv::market::'.$market , $id ) if $market ;
		}
		elsif($pay_type =~ /^point::/)
		{
			$pay{'CS::A::payin::point::times_'.$key_day } ++ ;
			$pay{'CS::A::payin::'.$pay_type.'::times_'.$key_day } ++ ;
			
			$redis_db2 -> sadd( 'CS::payin::'.$pay_type.'::uv_'.$key_day , $id ) ;
			$redis_db2 -> sadd( 'CS::payin::'.$pay_type.'::uv_'.$month   , $id ) ;
			
			$redis_db2 -> sadd( 'CS::payin::point::uv_'.$key_day , $id ) ;
			$redis_db2 -> sadd( 'CS::payin::point::uv_'.$month   , $id ) ;
			$redis_db2 -> sadd( 'CS::payin::point::uv' , $id ) ;
		}
		elsif($pay_type =~ /bedpoint::mora/)
		{
			$pay{'CS::A::payin::'.$pay_type.'::times_'.$key_day } ++ ;
			$redis_db2 -> sadd( 'CS::payin::'.$pay_type.'::uv_'.$key_day , $id ) ;
			$redis_db2 -> sadd( 'CS::payin::'.$pay_type.'::uv_'.$month   , $id ) ;
		}
		elsif($pay_type =~ /bedpoint::shicaigame/)
		{
			$pay{'CS::A::payin::'.$pay_type.'::times_'.$key_day } ++ if $pay < 0 ;
			$pay{'CS::A::payin::'.$pay_type.'::in_' .$key_day } += $pay if $pay < 0;
			$pay{'CS::A::payin::'.$pay_type.'::out_'.$key_day } += $pay if $pay > 0;
			$redis_db2 -> sadd( 'CS::payin::'.$pay_type.'::uv_'.$key_day , $id ) if $pay < 0 ;
			$redis_db2 -> sadd( 'CS::payin::'.$pay_type.'::uv_'.$month   , $id ) if $pay < 0 ;
		}
	
		$pay_user{'CS::A::payin::'.$pay_type.'::user::'.$id.'_'.$key_day}  += $pay ;
		
	}
	
    # 各类消费的详细用户清单
	foreach( keys %pay_user )
	{
		my $key = $_ ;		
		my $pay = $pay_user{$key} ;
		my ($pay_type,$accountId) = $key =~ /payin::(.*?)::user::(\d+)_/ ;
		my $redis_key = 'CS::A::payin::'.$pay_type.'::user_'.$key_day ;
		$redis->zadd($redis_key  , $pay , $accountId) ;
		
	}

	insert_redis_hash(\%pay) ;
	
	my $mora_take = 0.01 * $redis->get('CS::A::payin::mora::join_'.$key_day) ;
	insert_redis_scalar( 'CS::A::payin::mora::take_'.$key_day , $mora_take) ;
	
	my $mora_bedpoint_take = 0.01 * $redis->get('CS::A::payin::bedpoint::mora::join_'.$key_day) ;
	insert_redis_scalar( 'CS::A::payin::bedpoint::mora::take_'.$key_day , $mora_bedpoint_take) ;
	
	foreach( $redis_db2 -> keys( 'CS::payin*:uv*_'.$key_day ) )
	{
		my $k = $_ ;
		if ($k =~ /^CS::(payin.*?uv.*?)_/ )
		{
			my $type = $1 ;
			my $count = $redis_db2 -> scard($k) ;
			insert_redis_scalar( 'CS::A::'.$type.'_'.$key_day , $count ) if $count ;
		}
	}

}


=pod
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
			$redis -> sadd( 'L99::payin::recharge::uv'         , $id ) ;
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
		#my $ref_account = get_user_from_accountId($dbh_v506,$accountId) ;
		#my $l99NO = $ref_account->{l99NO} ;
		#my $name  = $ref_account->{name} ;
		#my $info = $pay.','.$l99NO.','.$name ;
		#insert_redis_scalar($key , $info) if $info;
	}
	
	my $num_payin_uv_month = $redis -> scard('L99::payin::recharge::uv_'.$month) ;
	insert_redis_scalar('L99::A::payin::recharge::uv_'.$month , $num_payin_uv_month) if $num_payin_uv_month;
	
	insert_redis(\%pay) ;
}

my $num_payin_uv = $redis -> scard('L99::payin::recharge::uv') ;
insert_redis_scalar('L99::A::payin::recharge::uv' , $num_payin_uv) ;

=cut

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

sub insert_redis_hash
{
    my ($ref) = @_ ;
    foreach (keys %$ref)
    {
        my $key = $_ ;
        my $value = $ref->{$key} ;
        $redis->set($key,$value);
		#say "$key => $value" ;
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

# --------------------------------------------------------------------------------
# 根据用户 accountId 获取用户系统
# --------------------------------------------------------------------------------
sub get_user_market_from_accountId
{
	my ($dbh,$accountId) = @_ ;
	my $ref_accountId ;
	my $os ;
	my $sth_account = $dbh -> prepare(" SELECT accountId,market FROM account_log WHERE accountId = $accountId ") ;
	$sth_account -> execute();
	while (my $ref_account = $sth_account -> fetchrow_hashref())
	{
		my $accountId = $ref_account -> {accountId};
		my $market    = $ref_account -> {market} ;
		
		if ($market =~ /iPhone/i){
			$os = 'ios'  ;
		}else{
			$os = 'android' ;
		}
		$ref_accountId -> {market}  = $os ;
	}
	$sth_account -> finish ;
	return $ref_accountId ;
}

sub get_user_market2_from_accountId
{
	my ($dbh,$accountId) = @_ ;
	my $ref_accountId ;
	my $os ;
	my $sth_account = $dbh -> prepare(" SELECT accountId,market FROM account_log WHERE accountId = $accountId ") ;
	$sth_account -> execute();
	while (my $ref_account = $sth_account -> fetchrow_hashref())
	{
		my $accountId = $ref_account -> {accountId};
		my $market    = $ref_account -> {market} ;
		
		if ($market =~ /iPhone/i){
			$os = 'AppStore'  ;
		}else{
			($os) = $market =~ /chuangshang_(.+)/ ;
		}
		$ref_accountId -> {market}  = $os ;
	}
	$sth_account -> finish ;
	return $ref_accountId ;
}

sub get_user_version
{
	my ($redis_p,$time) = @_ ;
	my $ref_version ;
	foreach( $redis_p->keys( 'STORM::CS::user::version*_'.$time ) )
	{
		my $key = $_ ;
		my ($version) = $key =~ /version::(.*?)_/ ;
		my @temp = $redis_p->smembers($key) ;
		foreach(@temp){
			my $l99NO = $_ ;
			$ref_version -> {$l99NO} = $version ;
		}
	}
	return $ref_version ;
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
