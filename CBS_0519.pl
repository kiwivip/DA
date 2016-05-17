#!/usr/bin/env perl 
# ==============================================================================
# function:	    立方体育－大赢家项目数据统计
# Author: 	    kiwi
# createTime:	2014.6.18
# ==============================================================================
use 5.10.1 ;
use utf8 ;
use Redis;
use Config::Tiny ;
use DBI ;
use JSON::XS ;
use Time::Local ;
use File::Lockfile ; 
use POSIX qw(strftime);
use Date::Calc::XS qw (Date_to_Time Time_to_Date);
use Unicode::UTF8 qw (decode_utf8 encode_utf8);
binmode(STDIN, ':encoding(utf8)');
binmode(STDOUT, ':encoding(utf8)');
binmode(STDERR, ':encoding(utf8)');

$| = 1;
# ---------------------------------------------------------
# 文件锁实现确保计划任务下的进程单实例，注意main段最后需要remove
# ---------------------------------------------------------
my $lockfile = File::Lockfile->new('cbs.lock' , '/home/DA/DataAnalysis');
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

#my $time_step = 50 ;
my $time_step = $Config -> {time} -> {step} ;		# 往前回滚N天，默认 N = 1 

my $L06_host      = $Config -> {L06_DB} -> {host};
my $L06_db        = $Config -> {L06_DB} -> {database} ;
my $L06_usr       = $Config -> {L06_DB} -> {username};
my $L06_password  = $Config -> {L06_DB} -> {password};

my $CBS_host      = $Config -> {CBS_DB}  -> {host};
my $CBS_db_user   = $Config -> {CBS_DB}  -> {database_user};
my $CBS_db_match  = $Config -> {CBS_DB}  -> {database_match};
my $CBS_db_pay    = $Config -> {CBS_DB}  -> {database_pay};
my $CBS_usr       = $Config -> {CBS_DB}  -> {username};
my $CBS_password  = $Config -> {CBS_DB}  -> {password};
my $CBS_usr1      = $Config -> {CBS_DB}  -> {username1};
my $CBS_password1 = $Config -> {CBS_DB}  -> {password1};
my $CBS_port1     = $Config -> {CBS_DB}  -> {port1};

my $redis_host = $Config -> {REDIS} -> {host};
my $redis_port = $Config -> {REDIS} -> {port};
my $redis_host_market = $Config -> {REDIS} -> {host_market};

my $file_robot = 'robot_cbs.csv' ;          # 由产品相关人员提供一份机器人名单

my $num_month_ago = $time_step / 30 + 1;
my $timestamp_start = int scalar time  - 86400 * $time_step  ;			
#my $day_start = strftime("%Y-%m-%d 00:00:00",localtime(time() - 86400 * $time_step)); 		# %Y-%m-%d %H:%M:%S

# ---------------------------------------------------------------------
# connect to redis
# ---------------------------------------------------------------------
my $redis = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
my $redis_db2 = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
$redis_db2 -> select(2) ;
my $redis_storm = Redis->new(server => "$redis_host_market:$redis_port",reconnect => 10, every => 2000);
$redis_storm -> select(12) ;

# ---------------------------------------------------------------------
# connect to mysql
# $dsn = "DBI:mysql:database=$database;host=$hostname;port=$port";
# ---------------------------------------------------------------------
my $dsn_v506 = "DBI:mysql:database=$L06_db;host=$L06_host" ;
my $dbh_v506 = DBI -> connect($dsn_v506, $L06_usr, $L06_password, {'RaiseError' => 1} ) ;
$dbh_v506 -> do ("SET NAMES UTF8");

my $dsn_new  = "DBI:mysql:database=$CBS_db_user;host=$CBS_host;port=$CBS_port1" ;
my $dbh_new = DBI -> connect($dsn_new, $CBS_usr1, $CBS_password1, {'RaiseError' => 1} ) ;
$dbh_new -> do ("SET NAMES UTF8");

# --------------------------
# 记录大赢家的机器人userId列表
# --------------------------
my %robots ;
open my $fh_robot, '<:utf8' , $file_robot ;
while (<$fh_robot>) { chomp ; $robots{$_} = 1 ; }

#=pod

# --------------------------------------------------
# 记录猜比赛付费的用户
# 按账户余额排序
# --------------------------------------------------
my %user_pay_cbs ;         
my $sth_user_temp = $dbh_v506 -> prepare("
		SELECT distinct(accountId) FROM pay_account_log
		WHERE
		changeType in (31,32,33,34,38)
") ;
$sth_user_temp -> execute();
while (my $ref = $sth_user_temp -> fetchrow_hashref())
{
		my $accountId = $ref -> {accountId} ;
		$user_pay_cbs{$accountId} = 1 ;
}
$sth_user_temp -> finish ;
my $payids = join ',' , keys %user_pay_cbs ;
my $today = strftime("%Y-%m-%d",localtime(time())); 
my $sth_money = $dbh_v506 -> prepare("
        SELECT accountId,userMoney,frozenMoney
        FROM pay_account WHERE
        accountId in ($payids) 
");
# 上面采用 id in (ids) 这种查询方式是为了快
# 目前ids数量少，以后如果充值用户达到‘万’甚至更高的数量级时，需要改变查询逻辑
$sth_money -> execute();
while (my $ref = $sth_money -> fetchrow_hashref())
{
    my $accountId = $ref -> {accountId} ;
    next unless exists $user_pay_cbs{$accountId} ;
    my $money     = $ref -> {userMoney} ;
    my $frozen    = $ref -> {frozenMoney} ;
    my $balance   = $money - $frozen ;
    $redis->zadd('CBS::A::user::money::top_'.$today , $balance , $accountId) ;	
}
$sth_money -> finish();

#=pod
# ------------------------------------------------------------
# 新增用户
# ------------------------------------------------------------
say "-> Redis.CBS::A::user::new*_TIME " ;
for ( 1 .. $time_step + 1)
{
	my $days_step = $_ - 1 ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
	
	my $ref_user_version = get_user_version($redis_storm , $key_day) ;
	my $ref_user_market  = get_user_market( $redis_storm , $key_day) ;
	
	my %temp ;
	my $sth_user = $dbh_new -> prepare("
                                        SELECT userId,userNO,userName,gender,status,createTime
										FROM
										cbs_user
										WHERE
										createTime between '$key_day 00:00:00' and '$key_day 23:59:59'
									");
	$sth_user -> execute();
	while (my $ref = $sth_user -> fetchrow_hashref())
	{
		my $gender = 0 ;
		$gender = $ref -> {gender} ;
		my $accountId  = $ref -> {userId} ;
		my $l99NO      = $ref -> {userNO} ;
		my $userName   = $ref -> {userName} ;
		my $status     = $ref -> {status} ;
		my $time       = $ref -> {createTime} ;
		my ($hour) = $time =~ / (\d+):\d+:\d+$/ ;
		
		my $version = $ref_user_version -> {$l99NO} ;
		my $market  = $ref_user_market  -> {$l99NO} ;
		#say "$market \t $version \t $gender" ;
		
		$temp{'CBS::A::user::new::hour'.$hour.'_'.$key_day} ++ ;
		$temp{'CBS::A::user::new::gender::'.$gender.'_'.$key_day} ++ ;
		$temp{'CBS::A::user::new::hour'.$hour.'::gender::'.$gender.'_'.$key_day} ++ ;
		$temp{'CBS::A::user::new::market::'.$market.'_'.$key_day} ++ if $market ;
		$temp{'CBS::A::user::new::market::'.$market.'::version::'.$version.'_'.$key_day} ++ if $market && $version ;
        $temp{'CBS::A::user::new::version::'.$version.'_'.$key_day} ++ if $version ;
		
		$redis_db2 -> sadd( 'CBS::user::new_'.$key_day , $l99NO ) ;
	}
	
	$sth_user -> finish();
	
	insert_redis_hash(\%temp) ;
	
	my $count = $redis_db2 -> scard('CBS::user::new_'.$key_day) ;
	insert_redis_scalar('CBS::A::user::new_'.$key_day , $count ) if $count ;
	
}
#=cut

#=pod
# 新增用户按月的统计
say "-> Redis.CBS::A::user::new*_MONTH" ;

for ( 1 .. $num_month_ago )	
{
	my $month_ago = $_ - 1 ;
	my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	say "\t $month" ;
	
	my %user_new_month ;
	foreach( $redis->keys( 'CBS::A::user::new*'.'_'.$month.'-*' ) )
	{
		my $key = $_ ;		
		my $type ;
		if ($key =~ /^CBS::A::user::new(.*?)_[\d\-]+$/ )
		{
			$type = $1 ;
			my $num = $redis->get($key);
			$user_new_month{'CBS::A::user::new'.$type.'_'.$month  } += $num ;
		}
	}

	insert_redis_hash(\%user_new_month) ;
}
#=cut

#=pod
# ---------------------------------------------
# 充值
# ---------------------------------------------
say "-> Redis.CBS::payin::rechage*_TIME " ;

for ( 1 .. $time_step + 1 )
{
		my $days_step = $_ - 1 ;
		my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
		say "\t $key_day" ;             # 打印当前正在统计的日期
		
		my $timestamp_l = local2unix($key_day.' 00:00:00')  ;
		my $timestamp_r = local2unix($key_day.' 23:59:59')  ;
		
        my %channel ;                   # 存储各渠道的充值金额
        my $type_recharge ;
        
		my $sth_recharge = $dbh_v506 -> prepare("
				SELECT logId,accountId,userMoney,changeType,changeDesc,changeTime
                FROM
				pay_account_log
				WHERE
				changeType in (31,32,33,34,38)
				and
				changeTime between $timestamp_l and $timestamp_r
		") ;
		$sth_recharge -> execute();
		while (my $ref = $sth_recharge -> fetchrow_hashref())
		{
				my $logId     = $ref -> {logId} ;
				my $accountId = $ref -> {accountId} ;
				my $userMoney = $ref -> {userMoney} ;
				my $changeType = $ref -> {changeType} ;
				my $changeDesc = decode_utf8 $ref -> {changeDesc} ;
				my $changeTime = $ref -> {changeTime} ;
				my $time = unix2local($changeTime) ;
				$time =~ s/ /_/ ;
				
				my $pay_type ;
				if   ($changeType == 31 )									
				{
					$pay_type = 'recharge'  if $changeDesc =~ /充值/ ;
                    
                    # 各充值支付渠道
                    if ($changeDesc =~ /微信支付/) {
                            $type_recharge = '微信支付' ;
                    }
                    elsif($changeDesc =~ /支付宝/){
                            $type_recharge = '支付宝' ;
                    }
					elsif($changeDesc =~ /银联移动支付/){
                            $type_recharge = '银联移动支付' ;
                    }
                    elsif($changeDesc =~ /#thing/){
                            $type_recharge = '#' ;
                    }
                    
                    $channel{'recharge::'.$type_recharge} += $userMoney ;
				}
				elsif($changeType == 32 )
				{
					$pay_type = 'recharge::admin' if $changeDesc =~ /后台充值/ ;
				}
				elsif($changeType == 33 )
				{
					$pay_type = 'bet' if $changeDesc =~ /下注/ ;
					$pay_type = 'top' if $changeDesc =~ /发表头版广告区/ ;
					$pay_type = 'buygoods' if $changeDesc =~ /购买商品/ ;
				}
				elsif($changeType == 34 )
				{
					$pay_type = 'takeout::admin' if $changeDesc =~ /后台扣除/ ;
				}
				elsif($changeType == 38 )
				{
					$pay_type = 'bet::win' if $changeDesc =~ /下注结算/ ;
					$pay_type = 'exchange' if $changeDesc =~ /龙筹兑换/ ;
				}	
				     
				next unless $pay_type ;
				my $redis_key = 'CBS::payin::'.$pay_type.'::user::'.$accountId . '::uuid::' . $logId . '_' .$time ;
				
				# mongoDB & Redis 双写，redis会定期清理key，后面的payin类型同逻辑
				#unless ($collection_payin->find_one({ "rediskey" => $redis_key }))
				#{
				#		$collection_payin -> insert( {"rediskey" => $redis_key , "redisvalue" => $userMoney} );
				#		say "MongoDB: $redis_key => $userMoney" ;
				#}
				
				$redis_db2 -> exists( $redis_key ) && next ;
				$redis_db2 -> set($redis_key , $userMoney);
				say "Redis_db2: $redis_key => $userMoney" ;
				
				my $expire_time = 60 * 24 * 3600 ;			# 60天后del
				$redis_db2 -> expire($redis_key ,  $expire_time) ;

		}
		$sth_recharge -> finish ;
		
        # 充值的支付渠道各金额
        my $channel_info = encode_json \%channel;
        insert_redis_scalar('CBS::A::payin::recharge::channel_'.$key_day , $channel_info );
        
}
#=cut

#=pod
#  goods表的映射
say "-> Redis.CBS::payin::mall::N_TIME" ;
for ( 1 .. $time_step + 1)
{
	my $days_step = $_ - 1 ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
	
	my $sth = $dbh_new -> prepare(" SELECT id,name,price FROM cbs_mall_goods ");
	$sth -> execute();
	while (my $ref = $sth -> fetchrow_hashref())
	{
		my %temp ;
		my $id = $ref -> {id} ;
		$temp{price} = $ref -> {price} ;
		$temp{name}  = decode_utf8 $ref -> {name} ;
		my $info = encode_json \%temp ;
		insert_redis_scalar('CBS::payin::mall::'.$id.'_'.$key_day , $info) ;
	}
	$sth -> finish ;
}
#=cut

#=pod
# -------------------------------------------------------------------
# 用户兑换礼品
# status: 0未付款   1未发货（已付款）  2未确认（已发货）  3已完成   10取消
# -------------------------------------------------------------------
say "-> Redis.CBS::payin::mall*_TIME " ;
for ( 1 .. $time_step + 1)
{
	my $days_step = $_ - 1 ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
	
	my %temp ;
	my $sth = $dbh_new -> prepare("
								   SELECT id,userId,goodsId,goodsNum,goodsPrice,amount,status,createTime
								   FROM 
								   cbs_mall_order
								   WHERE
								   createTime between '$key_day 00:00:00' and '$key_day 23:59:59'
								   and status between 1 and 3
								");
	$sth -> execute();
	while (my $ref = $sth -> fetchrow_hashref())
	{
		my $orderId    = $ref -> {id} ;
		my $accountId  = $ref -> {userId} ;
		my $itemId     = $ref -> {goodsId} ;
		my $num 	   = $ref -> {goodsNum} ;
		my $price 	   = $ref -> {goodsPrice} ;
		my $amount 	   = $ref -> {amount} ;
		my $status     = $ref -> {status} ;
		my $time       = $ref -> {createTime} ;
		$time =~ s/ /_/ ;
		
		my $userMoney = $amount ;
		my $redis_key = 'CBS::payin::mall::user::'.$accountId . '::uuid::' . $orderId . '_' .$time ;
		
		$redis_db2 -> sadd( 'CBS::payin::mall::uv_'.$key_day , $accountId ) ;
		$temp{'CBS::A::payin::mall::times_'.$key_day} ++ ;
		$temp{'CBS::A::payin::mall::'.$itemId.'::times_'.$key_day} ++ ;
		$temp{'CBS::A::payin::mall_'.$key_day} += $amount ;
        
		$redis_db2 -> exists( $redis_key ) && next ;
		$redis_db2 -> set($redis_key , $userMoney);
		say "Redis_db2: $redis_key => $userMoney" ;
		
		
	}
	
		$sth -> finish();
	
		insert_redis_hash(\%temp) ;
		my $count = $redis_db2 -> scard('CBS::payin::mall::uv_'.$key_day) ;
		insert_redis_scalar('CBS::A::payin::mall::uv_'.$key_day , $count ) if $count ;
}
#=cut

#=pod
say "-> Redis.CBS::A::payin*_DAY" ;
for ( 1 .. $time_step + 1)
{
		my $days_step = $_ - 1 ;
		my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
		say $key_day ;
		my ($month) = $key_day =~ /^(\d+-\d+)-\d+$/ ;
		
		my $ref_user_version = get_user_version($redis_storm , $key_day) ;
		my $ref_user_market  = get_user_market( $redis_storm , $key_day) ;
				
		my %pay ;			# 每天的消费
		my %pay_user ;
		
        #这里重复了一段代码，先是所有用户的统计，后面是排除机器人的统计
        foreach($redis_db2 -> keys( 'CBS::payin*::user::*_'.$key_day.'*' ) )
		{
				my $key = $_ ;		
				my $pay ;
				
				my $pay_type ;
				my $id ;
				my $hour ;
				
				if ($key =~ /^CBS::payin::(.+)::user::(\d+)::uuid::\d+_[-\d]+_(\d+):/ )
				{
						$pay = $redis_db2 -> get($key);
						$pay_type = $1 ;
						$id = $2 ;
						$hour = $3 ;
                        
				}else{
						next ;
				}
				
				$pay_user{'CBS::A::payin::'.$pay_type.'::user::'.$id.'_'.$key_day}  += $pay ;
                $pay{'CBS::A::payin::recharge::withrobot::uv_'.$key_day} ++ if $pay_type eq 'recharge' ;
                
				my $ref_account = get_user_from_accountId($dbh_new , $id) ;
				my ($l99NO,$gender,$name) = ($ref_account->{l99NO} , $ref_account->{gender} , $ref_account->{name}) ;
				my $version = $ref_user_version -> {$l99NO} ;
				my $market  = $ref_user_market  -> {$l99NO} ;
			
				$pay{'CBS::A::payin::'.$pay_type.'::withrobot_'.$key_day  } += $pay ;
				$pay{'CBS::A::payin::'.$pay_type.'::withrobot::times_'.$key_day  } ++ ;
				$pay{'CBS::A::payin::'.$pay_type.'::withrobot::hour'.$hour.'_'.$key_day  } += $pay ;
				$pay{'CBS::A::payin::'.$pay_type.'::withrobot::gender::'.$gender.'_'.$key_day  } += $pay if $gender;
				$pay{'CBS::A::payin::'.$pay_type.'::withrobot::hour'.$hour.'::gender::'.$gender.'_'.$key_day  } += $pay if $gender;
				$pay{'CBS::A::payin::'.$pay_type.'::withrobot::market::'.$market.'_'.$key_day  } += $pay if $market;
				$pay{'CBS::A::payin::'.$pay_type.'::withrobot::version::'.$version.'_'.$key_day  } += $pay if $version ;
				$pay{'CBS::A::payin::'.$pay_type.'::withrobot::market::'.$market.'::version::'.$version.'_'.$key_day  } += $pay if $market && $version ;
				
				$redis_db2 -> sadd( 'CBS::payin::'.$pay_type.'::withrobot::uv_'.$key_day , $id ) ;
				$redis_db2 -> sadd( 'CBS::payin::'.$pay_type.'::withrobot::uv_'.$month   , $id ) ;
				$redis_db2 -> sadd( 'CBS::payin::hour'.$hour.'::withrobot::uv_'.$key_day  , $id ) ;
				$redis_db2 -> sadd( 'CBS::payin::hour'.$hour.'::withrobot::uv_'.$month    , $id ) ;
				$redis_db2 -> sadd( 'CBS::payin::gender::'.$gender.'::withrobot::uv_'.$key_day  , $id ) ;
				$redis_db2 -> sadd( 'CBS::payin::gender::'.$gender.'::withrobot::uv_'.$month    , $id ) ;
			
				$redis_db2 -> sadd( 'CBS::payin::withrobot::uv_'.$key_day  , $id ) ;
				$redis_db2 -> sadd( 'CBS::payin::withrobot::uv_'.$month    , $id ) ;
		}
        
        # 这里就是排除机器人的另外一套key了
		foreach($redis_db2 -> keys( 'CBS::payin*::user::*_'.$key_day.'*' ) )
		{
				my $key = $_ ;		
				my $pay ;
				
				my $pay_type ;
				my $id ;
				my $hour ;
				
				if ($key =~ /^CBS::payin::(.+)::user::(\d+)::uuid::\d+_[-\d]+_(\d+):/ )
				{
						$pay = $redis_db2 -> get($key);
						$pay_type = $1 ;
						$id = $2 ;
						$hour = $3 ;
                        
                        next if exists $robots{$id} ;       # 忽略机器人
                        
				}else{
						next ;
				}
                
                $pay{'CBS::A::payin::recharge::uv_'.$key_day} ++ if $pay_type eq 'recharge' ;
                
				my $ref_account = get_user_from_accountId($dbh_new , $id) ;
				my ($l99NO,$gender,$name) = ($ref_account->{l99NO} , $ref_account->{gender} , $ref_account->{name}) ;
				my $version = $ref_user_version -> {$l99NO} ;
				my $market  = $ref_user_market  -> {$l99NO} ;
			
				$pay{'CBS::A::payin::'.$pay_type.'_'.$key_day  } += $pay ;
				$pay{'CBS::A::payin::'.$pay_type.'::times_'.$key_day  } ++ ;
				$pay{'CBS::A::payin::'.$pay_type.'::hour'.$hour.'_'.$key_day  } += $pay ;
				$pay{'CBS::A::payin::'.$pay_type.'::gender::'.$gender.'_'.$key_day  } += $pay if $gender;
				$pay{'CBS::A::payin::'.$pay_type.'::hour'.$hour.'::gender::'.$gender.'_'.$key_day  } += $pay if $gender;
				$pay{'CBS::A::payin::'.$pay_type.'::market::'.$market.'_'.$key_day  } += $pay if $market;
				$pay{'CBS::A::payin::'.$pay_type.'::version::'.$version.'_'.$key_day  } += $pay if $version ;
				$pay{'CBS::A::payin::'.$pay_type.'::market::'.$market.'::version::'.$version.'_'.$key_day  } += $pay if $market && $version ;
				
				$redis_db2 -> sadd( 'CBS::payin::'.$pay_type.'::uv_'.$key_day , $id ) ;
				$redis_db2 -> sadd( 'CBS::payin::'.$pay_type.'::uv_'.$month   , $id ) ;
				$redis_db2 -> sadd( 'CBS::payin::hour'.$hour.'::uv_'.$key_day  , $id ) ;
				$redis_db2 -> sadd( 'CBS::payin::hour'.$hour.'::uv_'.$month    , $id ) ;
				$redis_db2 -> sadd( 'CBS::payin::gender::'.$gender.'::uv_'.$key_day  , $id ) ;
				$redis_db2 -> sadd( 'CBS::payin::gender::'.$gender.'::uv_'.$month    , $id ) ;
                
                $redis_db2 -> sadd( 'CBS::payin::uv_'.$key_day  , $id ) ;
				$redis_db2 -> sadd( 'CBS::payin::uv_'.$month    , $id ) ;
		}
		insert_redis_hash(\%pay) ;
		
		# 消费用户列表
		foreach( keys %pay_user )
		{
			my $key = $_ ;
			my $pay = $pay_user{$key} ;
			my ($pay_type,$accountId) = $key =~ /payin::(.*?)::user::(\d+)_/ ;
			$redis->zadd('CBS::A::payin::'.$pay_type.'::user_'.$key_day , $pay , $accountId) ;		
		}
		
		# 消费人数／平均消费金额
		foreach( $redis_db2 -> keys( 'CBS::payin*:uv_'.$key_day ) )
		{
				my $k = $_ ;
				if ($k =~ /^CBS::(payin.*?)::uv_/ )
				{
					my $type = $1 ;
					my $count = $redis_db2 -> scard($k) ;
					my $m = $redis->get('CBS::A::'.$type.'_'.$key_day ) ;
					my $avg = sprintf("%.4f" , $m / $count ) if $count ;
					
					insert_redis_scalar( 'CBS::A::'.$type.'::uv_'.$key_day  , $count ) if $count;
					insert_redis_scalar( 'CBS::A::'.$type.'::avg_'.$key_day , $avg ) if $avg ;
				}
		}
		
}
#=cut

#=pod
say "-> Redis.CBS::A::payin*_MONTH" ;

for ( 1 .. $num_month_ago)
{
	my $month_ago = $_ - 1 ;
	my $month      = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	my %pay ;
	
	foreach($redis->keys( 'CBS::A::payin::[^u]*_'.$month.'-*' ) )
	{
		my $key = $_ ;
		next if $key =~ /::user/ ;
		next if $key =~ /::rate/ ;
		if ($key =~ /^CBS::A::payin::(.*?)_[-\d]+/ )
		{
			my $pay = $redis->get($key) ;
			$pay_type = $1 ;
			next if $pay_type =~ /uv/ ;
			$pay{'CBS::A::payin::'.$pay_type.'_'.$month } += $pay ;
		}
	}
	insert_redis_hash(\%pay) ;
	
	# 用户消费列表的月统计
	my %pay_user ;
	foreach($redis->keys( 'CBS::A::payin::*::user_'.$month.'-*' ) )
	{
		my $key_user = $_ ;
		my ($pay_type) = $key_user =~ /^CBS::A::payin::(.+)::user/ ;
		foreach($redis -> zrange($key_user, 0, -1))
		{
			my $id = $_ ;
			my $pay = $redis->zscore($key_user , $id) ;
			$pay_user{$pay_type.'_'.$id} += $pay ;
		}
	}
	foreach( keys %pay_user )
	{
		my ($pay_type,$accountId) = split '_' , $_ ;
		my $pay = $pay_user{$pay_type.'_'.$accountId} ;
		$redis->zadd('CBS::A::payin::'.$pay_type.'::user_'.$month  , $pay , $accountId) ;
	}
	
	
	foreach( $redis_db2 -> keys( 'CBS::payin*:uv_'.$month ) )
	{
		my $k = $_ ;
		if ($k =~ /^CBS::(payin.*?:uv)_/ )
		{
			my $type = $1 ;
			my $count = $redis_db2 -> scard($k) ;
			insert_redis_scalar( 'CBS::A::'.$type.'_'.$month , $count ) ;
		}
	}
}
#=cut

#=pod
# ---------------------------------------
# 重复付费率／充值率
# ---------------------------------------
say "-> Redis.CBS::A::payin::rate2_DAY" ;
for ( 1 .. $time_step)
{
		my $days_step = $_ - 1 ;
		my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
		#$redis -> exists( 'CBS::A::payin::rate2_'.$key_day ) && next ;			# 如果需要回滚操作请注释
		
        # 付费率 ＝ 付费人数 ／ 活跃用户数
		my $uv_pay = $redis->get('CBS::A::payin::uv_'.$key_day) ;
		my $uv_active = $redis_storm->get('CBS::A::user::active_'.$key_day) ;
		my $rate = sprintf("%.4f" , $uv_pay / $uv_active ) if $uv_active;
        $rate = 1 if $uv_pay > $uv_active ;
		insert_redis_scalar('CBS::A::payin::rate_'.$key_day , $rate ) if $rate ;
        
        my $uv_pay_withrobot = $redis->get('CBS::A::payin::withrobot::uv_'.$key_day) ;
        my $rate_withrobot = sprintf("%.4f" , $uv_pay_withrobot / $uv_active ) if $uv_active;
        $rate_withrobot = 1 if $uv_pay_withrobot > $uv_active ;
		insert_redis_scalar('CBS::A::payin::withrobot::rate_'.$key_day , $rate_withrobot ) if $rate_withrobot ;
		
		# 充值率 ＝ 充值人数 ／ 活跃用户数
		my $uv_pay_recharge = $redis->get('CBS::A::payin::recharge::uv_'.$key_day) ;
		my $rate_recharge = sprintf("%.4f" , $uv_pay_recharge / $uv_active ) if $uv_active;
		insert_redis_scalar('CBS::A::payin::rate::recharge_'.$key_day , $rate_recharge ) if $rate_recharge ;
        
        my $uv_pay_recharge_withrobot = $redis->get('CBS::A::payin::recharge::withrobot::uv_'.$key_day) ;
		my $rate_recharge_withrobot = sprintf("%.4f" , $uv_pay_recharge_withrobot / $uv_active ) if $uv_active;
		insert_redis_scalar('CBS::A::payin::rate::recharge::withrobot_'.$key_day , $rate_recharge_withrobot ) if $rate_recharge_withrobot ;
        
        # 重复付费率／重复充值率
		my %uv_pays ;
		my %uv_pays_recharge ;
		for(1 .. 30)
		{
			my $temp_day = strftime("%Y-%m-%d", localtime(time() - 86400 * ($days_step + $_ - 1))) ;
			my @uvs = $redis_db2->smembers('CBS::payin::uv_'.$temp_day) ;
			$uv_pays{$_} ++ for @uvs ;
			
			my @uvs_recharge = $redis_db2->smembers('CBS::payin::recharge::uv_'.$temp_day) ;
			$uv_pays_recharge{$_} ++ for @uvs_recharge ;
		}
		
		my $num_all = scalar keys %uv_pays ;
		my $num_rechage = scalar keys %uv_pays_recharge ;
		
		my $num_2 = scalar grep { $uv_pays{$_} > 1 } keys %uv_pays ;
		my $rate2 = sprintf("%.4f" , $num_2 / $num_all ) if $num_all;
		
		my $num_recharge_2 = scalar grep { $uv_pays_recharge{$_} > 1 } keys %uv_pays_recharge ;
		my $rate2_recharge = sprintf("%.4f" , $num_recharge_2 / $num_rechage ) if $num_rechage;
		
		insert_redis_scalar('CBS::A::payin::rate2_'.$key_day , $rate2 ) if $rate2;
		insert_redis_scalar('CBS::A::payin::rate2::recharge_'.$key_day , $rate2_recharge ) if $rate2_recharge ;
		
}
#=cut

#=pod
say "-> Redis.CBS::A::payin::bet::[bb/fb/yy/play]::[lb1/lb0]_DAY" ;

for ( 1 .. $time_step+1 )
{
		my $days_step = $_ - 1 ;
		my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
		my ($month) = $key_day =~ /^(\d+-\d+)-\d+$/ ;
		
		my %temp ;
		my @ops = ('jc','op') ;			# 目前有这2种玩法，数据结构一致所以这里循环了，如果哪天玩法业务改规则了可能就得拆开
		# 篮球
		for(@ops)
		{
			my $op = $_ ;   # jc / op
			
			my $sth = $dbh_new -> prepare("
										SELECT bId,userId,bet,back,isLongbi,coupon
										FROM
										cbs_bb_bet_$op
										WHERE
										createTime between '$key_day 00:00:00' and '$key_day 23:59:59'
										");
			$sth -> execute();
			while (my $ref = $sth -> fetchrow_hashref())
			{
				my $userId   = $ref -> {userId} ;
				my $bet      = $ref -> {bet} ;
				my $back     = $ref -> {back} ;
				my $isLongbi = $ref -> {isLongbi} ;
				my $coupon   = $ref -> {coupon} ;
				
				next if exists $robots{$userId} ;		# 忽略机器人
				
				if ($isLongbi == 1)
				{
					my $bet_long = $bet - $coupon ;
					$temp{'CBS::A::payin::bet::bb::'.$op.'::lb1_'.$key_day} += $bet_long ;
					$temp{'CBS::A::payin::bet::bb::lb1_'.$key_day} += $bet_long ;
					
					$temp{'CBS::A::payin::bet::bb::'.$op.'::lb1::back_'.$key_day} += $back ;
					$temp{'CBS::A::payin::bet::bb::lb1::back_'.$key_day} += $back ;
					
					$temp{'CBS::A::payin::bet::bb::'.$op.'::lb1::times_'.$key_day} ++ ;
					$temp{'CBS::A::payin::bet::bb::lb1::times_'.$key_day} ++ ;
					
					$redis_db2 -> sadd( 'CBS::payin::bet::bb::'.$op.'::lb1::uv_'.$key_day , $userId ) ;
					$redis_db2 -> sadd( 'CBS::payin::bet::bb::lb1::uv_'.$key_day , $userId ) ;
					
					if ( $coupon > 0 )
					{
							$temp{'CBS::A::payin::bet::bb::'.$op.'::lb0_'.$key_day} += $coupon ;
							$temp{'CBS::A::payin::bet::bb::lb0_'.$key_day} += $coupon ;
					
							$temp{'CBS::A::payin::bet::bb::'.$op.'::lb0::times_'.$key_day} ++ ;
							$temp{'CBS::A::payin::bet::bb::lb0::times_'.$key_day} ++ ;		
							
							$redis_db2 -> sadd( 'CBS::payin::bet::bb::'.$op.'::lb0::uv_'.$key_day , $userId ) ;
							$redis_db2 -> sadd( 'CBS::payin::bet::bb::lb0::uv_'.$key_day , $userId ) ;
                    }
						  
				}
				elsif($isLongbi == 0)
				{
					$temp{'CBS::A::payin::bet::bb::'.$op.'::lb0_'.$key_day} += $bet ;
					$temp{'CBS::A::payin::bet::bb::lb0_'.$key_day} += $bet ;
					
					$temp{'CBS::A::payin::bet::bb::'.$op.'::lb0::back_'.$key_day} += $back ;
					$temp{'CBS::A::payin::bet::bb::lb0::back_'.$key_day} += $back ;
					
					$temp{'CBS::A::payin::bet::bb::'.$op.'::lb0::times_'.$key_day} ++ ;
					$temp{'CBS::A::payin::bet::bb::lb0::times_'.$key_day} ++ ;
					
					$redis_db2 -> sadd( 'CBS::payin::bet::bb::'.$op.'::lb0::uv_'.$key_day , $userId ) ;
					$redis_db2 -> sadd( 'CBS::payin::bet::bb::lb0::uv_'.$key_day , $userId ) ;
				}
				
			}
		} # END for(@ops)
		
		# 足球，这里逻辑和篮球是一样的，分开写是为足球玩法后续出现不同于篮球的业务
		for(@ops)
		{
			my $op = $_ ;   # jc / op
			
			my $sth = $dbh_new -> prepare("
										SELECT bId,userId,bet,back,isLongbi,coupon
										FROM
										cbs_fb_bet_$op
										WHERE
										createTime between '$key_day 00:00:00' and '$key_day 23:59:59'
										");
			$sth -> execute();
			while (my $ref = $sth -> fetchrow_hashref())
			{
				my $userId   = $ref -> {userId} ;
				my $bet      = $ref -> {bet} ;
				my $back     = $ref -> {back} ;
				my $isLongbi = $ref -> {isLongbi} ;
				my $coupon   = $ref -> {coupon} ;
				
				next if exists $robots{$userId} ;		# 忽略机器人
				
				if ($isLongbi == 1)
				{
					my $bet_long = $bet - $coupon ;
					$temp{'CBS::A::payin::bet::fb::'.$op.'::lb1_'.$key_day} += $bet_long ;
					$temp{'CBS::A::payin::bet::fb::lb1_'.$key_day} += $bet_long ;
					
					$temp{'CBS::A::payin::bet::fb::'.$op.'::lb1::back_'.$key_day} += $back ;
					$temp{'CBS::A::payin::bet::fb::lb1::back_'.$key_day} += $back ;
					
					$temp{'CBS::A::payin::bet::fb::'.$op.'::lb1::times_'.$key_day} ++ ;
					$temp{'CBS::A::payin::bet::fb::lb1::times_'.$key_day} ++ ;
					
					$redis_db2 -> sadd( 'CBS::payin::bet::fb::'.$op.'::lb1::uv_'.$key_day , $userId ) ;
					$redis_db2 -> sadd( 'CBS::payin::bet::fb::lb1::uv_'.$key_day , $userId ) ;
					
					if ( $coupon > 0 )
					{
							$temp{'CBS::A::payin::bet::fb::'.$op.'::lb0_'.$key_day} += $coupon ;
							$temp{'CBS::A::payin::bet::fb::lb0_'.$key_day} += $coupon ;
					
							$temp{'CBS::A::payin::bet::fb::'.$op.'::lb0::times_'.$key_day} ++ ;
							$temp{'CBS::A::payin::bet::fb::lb0::times_'.$key_day} ++ ;		
							
							$redis_db2 -> sadd( 'CBS::payin::bet::fb::'.$op.'::lb0::uv_'.$key_day , $userId ) ;
							$redis_db2 -> sadd( 'CBS::payin::bet::fb::lb0::uv_'.$key_day , $userId ) ;
                    }
						  
				}elsif($isLongbi == 0)
				{
					$temp{'CBS::A::payin::bet::fb::'.$op.'::lb0_'.$key_day} += $bet ;
					$temp{'CBS::A::payin::bet::fb::lb0_'.$key_day} += $bet ;
					
					$temp{'CBS::A::payin::bet::fb::'.$op.'::lb0::back_'.$key_day} += $back ;
					$temp{'CBS::A::payin::bet::fb::lb0::back_'.$key_day} += $back ;
					
					$temp{'CBS::A::payin::bet::fb::'.$op.'::lb0::times_'.$key_day} ++ ;
					$temp{'CBS::A::payin::bet::fb::lb0::times_'.$key_day} ++ ;
					
					$redis_db2 -> sadd( 'CBS::payin::bet::fb::'.$op.'::lb0::uv_'.$key_day , $userId ) ;
					$redis_db2 -> sadd( 'CBS::payin::bet::fb::lb0::uv_'.$key_day , $userId ) ;
				}
				
			}
		} # END for(@ops)
		
		# 押押
		my $sth_yy = $dbh_new -> prepare("
										SELECT userId,bet,back,isLongbi,coupon
										FROM
										cbs_yy_bet
										WHERE
										createTime between '$key_day 00:00:00' and '$key_day 23:59:59'
										");
		$sth_yy -> execute();
		while (my $ref = $sth_yy -> fetchrow_hashref())
		{
				my $userId   = $ref -> {userId} ;
				my $bet      = $ref -> {bet} ;
				my $back     = $ref -> {back} ;
				my $isLongbi = $ref -> {isLongbi} ;
				my $coupon   = $ref -> {coupon} ;
				
				next if exists $robots{$userId} ;		# 忽略机器人
				
				if ($isLongbi == 1)
				{
					my $bet_long = $bet - $coupon ;
					$temp{'CBS::A::payin::bet::yy::lb1_'.$key_day} += $bet_long ;
					$temp{'CBS::A::payin::bet::yy::lb1::back_'.$key_day} += $back ;
					$temp{'CBS::A::payin::bet::yy::lb1::times_'.$key_day} ++ ;
					$redis_db2 -> sadd( 'CBS::payin::bet::yy::lb1::uv_'.$key_day , $userId ) ;
					
					if ( $coupon > 0 )
					{
						$temp{'CBS::A::payin::bet::yy::lb0_'.$key_day} += $coupon ;
						$temp{'CBS::A::payin::bet::yy::lb0::times_'.$key_day} ++ ;
						$redis_db2 -> sadd( 'CBS::payin::bet::yy::lb0::uv_'.$key_day , $userId ) ;
					}
				}
				elsif($isLongbi == 0)
				{
					$temp{'CBS::A::payin::bet::yy::lb0_'.$key_day} += $bet ;
					$temp{'CBS::A::payin::bet::yy::lb0::back_'.$key_day} += $back ;
					$temp{'CBS::A::payin::bet::yy::lb0::times_'.$key_day} ++ ;
					$redis_db2 -> sadd( 'CBS::payin::bet::yy::lb0::uv_'.$key_day , $userId ) ;
				}
				
		}
		$sth_yy -> finish() ;
		
		
		# 游戏
		my $sth_play = $dbh_new -> prepare("
										SELECT userId,betSum,backSum,isLongbi
										FROM
										cbs_game_play
										WHERE
										createTime between '$key_day 00:00:00' and '$key_day 23:59:59'
										");
		$sth_play -> execute();
		while (my $ref = $sth_play -> fetchrow_hashref())
		{
				my $userId   = $ref -> {userId} ;
				my $bet      = $ref -> {betSum} ;
				my $back     = $ref -> {backSum} ;
				my $isLongbi = $ref -> {isLongbi} ;
				
				next if exists $robots{$userId} ;		# 忽略机器人 
				
				if ($isLongbi == 1)
				{
					$temp{'CBS::A::payin::bet::play::lb1_'.$key_day} += $bet ;
					$temp{'CBS::A::payin::bet::play::lb1::back_'.$key_day} += $back ;
					$temp{'CBS::A::payin::bet::play::lb1::times_'.$key_day} ++ ;
					$redis_db2 -> sadd( 'CBS::payin::bet::play::lb1::uv_'.$key_day , $userId ) ;
						  
				}elsif($isLongbi == 0)
				{
					$temp{'CBS::A::payin::bet::play::lb0_'.$key_day} += $bet ;
					$temp{'CBS::A::payin::bet::play::lb0::back_'.$key_day} += $back ;
					$temp{'CBS::A::payin::bet::play::lb0::times_'.$key_day} ++ ;
					$redis_db2 -> sadd( 'CBS::payin::bet::play::lb0::uv_'.$key_day , $userId ) ;
				}
				
		}
		$sth_play -> finish() ;
		
		
		insert_redis_hash(\%temp) ;
		
        # 各种下注的人数
		foreach( $redis_db2 -> keys( 'CBS::payin::bet::*::uv_'.$key_day ) )
		{
				my $k = $_ ;
				if ($k =~ /^CBS::(payin::bet.*?)::uv_/ )
				{
					my $type = $1 ;
					my $count = $redis_db2 -> scard($k) ;
					insert_redis_scalar( 'CBS::A::'.$type.'::uv_'.$key_day  , $count ) if $count;
				}
		}
}

say "-> Redis.CBS::A::payin::bet::*_MONTH" ;
for ( 1 .. $num_month_ago )	
{
	my $month_ago = $_ - 1 ;
	my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	say "\t $month" ;
	
	my %month ;
	foreach( $redis->keys( 'CBS::A::payin::bet::*_'.$month.'-*' ) )
	{
		my $key = $_ ;		
		my $type ;
		next if $key =~ /::uv/ ;
		next if $key =~ /::user/ ;
		if ($key =~ /^CBS::A::payin::(bet::.*?)_[\d\-]+$/ )
		{
			$type = $1 ;
			my $num = $redis->get($key);
			$month{'CBS::A::payin::'.$type.'_'.$month  } += $num ;
		}
	}

	insert_redis_hash(\%month) ;
	
	my %uvs ;
	foreach( $redis_db2 -> keys( 'CBS::payin::bet::*::uv_'.$month.'-*' ) )
	{
				my $k = $_ ;
				if ($k =~ /^CBS::(payin::bet.*?)::uv_/ )
				{
					my $type = $1 ;
					foreach($redis_db2->smembers($k)){
						my $id = $_ ;
						$uvs{'CBS::A::'.$type.'::uv_'.$month}{$id} = 1 ;
					}
					
				}
	}
	foreach(keys %uvs)
	{
			my $key = $_ ;
			my $count = scalar keys %{$uvs{$key}} ;
			insert_redis_scalar( $key  , $count ) if $count;
	}
	
}

$lockfile->remove;      # 删除开头为了实现单实例进程而创建的文件锁
#=cut

# ==================================== functions =========================================

sub get_user_from_accountId
{
	my ($dbh,$accountId) = @_ ;
	my $ref_accountId ;
	my $sth_account = $dbh -> prepare(" SELECT userId,userNO,userName,gender,status FROM cbs_user WHERE userId = $accountId ") ;
	$sth_account -> execute();
	while (my $ref_account = $sth_account -> fetchrow_hashref())
	{
		my $accountId = $ref_account -> {userId};
		my $l99NO     = $ref_account -> {userNO};
		my $name      = $ref_account -> {userName};
		my $gender    = $ref_account -> {gender} ;
		my $status    = $ref_account -> {status} ;
		$ref_accountId -> {l99NO}  = $l99NO ;
		$ref_accountId -> {name}   = $name ;
		$ref_accountId -> {gender} = $gender ;
		$ref_accountId -> {status} = $status ;
	}
	$sth_account -> finish ;
	return $ref_accountId ;
}

sub insert_redis_scalar
{
    my ($rediskey,$redisvalue) = @_ ;
    $redis->set($rediskey,$redisvalue);
    say "$rediskey => $redisvalue" ;
}

sub insert_redis_hash
{
    my ($ref) = @_ ;
    foreach (keys %$ref)
    {
        my $key = $_ ;
        my $value = $ref->{$key} ;
        $redis->set($key,$value);
		say "$key => $value" ;
    }
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


sub get_user_market
{
	my ($redis_p,$time) = @_ ;
	my $ref ;
	foreach( $redis_p->keys( 'CBS::user::active::market::*_'.$time ) )
	{
		my $key = $_ ;
		my ($s) = $key =~ /market::(.*?)_\d{4}/ ;
		next if $s =~ /version/ ;
		$s =~ s/^cbs_// ;
		my @temp = $redis_p->smembers($key) ;
		foreach(@temp){
			my $l99NO = $_ ;
			$ref -> {$l99NO} = $s ;
		}
	}
	return $ref ;
}

sub get_user_version
{
	my ($redis_p,$time) = @_ ;
	my $ref ;
	foreach( $redis_p->keys( 'CBS::user::active::version::*_'.$time ) )
	{
		my $key = $_ ;
		my ($s) = $key =~ /version::(.*?)_\d{4}/ ;
		say $s ;
		my @temp = $redis_p->smembers($key) ;
		foreach(@temp){
			my $l99NO = $_ ;
			$ref -> {$l99NO} = $s ;
		}
	}
	return $ref ;
}

=pod

$redis -> set('key' => 'value');
$redis -> incr($key);
$redis -> rpush($key , $value);

# hello world
