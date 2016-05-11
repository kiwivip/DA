#!/usr/bin/env perl 
# ==============================================================================
# function:		床上项目数据统计
# Author:		kiwi
# createTime:	2015.4.7
# ==============================================================================
use 5.10.1 ;

BEGIN {
        my @PMs = (
		   #'Redis' ,
		   #'Config::Tiny' ,
		   #'DBI' ,
		   #'JSON::XS' ,
		   #'Date::Calc::XS' ,
		   #'Time::Local'
	) ;
        foreach(@PMs){
                my $pm = $_ ;
                eval {require $pm;};
                if ($@ =~ /^Can't locate/) {
                        print "install module $pm";
                        `cpanm $pm`;			# 需要Linux事先配置好cpanm环境
                }
        }
}

use utf8 ;
use Redis;
use MongoDB;
use Config::Tiny ;
use DBI ;
use JSON::XS ;
use LWP::Simple;
use POSIX qw(strftime);
use Time::Local ;
use Date::Calc::XS qw (Date_to_Time Time_to_Date);
use Unicode::UTF8 qw (decode_utf8 encode_utf8);
binmode(STDIN,  ':encoding(utf8)');
binmode(STDOUT, ':encoding(utf8)');
binmode(STDERR, ':encoding(utf8)');

$| = 1;
die 'The last perl script is not over yet ...' if -e '/tmp/kkkk' ; 
`touch /tmp/kkkk` ;										
# -----------------------------------------------------------------------
# read config.ini
# -----------------------------------------------------------------------
my $Config = Config::Tiny->new;
$Config = Config::Tiny->read( '/home/DA/DataAnalysis/config.ini', 'utf8' );
my $version = $Config -> {_} -> {version};

my $redis_host_market = $Config -> {REDIS} -> {host_market};
my $redis_host = $Config -> {REDIS} -> {host};
my $redis_port = $Config -> {REDIS} -> {port};

my $mongo_host = $Config -> {MONGODB} -> {host};
my $mongo_port = $Config -> {MONGODB} -> {port};

my $L06_host     = $Config -> {L06_DB} -> {host};
my $L06_db       = $Config -> {L06_DB} -> {database} ;
my $L06_usr      = $Config -> {L06_DB} -> {username};
my $L06_password = $Config -> {L06_DB} -> {password};

my $CS_host      = $Config -> {CS_DB}  -> {host};
my $CS_db        = $Config -> {CS_DB}  -> {database};
my $CS_usr       = $Config -> {CS_DB}  -> {username};
my $CS_password  = $Config -> {CS_DB}  -> {password};
my $CS_user_dbs  = $Config -> {CS_DB}  -> {database_account} ;
my $CS_db_comment= $Config -> {CS_DB}  -> {database_comment} ;
my $CS_log_dir   = $Config -> {CS_LOG} -> {dir} ;
my $article_topN = $Config -> {CS_LOG} -> {topN} ;

my $Wwere_host     = $Config -> {Wwere_DB} -> {host};
my $Wwere_db       = $Config -> {Wwere_DB} -> {database} ;
my $Wwere_usr      = $Config -> {Wwere_DB} -> {username};
my $Wwere_password = $Config -> {Wwere_DB} -> {password};

#my $time_step = 5 ;
my $time_step = $Config -> {time} -> {step} ;													# 设置为往前推 N天 统计，默认 N = 1 	
my $day_start = strftime( "%Y-%m-%d 00:00:00" , localtime(time() - 86400 * $time_step) );		# %Y-%m-%d %H:%M:%S
my ($sec,$min,$hour,$dday,$mmon,$yyear,$wday,$yday,$isdst) = localtime(time - 86400 * $time_step);    
$yyear += 1900;    
my $timestamp_start = timelocal(0,  0,  0 , $dday , $mmon, $yyear);
my $num_month_ago = $time_step / 30 + 1;

# ------------------------------------------------------------------------------------------------
# connect to Redis & mongoDB
# ------------------------------------------------------------------------------------------------
my $redis    = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
my $redis_ip = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
$redis_ip -> select(1) ;
my $redis_market = Redis->new(server => "$redis_host_market:$redis_port",reconnect => 10, every => 2000);
$redis_market -> select(6) ;
my $redis_active = Redis->new(server => "192.168.201.57:6379",reconnect => 10, every => 2000);
my $mongo = MongoDB::MongoClient->new(host => $mongo_host, port => $mongo_port , query_timeout => 1000000);
my $collection_present = $mongo -> get_database( 'cs' ) -> get_collection( 'presents' );

# ------------------------------------------------------------------
# connect to mysql
# ------------------------------------------------------------------
my $dsn   = "DBI:mysql:database=$CS_db;host=$CS_host" ;
my $dbh   = DBI -> connect($dsn, $CS_usr, $CS_password, {'RaiseError' => 1} ) ;
$dbh -> do ("SET NAMES UTF8");


my $dsn_v506 = "DBI:mysql:database=$L06_db;host=$L06_host" ;
my $dbh_v506 = DBI -> connect($dsn_v506, $L06_usr, $L06_password, {'RaiseError' => 1} ) ;
$dbh_v506 -> do ("SET NAMES UTF8");

my $dsn_wwere  = "DBI:mysql:database=$Wwere_db;host=$Wwere_host" ;
my $dbh_wwere = DBI -> connect($dsn_wwere, $Wwere_usr, $Wwere_password, {'RaiseError' => 1} ) ;
$dbh_wwere -> do ("SET NAMES UTF8");

=pod 
# ----------------------------------------------------------------------
# 活跃用户的渠道，storm坏了 临时的逻辑 先留着
# ----------------------------------------------------------------------

#STORM::CS::A::user::active::gender::0/1::market::AppStore_TIME
#STORM::CS::A::user::active::market::AppStore_TIME
#STORM::CS::A::user::active::hour::N::gender::0/1::market::AppStore_TIME
#STORM::CS::A::user::active::hour::N::market::AppStore_TIME

for ( 1 .. $time_step + 1)
{
	my $days_step = $_ - 1 ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
	
	my %active ;
	my $num_gender0_ios ;
	my $num_gender1_ios ;
	my $num_ios ;

	#foreach( $redis_active->keys( 'STORM::CS::user::active::hour*_'.$key_day ) )
	#{
	#	my $k = $_ ;
	#	my $hour ;
	#	if ($k =~ /STORM::CS::user::active::hour(\d+)_/) {
	#		$hour = $1 ;
	#	}
		
		my @users = $redis_active->smembers('STORM::CS::user::active_'.$key_day) ;
		foreach(@users)
		{
			my $l99NO = $_ ;
			my $accountId = get_accountId_from_l99NO($dbh_v506,$l99NO) ;
			my $ref_temp = get_user_market2_from_accountId($dbh_v506,$accountId) ;
			my $market = $ref_temp -> {market} ;
			next unless $market eq 'AppStore' ;
			$num_ios ++ ;
			my $gender ;
			my $sth_user = $dbh -> prepare("
                                        SELECT gender
					FROM
					nyx_1.account
					WHERE
					accountId = $accountId
				       ");
			$sth_user -> execute();
			while (my $ref = $sth_user -> fetchrow_hashref())
			{ $gender = $ref -> {gender} }
			$num_gender0_ios ++ if $gender == 0 ;
			$num_gender1_ios ++ if $gender == 1 ;
		}	
	$redis_active->set( 'STORM::CS::A::user::active::gender::0::market::AppStore_'.$key_day	, $num_gender0_ios ) ;
	$redis_active->set( 'STORM::CS::A::user::active::gender::1::market::AppStore_'.$key_day	, $num_gender1_ios ) ;
	$redis_active->set( 'STORM::CS::A::user::active::market::AppStore_'.$key_day , $num_ios ) ;
	
}

=cut


#=pod
# -----------------------------------------------------------------------------
#  扫描新增用户   insert into MongoDB
# -----------------------------------------------------------------------------

say "-> MongoDB.cs.user " ;
foreach(split(',',$CS_user_dbs))
{
	my $table_user = $_ ;
	#say $table_user ;
	my $database   = $mongo -> get_database( 'cs' );
	my $collection = $database -> get_collection( 'user' );
	
	my $sth_user = $dbh -> prepare("
                                        SELECT accountId,gender,lat,lng,localName,createTime
										FROM
										$table_user
										WHERE
										createTime > '$day_start'
									");
	$sth_user -> execute();
	while (my $ref = $sth_user -> fetchrow_hashref())
	{

		my $accountId  = $ref -> {accountId} ;
		next if $collection->find_one({ accountId => $accountId }) ;    	# 如果Mongo表中有了，就跳过
		
		my $gender     = $ref -> {gender} ;
		my $lat        = $ref -> {lat} ;
		my $lng        = $ref -> {lng} ;
		my $cityName   = decode_utf8 $ref -> {localName} ;
		my $time       = $ref -> {createTime} ;
		
		my $ref_account = get_user_from_accountId($dbh_v506,$accountId) ;
        my ($l99NO,$name) = ($ref_account->{l99NO} , $ref_account->{name}) ;

        
		my $province ;
		if ($cityName =~ /北京|Beijing/i){ $province = 'CN_北京市' ;}
                else{
		    $province = decode_utf8 getProvince($lat,$lng) ;
		}

		$collection -> insert( {"accountId" => $accountId , "l99NO" => $l99NO , "name" => $name ,
					"gender" => $gender , "lat" => $lat , "lng" => $lng ,
					"province" => $province ,"time" => $time} );
	}
	$sth_user -> finish();
}

say "-> Redis.CS::A::user::new::location" ;
my %user_province ;
my $database   = $mongo -> get_database( 'cs' );
my $collection = $database -> get_collection( 'user' );
my $data = $collection->find({'time' => { '$gte' => $day_start}}); 
while (my $ref = $data -> next)
{
    my $accountId = $ref -> {accountId} ;
    my $time      = $ref -> {time} ;
    my $province  = $ref -> {province} ;
    
    my ($day) = $time =~ /^(\d+-\d+-\d+) / ;
    my $key_province_day = encode_utf8 "CS::A::user::new::location::{'PROVINCE','$province'}" .  "_" .$day ; 
    $user_province{$key_province_day} ++ ;
}
insert_redis_hash(\%user_province) ;
#=cut

#=pod
# ----------------------------------------------------------------
# 新增用户统计	CS::A::user::new
# ----------------------------------------------------------------
say "-> Redis.CS::A::user::new*_DAY" ;
for ( 1 .. $time_step + 1)
{
    my $days_step = $_ - 1 ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    
	my $num_new ;
    my ($num_gender_0,$num_gender_1) ;			# 新增用户分性别数量
    my ($num_ios,$num_android) ;				# 新增用户分系统数量
    my %user_new_hours ;
    
	my $ref_user_version = get_user_version($redis_active,$key_day) ;
	
    # 因为床上的分库分表不用了，这里又改回直接扫单表了，蛋疼
    my $dsn_cs = "DBI:mysql:database=$CS_db;host=$CS_host" ;
    my $dbh_cs = DBI -> connect($dsn_cs, $CS_usr, $CS_password, {'RaiseError' => 1} ) ;
    $dbh_cs -> do ("SET NAMES UTF8");
    
    foreach(split(',',$CS_user_dbs))
    {
        my $table_user = $_ ;
        my $sth_user = $dbh_cs -> prepare("
					  SELECT accountId,gender,createTime
					  FROM $table_user
					  WHERE
					  createTime between '$key_day 00:00:00' and '$key_day 23:59:59'
					  ");
        $sth_user -> execute();
        while (my $ref = $sth_user -> fetchrow_hashref())
        {
        	my $accountId  = $ref -> {accountId} ;
        	my $gender     = $ref -> {gender} ;
        	my $time       = $ref -> {createTime} ;
        	my ($hour)     = $time =~ / (\d+):\d+:\d+$/ ;
            
        	my $ref_account_l99NO = get_user_from_accountId($dbh_v506,$accountId) ;
        	my $l99NO = $ref_account_l99NO->{l99NO} ;
        	#my $market = $redis_market->get('STORM::CS::user::market::'.$l99NO) ;		# 通过龙号拿渠道 in Redis's STORM-key
            
            $redis_active -> sadd('STORM::CS::user::new_'.$key_day , $l99NO );
            
			my $version = $ref_user_version -> {$l99NO} ;
			
        	my $ref_temp = get_user_market2_from_accountId($dbh_v506,$accountId) ;
        	my $market = $ref_temp -> {market} ;
            
        	my $ref_account = get_user_market_from_accountId($dbh_v506,$accountId) ;
            my $os = $ref_account -> {market} ;
            
        	$user_new_hours{'CS::A::user::new::hour'.$hour.'::gender::'.$gender.'::'.$os.'_'.$key_day} ++ if $os;
        	$user_new_hours{'CS::A::user::new::hour'.$hour.'::gender::'.$gender.'_'.$key_day} ++ ;
        	$user_new_hours{'CS::A::user::new::hour'.$hour.'::'.$os.'_'.$key_day} ++ if $os;	
        	$user_new_hours{'CS::A::user::new::gender::'.$gender.'::'.$os.'_'.$key_day} ++ if $os;
        	$user_new_hours{'CS::A::user::new::hour'.$hour.'_'.$key_day} ++ ;
        	$user_new_hours{'CS::A::user::new::gender::'.$gender.'::market::'.$market.'_'.$key_day} ++ if $market;
			$user_new_hours{'CS::A::user::new::gender::'.$gender.'::market::'.$market.'::version::'.$version.'_'.$key_day} ++ if $market && $version;
        	$user_new_hours{'CS::A::user::new::hour'.$hour.'::market::'.$market.'_'.$key_day} ++ if $market;
        	$user_new_hours{'CS::A::user::new::hour'.$hour.'::gender::'.$gender.'::market::'.$market.'_'.$key_day} ++ if $market;
			$user_new_hours{'CS::A::user::new::hour'.$hour.'::market::'.$market.'::version::'.$version.'_'.$key_day} ++ if $market && $version ;
        	$user_new_hours{'CS::A::user::new::hour'.$hour.'::gender::'.$gender.'::market::'.$market.'::version::'.$version.'_'.$key_day} ++ if $market && $version ;
			$user_new_hours{'CS::A::user::new::market::'.$market.'_'.$key_day} ++ if $market ;				    # 分渠道
        	$user_new_hours{'CS::A::user::new::market::'.$market.'::'.$os.'_'.$key_day} ++ if $market && $os ;
			$user_new_hours{'CS::A::user::new::market::'.$market.'::version::'.$version.'_'.$key_day} ++ if $market && $version ;
        	$user_new_hours{'CS::A::user::new::version::'.$version.'_'.$key_day} ++ if $version ;
			
			$num_new ++ ;
			$num_gender_0 ++  if $gender == 0 ;
        	$num_gender_1 = $num_new - $num_gender_0 ;
        	$num_ios ++ if $os eq 'ios' ;
        	$num_android = $num_new - $num_ios ;
	    
        }
        $sth_user -> finish ;
    }
    $dbh_cs -> disconnect ;
    
    insert_redis_scalar('CS::A::user::new::gender::0_'.$key_day , $num_gender_0) if $num_gender_0;
    insert_redis_scalar('CS::A::user::new::gender::1_'.$key_day , $num_gender_1) if $num_gender_1;
    insert_redis_scalar('CS::A::user::new_'.$key_day , $num_new) if $num_new;
    insert_redis_scalar('CS::A::user::new::ios_'    .$key_day , $num_ios)     if $num_ios ;
    insert_redis_scalar('CS::A::user::new::android_'.$key_day , $num_android) if $num_android ;
    
    insert_redis_hash(\%user_new_hours) ;	
  
}
#=cut

#=pod
say '-> Redis.CS::A::user::new::auth_TIME' ;
my %auth_type = (
	'10' => 'Email' , '20' => '手机号'  , '110' => 'QQ微博' , '111' => 'QQ' ,
	'120' => '新浪微博' , '130' => '搜狐微博' , '240' => '微信'
) ;
for ( 1 .. $time_step )
{
    my $days_step = $_ - 1 ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
	
	my $timestamp_l = local2unix($key_day.' 00:00:00') * 1000 ;
	my $timestamp_r = local2unix($key_day.' 23:59:59') * 1000 ;
	my %temp ;
	my $sth_auth = $dbh_v506 -> prepare("
										SELECT r.authId,r.accountId,r.authType,l.market
										FROM account_log l left join account_authentication r 
										on l.accountId = r.accountId
										WHERE
										r.eventTime between $timestamp_l and $timestamp_r
										") ;
	$sth_auth -> execute();
	while (my $ref = $sth_auth -> fetchrow_hashref())
	{
		my $authId = $ref -> {authId} ;
		my $accountId  = $ref -> {accountId} ;
		my $authType = $ref -> {authType} ;
		my $type = $auth_type{$authType} ;
		my $market = $ref -> {market} ;
		$temp{$type} ++ if $market =~ /Bed/ ;
	}
	$sth_auth -> finish ;
	
	my $auth_info = encode_json \%temp ;
	insert_redis_scalar('CS::A::user::new::auth_'.$key_day , $auth_info) ;
}

for ( 1 .. $num_month_ago )	
{
	my $month_ago = $_ - 1 ;
	my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	
	my %temp_month ;
	foreach( $redis->keys( 'CS::A::user::new::auth_'.$month.'-*' ) )
	{
		my $ref = decode_json $redis -> get( $_ ) ;
		foreach(keys %$ref)
		{
			my $type = $_ ;
			my $num = $$ref{$type} ;
			$temp_month{$type} += $num ;
		}
	}
	my $info = encode_json \%temp_month;
	insert_redis_scalar('CS::A::user::new::auth_'.$month , $info) ;
}
#=cut

#=pod
# 新增用户按月的统计，往前取几个月
say "-> Redis.CS::A::user::new*_MONTH" ;

for ( 1 .. $num_month_ago )	
{
	my $month_ago = $_ - 1 ;
	my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	say $month ;
	
	my %user_new_month ;
	foreach( $redis->keys( 'CS::A::user::new*'.'_'.$month.'-*' ) )
	{
		my $key = $_ ;		
		my $type ;
		if ($key =~ /^CS::A::user::new(.*?)_[\d\-]+$/ )
		{
			$type = $1 ;
			next if $type =~ /auth/ ;
			my $num = $redis->get($key);
			$user_new_month{'CS::A::user::new'.$type.'_'.$month  } += $num ;
		}
	}

	insert_redis_hash(\%user_new_month) ;
}

#=cut

#=pod
say '-> Redis.CS::A::user::active_DAY ' ;
for ( 1 .. $time_step)
{
    my $days_step = $_ ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    next if $redis -> get( 'CS::A::user::active_'.$key_day );
	
    my $n = $redis -> scard( 'CS::user::active_'.$key_day );
    insert_redis_scalar('CS::A::user::active_'.$key_day , $n) if $n ;
    
    my %ip_city ;
    my ($gender_1,$gender_0) ;				# 活跃用户分性别统计
    
    my @elements = $redis->smembers( 'CS::user::active_'.$key_day );
    foreach(@elements)
	{
		next unless /_/ ;
		next if /^__$/ ;
		my ($l99NO,$ip,$gender) = split '_' , $_ ;
		$gender_0 ++ if $gender eq 'gender0' ;
	
		# user in province
		my $city = $redis_ip->get($ip) ;
		my ($province) = $city =~ /^(.*?_.*?)_.*?$/ ;
		$ip_city{$province} ++ ;
    }
    $gender_1 = $n - $gender_0 ;
    
    insert_redis_scalar('CS::A::user::active::gender::1_'.$key_day , $gender_1) if $gender_1;
    insert_redis_scalar('CS::A::user::active::gender::0_'.$key_day , $gender_0) if $gender_0;
    
    my $temp = join ';' , map {$_ . ',' . $ip_city{$_}} keys %ip_city ;
    insert_redis_scalar('CS::A::user::active::ip_'.$key_day , $temp) ;

}

for ( 1 .. $num_month_ago)
{
	my $month_ago = $_ - 1 ;
	my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	next if $redis -> get( 'CS::A::user::active_'.$month );
   
	my $n = $redis -> scard( 'CS::user::active_'.$month ) ;
	insert_redis_scalar('CS::A::user::active_'.$month , $n) ;

	my @elements_month = $redis->smembers( 'CS::user::active_'.$month );
	my $gender_0_month = ~~ grep {/_gender0/} @elements_month ;
	my $gender_1_month = $n - $gender_0_month ;
	
	insert_redis_scalar('CS::A::user::active::gender::1_'.$month , $gender_1_month) if $gender_1_month;
	insert_redis_scalar('CS::A::user::active::gender::0_'.$month , $gender_0_month) if $gender_0_month;
}
#=cut

# -----------------------------------------------------------------------
# CS::A::article 文章阅读排行
# -----------------------------------------------------------------------
say "-> Redis.CS::A::content::article::topN_TIME " ;

for ( 1 .. $time_step)
{
    my $days_step = $_ ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;

    $redis -> exists('CS::A::content::article::top1_'.$key_day) && next ;
    
    my $key = 'CS::article_'.$key_day ;
    my $value = $redis->get($key) ;
    next unless $value ;
    
    my %temp ;
    foreach(split(';',$value))
    {
		my ($article,$num) = split ',' , $_ ;
		$temp{$article} =  $num ;
    }
	my $i = 1;
	foreach (sort { $temp{$b} <=> $temp{$a} } keys %temp)
	{
		last if $i > $article_topN ;
        my $article = $_ ;
		my $num = $temp{$article} ;
		insert_redis_scalar( 'CS::A::content::article::top'.$i.'_'.$key_day , $article.','.$num ) ;
        $i ++ ;
	}

}

for ( 1 .. $num_month_ago )
{
	my $month_ago = $_ - 1 ;
	my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	
	my %temp_month ;
	foreach($redis->keys( 'CS::article_'.$month.'-*' ) )
	{
		my $key = $_ ;							
		my $articles = $redis->get($key) ;
		foreach(split(';',$articles)){
			my ($article,$num) = split ',' , $_ ;
			$temp_month{$article} += $num ;
		}
	}
	my $i = 1;
	foreach ( sort { $temp_month{$b} <=> $temp_month{$a} } keys %temp_month )
	{
		last if $i > $article_topN ;
        my $article = $_ ;
		my $num = $temp_month{$article} ;
		insert_redis_scalar( 'CS::A::content::article::top'.$i.'_'.$month , $article.','.$num ) ;
        $i ++ ;
	}
	
}

#=pod
# ---------------------------------------------------------------------------------------
# 床上用户内容被选入各版块的数量
# ---------------------------------------------------------------------------------------
say "-> Redis.CS::A::content::guide" ;
for ( 1 .. $time_step )
{
	my $days_step = $_ - 1 ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    
	my %guides ;
	my $sth_guide = $dbh -> prepare("
                                        SELECT typeId,dashboardId FROM
					content_guide
					WHERE
					createTime between '$key_day 00:00:00' and '$key_day 23:59:59'
					");
	$sth_guide -> execute();
	while (my $ref = $sth_guide -> fetchrow_hashref())
	{
		my $typeId = $ref -> {typeId} ;
		$guides{$typeId} ++ ;
	}
	my $guide_info = encode_json \%guides;
	#say $guide_info ;
	insert_redis_scalar('CS::A::content::guide_'.$key_day , $guide_info) ;
}

for ( 1 .. $num_month_ago)	# 往前取几个月
{
	my $month_ago = $_ - 1 ;
	my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
   
	my %guide_month ;
	foreach($redis->keys( 'CS::A::content::guide_'.$month.'-*' ) )
	{
		my $ref_words = decode_json $redis -> get( $_ ) ;
		foreach(keys %$ref_words)
		{
			my $typeId = $_ ;
			my $num = $$ref_words{$typeId} ;
			$guide_month{$typeId} += $num ;
		}
	}
	my $guide_info = encode_json \%guide_month;
	insert_redis_scalar('CS::A::content::guide_'.$month , $guide_info) ;
}

# -----------------------------------------------------------
# 用户发布内容
# -----------------------------------------------------------
say "-> Redis.CS::A::content::article_TIME" ;
for ( 1 .. $time_step )
{
	my $days_step = $_ - 1 ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
	say $key_day ;
	
	my $ref_user_version = get_user_version($redis_active,$key_day) ;
	my %temp ;
	my $sth_article = $dbh -> prepare("
                                        SELECT contentId,accountId,dashboardId,blockFlag
										FROM
										content
										WHERE
										dashboardType = 10 and deleteFlag = 0 and
										contentTime between '$key_day 00:00:00' and '$key_day 23:59:59'
									");
	$sth_article -> execute();
	
	while (my $ref = $sth_article -> fetchrow_hashref())
	{
		my $accountId = $ref -> {accountId} ;
		my $blockFlag = $ref -> {blockFlag} ;
		
		my $ref_account = get_gender_from_accountId($dbh , 'account' , $accountId) ;
		my $gender = $ref_account->{gender} ;
		my $l99NO  = $ref_account->{l99NO}  ;
		
		my $ref_temp = get_user_market2_from_accountId($dbh_v506 , $accountId) ;
		my $market = $ref_temp -> {market} ;
		
		my $version = $ref_user_version -> {$l99NO} ;
		
		$temp{'CS::A::content::article_' . $key_day} ++ ;
		$temp{'CS::A::content::article::block::' .$blockFlag. '_' . $key_day} ++ ;
		$temp{'CS::A::content::article::gender::' . $gender . '_' . $key_day} ++ ;
		$temp{'CS::A::content::article::market::' . $market . '_' . $key_day} ++ if $market ;
		$temp{'CS::A::content::article::version::'. $version. '_' . $key_day} ++ if $version;
		$temp{'CS::A::content::article::market::' . $market . '::version::' . $version. '_' . $key_day} ++ if $market && $version;
		
	}
	$sth_article -> finish ;
	
	insert_redis_hash(\%temp) ;
}

# --------------------------------------------------
# 用户发布照片
# --------------------------------------------------
say "-> Redis.CS::A::content::photo_TIME" ;
for ( 1 .. $time_step )
{
	my $days_step = $_ - 1 ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
	say $key_day ;
	
	my $ref_user_version = get_user_version($redis_active,$key_day) ;
	my %temp ;
	my $sth_photo = $dbh -> prepare("
                                        SELECT photoId,accountId,status
										FROM
										account_photo
										WHERE
										createTime between '$key_day 00:00:00' and '$key_day 23:59:59'
									");
	$sth_photo -> execute();
	
	while (my $ref = $sth_photo -> fetchrow_hashref())
	{
		my $accountId = $ref -> {accountId} ;
		my $status = $ref -> {status} ;
		
		my $ref_account = get_gender_from_accountId($dbh , 'account' , $accountId) ;
		my $gender = $ref_account->{gender} ;
		my $l99NO  = $ref_account->{l99NO}  ;
		
		my $ref_temp = get_user_market2_from_accountId($dbh_v506 , $accountId) ;
		my $market = $ref_temp -> {market} ;
		
		my $version = $ref_user_version -> {$l99NO} ;
		
		$temp{'CS::A::content::photo_' . $key_day} ++ ;
		$temp{'CS::A::content::photo::status::' . $status . '_' . $key_day} ++ ;
		$temp{'CS::A::content::photo::gender::' . $gender . '_' . $key_day} ++ ;
		$temp{'CS::A::content::photo::market::' . $market . '_' . $key_day} ++ if $market ;
		$temp{'CS::A::content::photo::version::'. $version. '_' . $key_day} ++ if $version;
		$temp{'CS::A::content::photo::market::' . $market . '::version::' . $version. '_' . $key_day} ++ if $market && $version;
		
	}
	
	$sth_photo -> finish ;
	
	insert_redis_hash(\%temp) ;
}

# ---------------------------------------------------------
# 用户发布评论
# ---------------------------------------------------------
say "-> Redis.CS::A::content::comment" ;
for ( 1 .. $time_step )
{
	my $days_step = $_ - 1 ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;

	say $key_day ;
	
	my $ref_user_version = get_user_version($redis_active,$key_day) ;
	my %temp ;
	my $sth_comment = $dbh -> prepare("
                                        SELECT commentId,accountId,dashboardId,status
										FROM
										nyx_comment.comment
										WHERE
										createTime between '$key_day 00:00:00' and '$key_day 23:59:59'
									");
	$sth_comment -> execute();
	
	while (my $ref = $sth_comment -> fetchrow_hashref())
	{
		my $accountId = $ref -> {accountId} ;
		my $status = $ref -> {status} ;
		
		my $ref_account = get_gender_from_accountId($dbh , 'account' , $accountId) ;
		my $gender = $ref_account->{gender} ;
		my $l99NO  = $ref_account->{l99NO}  ;
		
		my $ref_temp = get_user_market2_from_accountId($dbh_v506 , $accountId) ;
		my $market = $ref_temp -> {market} ;
		
		my $version = $ref_user_version -> {$l99NO} ;
		
		$temp{'CS::A::content::comment_' . $key_day} ++ ;
		$temp{'CS::A::content::comment::status::' . $status . '_' . $key_day} ++ ;
		$temp{'CS::A::content::comment::gender::' . $gender . '_' . $key_day} ++ ;
		$temp{'CS::A::content::comment::market::' . $market . '_' . $key_day} ++ if $market ;
		$temp{'CS::A::content::comment::version::'. $version. '_' . $key_day} ++ if $version;
		$temp{'CS::A::content::comment::market::' . $market . '::version::' . $version. '_' . $key_day} ++ if $market && $version;
	}
		
	$sth_comment -> finish ;
	
	insert_redis_hash(\%temp) ;
	
}

for ( 1 .. $num_month_ago)
{
	my $month_ago = $_ - 1 ;
	my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	say $month ;
	my %temp ;
	
	foreach($redis->keys( 'CS::A::content::*_'.$month.'-*' ) )
	{
		my $key = $_ ;
		if ($key =~ /^CS::A::content::(.*?)_\d+/)
		{
            my $type = $1 ;
			next if $type =~ /top/ ;
			next unless $type =~ /^article|^photo|^comment/ ;
			my $n = $redis->get($key) ;
			$temp{'CS::A::content::'.$type.'_'.$month} += $n ;
        }
        
	}
	
	insert_redis_hash(\%temp) ;
}
#=cut

#=pod

# ------------------------------------------------------------------------------------------------------
# 扫描床上礼物记录
# ------------------------------------------------------------------------------------------------------

say "-> MongoDB.cs.presents " ;


my $sth_present = $dbh -> prepare(
			"
			SELECT g.id,g.presentId,g.priceType,g.presentCount,p.name,g.fromId,g.toId,p.price,g.createTime
			FROM
			present_give g left join present p
			on g.presentId = p.id
			WHERE
			g.createTime > '$day_start'");

$sth_present -> execute();
while (my $ref = $sth_present -> fetchrow_hashref())
{
	my $pId = $ref -> {id} ;
	next if $collection_present->find_one({ pId => $pId }) ;
	
	my $presentId    = $ref -> {presentId} ;
	my $presentName  = decode_utf8 $ref -> {name} ;
	my $presentPrice = $ref -> {price} ;
	my $priceType    = $ref -> {priceType} ;
	my $fromId       = $ref -> {fromId} ;
	my $toId         = $ref -> {toId} ;
	my $count        = $ref -> {presentCount} ;
	my $time         = $ref -> {createTime} ;
	#$time =~ s/ /_/ ;
	#
	# 根据 accountId 查用户信息
	my %accountIds ;
	foreach ($fromId,$toId)
	{
		my $accountId = $_ ;
		eval{
			my $ref_account = get_user_from_accountId($dbh_v506,$accountId) ;
                        my ($l99NO,$name) = ($ref_account->{l99NO} , $ref_account->{name}) ;
			my $name_utf8 = decode_utf8 $name ;
			$accountIds{$accountId} = [$l99NO,$name_utf8] ;
		};
	}
	
	$collection_present -> insert(
		{
			"pId" => $pId , "presentId" => $presentId , "presentName" => $presentName ,
			"presentPrice" => $presentPrice , "priceType" => $priceType ,
			"from_id" => $fromId , "from_l99NO" => $accountIds{$fromId}->[0] , "from_name" => $accountIds{$fromId}->[1] ,
			"toId" => $toId , "to_l99NO" => $accountIds{$toId}->[0] , "to_name" => $accountIds{$toId}->[1] ,
			"count" => $count , "time" => $time
		}
	);
}
$sth_present -> finish ;
#=cut

#=pod
# 这里是取机器人帐号，后面的礼物赠送等统计要排除机器人
my %ids_robot;
my $sth_robot = $dbh -> prepare("SELECT robotId,userInfo FROM nyx_robot") ;
$sth_robot -> execute();
while (my $ref = $sth_robot -> fetchrow_hashref())
{
	my $accountId = $ref -> {robotId} ;
	#my $l99NO = $ref -> {longNo} ;
	$ids_robot{$accountId} = 1 ;
}
$sth_robot -> finish ;


# ------------------------------------------------------------------------
# CS::A::礼物赠送次数
# ------------------------------------------------------------------------
say "-> Redis.CS::A::payin::present" ;
for ( 1 .. $time_step + 1)
{
	my $days_step = $_ - 1 ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;

	my $time_start = "$key_day 00:00:00" ;
	my $time_end   = "$key_day 23:59:59" ;

	my %present_info ;
	my %present_market ;

	my $ref_user_version = get_user_version($redis_active,$key_day) ;
	
	my $data_present = $collection_present -> find({'time' => { '$gte' => $time_start , '$lte' => $time_end }}); 
	while (my $ref = $data_present -> next)
	{
		
		my $fromId = $ref -> {from_id} ;
		next if exists $ids_robot{$fromId} ;			# 机器人送的 不参与统计
		
		my $presentId = $ref -> {presentId} ;
		my $presentPrice = $ref -> {presentPrice} ;
		my $presentName = $ref -> {presentName} ;
		my $presentCount = $ref -> {count} ;
		$presentName =~ s/\s+$//g ;
		
		# 取用户渠道
		my $ref_account_l99NO = get_user_from_accountId($dbh_v506,$fromId) ;
		my $l99NO = $ref_account_l99NO->{l99NO} ;
		
		my $version = $ref_user_version -> {$l99NO} ;
		
		#my $market = $redis_market->get('STORM::CS::user::market::'.$l99NO) ;	
		my $ref_temp = get_user_market2_from_accountId($dbh_v506,$fromId) ;
		my $market = $ref_temp -> {market} ;
		
		my $ref_account = get_user_market_from_accountId($dbh_v506,$fromId) ;
        my $os = $ref_account -> {market} ;
		
		$present_info{$presentId}{price} = $presentPrice ;
		$present_info{$presentId}{name}  = $presentName ;
		
		$present_info{$presentId}{count} += $presentCount ;
		$present_info{$presentId}{$os}   += $presentCount if $os;
		
		$present_market{'present::'.$presentId.'::market::'.$market}   += $presentCount if $market ;
		$present_market{'present::'.$presentId.'::version::'.$version} += $presentCount if $version;
	}

	foreach (%present_info)
	{
		my $presentId = $_ ;
		my $count = $present_info{$presentId}{count} ;
		next unless $count > 0 ;
		my $name  = $present_info{$presentId}{name} ;
		my $price = $present_info{$presentId}{price} ;
		
		my $count_os      = $present_info{$presentId}{ios} ;
		my $count_android = $present_info{$presentId}{android} ;
		
		my %temp = ('name' => $name , 'price' => $price , 'count' => $count) ;
		my $present_temp = encode_json \%temp;
		my %temp_ios = ('name' => $name , 'price' => $price , 'count' => $count_os) ;
		my $present_temp_ios = encode_json \%temp_ios;
		my %temp_android = ('name' => $name , 'price' => $price , 'count' => $count_android ) ;
		my $present_temp_android = encode_json \%temp_android;
		
		#say 'CS::A::payin::present::'.$presentId.'_'.$key_day , "\t" , $present_temp if $presentId == 8;
		insert_redis_scalar('CS::A::payin::present::'.$presentId.'_'         .$key_day , $present_temp) ;
		insert_redis_scalar('CS::A::payin::present::'.$presentId.'::ios_'    .$key_day , $present_temp_ios) if $count_os;
		insert_redis_scalar('CS::A::payin::present::'.$presentId.'::android_'.$key_day , $present_temp_android) if $count_android;
	}
	foreach (%present_market)
	{
		my $present_market = $_ ;	# present::55::market::360
		my ($presentId) = $present_market =~ /present::(\d+)::/ ;
		my $name  = $present_info{$presentId}{name} ;
		my $price = $present_info{$presentId}{price} ;
		my $count = $present_market{$present_market} ;
		
		my %temp = ('name' => $name , 'price' => $price , 'count' => $count) ;
		my $present_temp = encode_json \%temp;
		insert_redis_scalar('CS::A::payin::'.$present_market.'_'.$key_day , $present_temp) if $count;
	}
}

for ( 1 .. $num_month_ago)
{
	my $month_ago = $_ - 1 ;
	my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
    
	my %present_info ;
	my %month_temp ;
	
	foreach($redis->keys( 'CS::A::payin::present::*_'.$month.'-*' ) )
	{
		my $key = $_ ;
		
		if ($key =~ /^CS::A::payin::(present::\d+.*?)_(\d+-\d+)-\d+$/ )
		{
			my $v = $redis -> get($key) ;
			my ($type,$month) = ($1,$2) ;
			my ($presentId) = $type =~ /present::(\d+)/ ;
			
			my ($count) = $v =~ /"count":(\d+)/  ;
			my ($name)  = $v =~ /"name":"(.*?)"/ ;
			my ($price) = $v =~ /"price":"(.*?)"/ ;
			
			$present_info{$presentId}{name}  = $name ;
			$present_info{$presentId}{price} = $price ;
			
			$month_temp{$type} += $count ;
		}
	}
		
	foreach (%month_temp)
	{
		my $type = $_ ;
		my ($presentId) = $type =~ /present::(\d+)/ ;
		
		my $count = $month_temp{$type} ;
		next unless $count > 0 ;
		my $name  = $present_info{$presentId}{name} ;
		my $price = $present_info{$presentId}{price} ;
		
		my %temp = ('name' => $name , 'price' => $price , 'count' => $count) ;
		my $v_temp = decode_utf8 encode_json \%temp;
		
		insert_redis_scalar('CS::A::payin::'.$type.'_'.$month , $v_temp) ;
	}
	
}


#=cut

#=pod
# ----------------------------------------------------------------------------------------------
# 床上用户消费记录(这里不含商城收入，在后面另算)
# 1：充值  2：冻结资金  3：支付  4：龙币转立方币  5：购买魔法头像  6：购买
# 7：购买PK权利卡  8：赚取  9：其它  10：充值  11：商城消费  12：商城退款
# 13：商城返利  14：商家结算  15：龙币兑现
# 16：床上购买礼物  17：床上购买推荐榜单位  18：床上购买贴纸  19：床上购买道具
# 20：床上购买VIP  21：床上购买置顶位  22：发红包  23：购买床点
# 以上是 v506.pay_account_log_type 表的说明，事实上，还是以代码为准...
# ----------------------------------------------------------------------------------------------

say "-> Redis.CS::payin::[vip/top/...] " ;

my $sth_pay = $dbh_v506 -> prepare(" SELECT logId,accountId,userMoney,changeType,changeDesc,changeTime
				  FROM
				  pay_account_log
				  WHERE
				  changeType in (5,8,16,17,18,19,20,21,25) and changeTime > $timestamp_start") ;
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
	
	my $pay_type ;
	if ($changeType == 8 )						# 8的情况较多，API用string标记比较蛋疼
	{
		$pay_type = 'present::fail'  if $changeDesc =~ /礼物过期/ ;
		$pay_type = 'present::back'  if $changeDesc =~ /礼物用户提成/ ;
		$pay_type = 'hotlist::fail'  if $changeDesc =~ /未通过/ ;
		$pay_type = 'recharge'       if $changeDesc =~ /床上充值/ ;
		$pay_type = 'recharge::back' if $changeDesc =~ /龙币充值返赠/ ;
	}
	elsif($changeType == 5)
	{
		$pay_type = 'bedpoint' if $changeDesc =~ /龙币转床点/ ;
	}	
	elsif($changeType == 16){ $pay_type = 'present' }		# 礼物
	elsif($changeType == 17){ $pay_type = 'hotlist' }		# 榜单位
	elsif($changeType == 18){ $pay_type = 'tiezhi' }		# 擦擦的贴纸
	elsif($changeType == 19){ $pay_type = 'daoju' }			# 擦擦的道具
	elsif($changeType == 20){ $pay_type = 'vip' }			# vip
	elsif($changeType == 21){ $pay_type = 'top' }			# 置顶位
	elsif($changeType == 25)					# 系统给马甲充值
	{
		$pay_type = 'recharge::service'       if $changeDesc =~ /客服充值\s*$/ ;
		$pay_type = 'recharge::servicegiving' if $changeDesc =~ /客服充值赠送\s*$/ ;
		$pay_type = 'recharge::majia'         if $changeDesc =~ /马甲充值\s*$/ ;
		$pay_type = 'recharge::test'          if $changeDesc =~ /测试账号充值\s*$/ ;
	}
	
	my $redis_key = 'CS::payin::'.$pay_type.'::user::'.$accountId . '::uuid::' . $logId . '_' .$time ;
	$redis -> exists( $redis_key ) && next ;
	insert_redis_scalar( $redis_key => $userMoney ) if $pay_type;
	
}
$sth_pay -> finish ;
#=cut

#=pod
# ----------------------------------------------------------------------------------------------
# 床点消费
# 1：充值  2：充值  3：送红包  4：获得床点  5：龙币转床点  6：退还床点
# 7：购买置顶位  8：龙币兑换床点  9：附近排行榜  10：床点礼物提成  11：购买礼物  12：体验新钱包送床点
# ----------------------------------------------------------------------------------------------
my $sth_pay_point = $dbh_v506 -> prepare("
					SELECT logId,accountId,bedMoney,changeTime,changeDesc,changeType 
					FROM
					bed_pay_account_log
					WHERE
					changeTime > $timestamp_start
					 ") ;
$sth_pay_point -> execute();
while (my $ref = $sth_pay_point -> fetchrow_hashref())
{
        my $logId      = $ref -> {logId} ;
        my $accountId  = $ref -> {accountId} ;
        my $point      = $ref -> {bedMoney} ;
        my $changeType = $ref -> {changeType} ;
        my $changeDesc = decode_utf8 $ref -> {changeDesc} ;
        my $changeTime = $ref -> {changeTime} ;
        my $time = unix2local($changeTime) ;
        $time =~ s/ /_/ ;

        my $pay_type ;
        if   ($changeType == 3 ){ $pay_type = 'bedpoint::red' }
        elsif($changeType == 4 )
        {
                #$pay_type = 'bedpoint::add' ;
                $pay_type = 'bedpoint::add::present'  if $changeDesc =~ /送礼物得床点/ ;
                $pay_type = 'bedpoint::add::recharge' if $changeDesc =~ /充值赠送/ ;
				$pay_type = 'bedpoint::add::red'      if $changeDesc =~ /红包赠送/ ;
                $pay_type = 'bedpoint::add::login'    if $changeDesc =~ /登录奖励/ ;
				
				$pay_type = 'bedpoint::add::greenhands::info'      if $changeDesc =~ /新手任务.*?完善个人信息/ ;
				$pay_type = 'bedpoint::add::greenhands::photo'     if $changeDesc =~ /新手任务.*?相册照片/ ;
				$pay_type = 'bedpoint::add::greenhands::present'   if $changeDesc =~ /新手任务.*?送礼物/ ;
				$pay_type = 'bedpoint::add::greenhands::mood'      if $changeDesc =~ /新手任务.*?发心情/ ;
				$pay_type = 'bedpoint::add::greenhands::recharge'  if $changeDesc =~ /新手任务.*?首次充值/ ;
				$pay_type = 'bedpoint::add::greenhands::phone'     if $changeDesc =~ /新手任务.*?手机号/ ;
				
        }
        elsif($changeType == 5 ){ $pay_type = 'bedpoint::fromlong' }		
        elsif($changeType == 6 ){ $pay_type = 'bedpoint::back' }		
        elsif($changeType == 7 ){ $pay_type = 'bedpoint::top' }
        elsif($changeType == 9 ){ $pay_type = 'bedpoint::nearbylist' }		
        elsif($changeType == 10){ $pay_type = 'bedpoint::present::back' }
        elsif($changeType == 11){ $pay_type = 'bedpoint::present' }	
        elsif($changeType == 12){ $pay_type = 'bedpoint::wallet' }
	
        my $redis_key = 'CS::payin::'.$pay_type.'::user::'.$accountId . '::uuid::' . $logId . '_' .$time ;
        $redis -> exists( $redis_key ) && next ;
        insert_redis_scalar( $redis_key => $point ) if $pay_type;
	
}

#=cut

#=pod
# -----------------------------------------------------------------------------------
# 床上商城走L06的商城API，在这里单独统计，牵扯较多的业务逻辑，这里注明一下
# sourceType可能值为0，1，2   -- 0表示来自web端，1表示来自猜比赛，2表示来自床上
#
# ordersStatus字段表示订单的状态，可能取值如下(其中1、2、3、20、100都表示订单已经支付)：
#
# public static final Integer NOT_PAID = 0;			# /**买家未付款。*/
# public static final Integer TO_BE_SHIPPED = 1;		# /**买家已付款，等待卖家发货。*/
# public static final Integer TO_BE_RECEIVED = 2;		# /**卖家已发货，等待买家确认收货。*/
# public static final Integer RECEIVED = 3;			# /**买家已确认收货。*/
# public static final Integer TO_BE_RETURNED = 20;		# /**买家申请退货。*/
# public static final Integer FINISHED = 100;			# /**订单已结束，可参与结算。*/
# public static final Integer CANCELED = 10;			# /**订单被取消。*/
# public static final Integer RETURNED = -1;			# /**订单已退货，可参与结算。*/
# ------------------------------------------------------------------------------------
say "-> CS::payin::mall::user " ;
my $sth_pay_mall = $dbh_v506 -> prepare("
					SELECT ordersId,accountId,ordersAmount,shipPrize,ordersStatus,createTime
					FROM
					mall_orders_header
					WHERE
					sourceType = 2 and ordersStatus in (1,2,3,20,100) and createTime > '$day_start'
					") ;
$sth_pay_mall -> execute();
while (my $ref = $sth_pay_mall -> fetchrow_hashref())
{
	my $ordersId  = $ref -> {ordersId} ;
	my $accountId = $ref -> {accountId} ;
	my $goods   = $ref -> {ordersAmount} ;		# 商品价格
	my $postage = $ref -> {shipPrize} ;			# 邮费
	my $status  = $ref -> {ordersStatus} ;		# 订单状态
	my $time    = $ref -> {createTime} ;		# 订单生成时间
	$time =~ s/ /_/ ;

	my $redis_key = 'CS::payin::mall::user::' . $accountId . '::uuid::' . $ordersId . '_' .$time ;

	$redis -> exists( $redis_key ) && next ;
	
	my $redisvalue = $goods + $postage ;
	insert_redis_scalar( $redis_key , $redisvalue ) ;
}
$sth_pay_mall -> finish ;

say "-> CS::payin::mall::back::user::id_TIME " ;
my $sth_pay_mall_back = $dbh_v506 -> prepare("
						SELECT ordersId,accountId,ordersAmount,cancelTime 
						FROM 
						mall_orders_header 
						WHERE 
						cancelTime > '$day_start' and 
						ordersStatus = 10 and sourceType = 2 and paymentTime is not null
					     ") ;
$sth_pay_mall_back -> execute();
while (my $ref = $sth_pay_mall_back -> fetchrow_hashref())
{
	my $ordersId  = $ref -> {ordersId} ;
	my $accountId = $ref -> {accountId} ;
	my $goods   = $ref -> {ordersAmount} ;
	my $time    = $ref -> {cancelTime} ;	
	$time =~ s/ /_/ ;

	my $redis_key = 'CS::payin::mall::back::user::'.$accountId. '::uuid::' . $ordersId . '_' .$time ;
	$redis -> exists( $redis_key ) && next ;
	insert_redis_scalar( $redis_key , $goods ) ;
}
$sth_pay_mall_back -> finish ;




# ----------------------------------------------------------------------------------
# 积分消费
# ----------------------------------------------------------------------------------
say "-> Redis.CS::payin::point::item::N::user::N::uuid::N_TIME" ;
for ( 1 .. $time_step + 1 )
{
	my $days_step = $_ - 1 ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;

	my $sth_point_mall = $dbh -> prepare("
											SELECT orderId,accountId,itemId,amount,createTime
											FROM
											mall_order
											WHERE
											createTime between '$key_day 00:00:00' and '$key_day 23:59:59'
										") ;
	$sth_point_mall -> execute();
	while (my $ref = $sth_point_mall -> fetchrow_hashref())
	{
		my $orderId   = $ref -> {orderId} ;
		my $accountId = $ref -> {accountId} ;
		my $itemId    = $ref -> {itemId} ;
		my $num       = $ref -> {amount} ;
		my $time      = $ref -> {createTime} ;
		$time =~ s/ /_/ ; 
		
		my $redis_key = 'CS::payin::point::item::'.$itemId.'::user::'.$accountId.'::uuid::'.$orderId.'_'.$time;
		$redis -> exists( $redis_key ) && next ;
	
		insert_redis_scalar( $redis_key , $num ) ;
		
	}
	$sth_point_mall -> finish ;
	
}
$dbh -> disconnect ;
#=cut

#=pod
# ---------------------------------------------------------------------------------------------------
# 用户消费统计  CS::A::payin
# ---------------------------------------------------------------------------------------------------
say "-> Redis.CS::A::payin*_DAY" ;
for ( 1 .. $time_step )
{
	my $days_step = $_ - 1 ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
	my ($month) = $key_day =~ /^(\d+-\d+)-\d+$/ ;
	
	my %pay ;			# 每天的消费
	my %pay_user ;
	
	my $ref_user_version = get_user_version($redis_active,$key_day) ;
	say "Get all user-version on $key_day ... " ;
	
	foreach($redis->keys( 'CS::payin*::user::*_'.$key_day.'*' ) )
	{
		my $key = $_ ;		
		my $pay ;
		
		my $pay_type ;
		my $id ;
		my $hour ;
		
		if ($key =~ /^CS::payin::(.+)::user::(\d+)::uuid::\d+_[-\d]+_(\d+):/ )
		{
			$pay = $redis->get($key);
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
		my $ref_account_l99NO = get_user_from_accountId($dbh_v506,$id) ;
		my $l99NO = $ref_account_l99NO->{l99NO} ;
		#my $market = $redis_market->get('STORM::CS::user::market::'.$l99NO) ;	
		my $ref_temp = get_user_market2_from_accountId($dbh_v506,$id) ;
		my $market = $ref_temp -> {market} ;
		
		my $version = $ref_user_version -> {$l99NO} ;
		
		# 获取用户的系统
		my $ref_account = get_user_market_from_accountId($dbh_v506,$id) ;
        my $os = $ref_account -> {market} ;
		
		$pay{'CS::A::payin::'.$pay_type.'_'.$key_day  } += $pay ;
		$pay{'CS::A::payin::'.$pay_type.'::hour'.$hour.'_'.$key_day  } += $pay ;
		$pay{'CS::A::payin::'.$pay_type.'::'.$os.'_'.$key_day  } += $pay if $os ;
		$pay{'CS::A::payin::'.$pay_type.'::market::' . $market .'_'.$key_day  } += $pay if $market ;
		$pay{'CS::A::payin::'.$pay_type.'::version::'.$version .'_'.$key_day  } += $pay if $version ;
		$pay{'CS::A::payin::'.$pay_type.'::market::'.$market.'::version::'.$version.'_'.$key_day  } += $pay if $market && $version ;
		
		$pay_user{'CS::A::payin::'.$pay_type.'::user::'.$id.'_'.$key_day}  += $pay ;
		
		if ($pay_type !~ /point::/)
		{
			$redis -> sadd( 'CS::payin::uv::hour'.$hour.'_'.$key_day  , $id ) ;
			$redis -> sadd( 'CS::payin::uv::hour'.$hour.'_'.$month    , $id ) ;
			
			$redis -> sadd( 'CS::payin::uv_'.$key_day  , $id ) ;
			$redis -> sadd( 'CS::payin::uv_'.$month    , $id ) ;
			$redis -> sadd( 'CS::payin::uv' , $id ) ;
			
			$redis -> sadd( 'CS::payin::uv::'.$os.'_'.$key_day  , $id ) if $os ;
			$redis -> sadd( 'CS::payin::uv::'.$os.'_'.$month    , $id ) if $os ;
			$redis -> sadd( 'CS::payin::uv::market::'.$market.'_'.$key_day    , $id ) if $market ;
			$redis -> sadd( 'CS::payin::uv::market::'.$market.'_'.$month      , $id ) if $market ;
			$redis -> sadd( 'CS::payin::uv::version::'.$version.'_'.$key_day  , $id ) if $version ;
			$redis -> sadd( 'CS::payin::uv::version::'.$version.'_'.$month    , $id ) if $version ;
			$redis -> sadd( 'CS::payin::uv::market::'.$market.'::version::'.$version.'_'.$key_day , $id ) if $market && $version ;
			$redis -> sadd( 'CS::payin::uv::market::'.$market.'::version::'.$version.'_'.$month   , $id ) if $market && $version ;
			
			$redis -> sadd( 'CS::payin::uv::'.$os , $id ) if $os ;
			$redis -> sadd( 'CS::payin::uv::market::'.$market , $id ) if $market ;
		}
		elsif($pay_type =~ /^point::/)
		{
			$pay{'CS::A::payin::point::times_'.$key_day } ++ ;
			$pay{'CS::A::payin::'.$pay_type.'::times_'.$key_day } ++ ;
			
			$redis -> sadd( 'CS::payin::'.$pay_type.'::uv_'.$key_day , $id ) ;
			$redis -> sadd( 'CS::payin::'.$pay_type.'::uv_'.$month   , $id ) ;
			
			$redis -> sadd( 'CS::payin::point::uv_'.$key_day , $id ) ;
			$redis -> sadd( 'CS::payin::point::uv_'.$month   , $id ) ;
			$redis -> sadd( 'CS::payin::point::uv' , $id ) ;
		}
		
	}
	
	foreach( keys %pay_user )
	{
		my $key = $_ ;				# 'CS::A::payin::'.$pay_type.'::user::'.$id.'_'.$key_day
		my $pay = $pay_user{$key} ;
		my ($accountId) = $key =~ /::user::(\d+)_/ ;
		
		if ($key =~ /payin::bedpoint/)
		{
            $redis->set($key , $pay) ;
        }
		else
		{
			my $ref_account = get_user_from_accountId($dbh_v506,$accountId) ;
			my $l99NO = $ref_account->{l99NO} ;
			my $name  = $ref_account->{name} ;
			$redis->set($key , $pay.','.$l99NO.','.$name) ;
		}
        
		
	}
	
	insert_redis_hash(\%pay) ;
	
	foreach( $redis->keys( 'CS::payin*:uv*_'.$key_day ) )
	{
		my $k = $_ ;
		if ($k =~ /^CS::(payin.*?uv.*?)_/ )
		{
			my $type = $1 ;
			my $count = $redis->scard($k) ;
			insert_redis_scalar( 'CS::A::'.$type.'_'.$key_day , $count ) ;
		}
	}
}

#=cut

#=pod

say "-> Redis.CS::A::payin*_MONTH" ;

for ( 1 .. $num_month_ago)
{
	my $month_ago = $_ - 1 ;
	my $month      = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	
	foreach( $redis->keys( 'CS::payin*:uv*_'.$month ) )
	{
		my $k = $_ ;
		if ($k =~ /^CS::(payin.*?uv.*?)_/ )
		{
			my $type = $1 ;
			my $count = $redis->scard($k) ;
			insert_redis_scalar( 'CS::A::'.$type.'_'.$month , $count ) ;
		}
	}
}

for ( 1 .. $num_month_ago)
{
	my $month_ago = $_ - 1 ;
	my $month      = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
    say $month ;
	
	my %pay ;
	my %pay_user ;
	my $pay_type ;
	
	foreach($redis->keys( 'CS::A::payin::*::user::*_'.$month.'-*' ) )
	{
		my $key = $_ ;
		next if $key =~ /bedpoint::wallet/ ;
		if ($key =~ /^CS::A::payin::(.+)::user::(\d+)_[-\d]+/ )
		{
			$pay_type = $1 ;
			my $id = $2 ;
			my $info = $redis->get($key) ;
			my ($pay,$l99NO,$name) = split /,/ , $info ;
			$pay_user{'CS::A::payin::'.$pay_type.'::user::'.$id.'_'.$month} += $pay ;
		}
	}
	foreach( keys %pay_user )
	{
		my $key = $_ ;
		my $pay = $pay_user{$key} ;
		if ($key =~ /payin::bedpoint/)
		{
            $redis->set($key , $pay) ;
        }
		else
		{
			my ($accountId) = $key =~ /::user::(\d+)_/ ;
		
			my $ref_account = get_user_from_accountId($dbh_v506,$accountId) ;
			my $l99NO = $ref_account->{l99NO} ;
			my $name  = $ref_account->{name} ;
			$redis->set($key , $pay.','.$l99NO.','.$name) ;
		}
	}
	
	foreach($redis->keys( 'CS::A::payin::[^bu]*_'.$month.'-*' ) )
	{
		my $key = $_ ;
		next if $key =~ /::user::/ ;
		if ($key =~ /^CS::A::payin::(.*?)_[-\d]+/ )
		{
			my $pay = $redis->get($key) ;
			$pay_type = $1 ;
			next if $pay_type =~ /uv/ ;
			$pay{'CS::A::payin::'.$pay_type.'_'.$month } += $pay ;
		}
	}
	foreach($redis->keys( 'CS::A::payin::bedpoint*_'.$month.'-*' ) )
	{
		my $key = $_ ;
		next if $key =~ /::user::/ ;
		if ($key =~ /^CS::A::payin::(.*?)_[-\d]+/ )
		{
			my $pay = $redis->get($key) ;
			$pay_type = $1 ;
			next if $pay_type =~ /uv/ ;
			$pay{'CS::A::payin::'.$pay_type.'_'.$month } += $pay ;
		}
	}
	
	insert_redis_hash(\%pay) ;


}

insert_redis_scalar( 'CS::A::payin::uv'          , $redis->scard('CS::payin::uv') ) ;
insert_redis_scalar( 'CS::A::payin::uv::ios'     , $redis->scard('CS::payin::uv::ios') ) ;
insert_redis_scalar( 'CS::A::payin::uv::android' , $redis->scard('CS::payin::uv::android') ) ;

#=cut

`rm -f /tmp/kkkk` ;

# =========================================  functions  ==========================================

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
		#say "$key => $value" ;
    }
}

# -----------------------------------------------------------------------------------------
# 根据用户 accountId 获取用户[userl99No,username,status]  -- TABLE:account
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

sub get_gender_from_accountId
{
	my ($dbh,$table,$accountId) = @_ ;
	my $ref_accountId ;
	my $sth_user = $dbh -> prepare(" SELECT accountId,gender,longNO FROM $table WHERE accountId = $accountId ");
    $sth_user -> execute();
    while (my $ref = $sth_user -> fetchrow_hashref())
    {
        	my $accountId  = $ref -> {accountId} ;
        	my $gender     = $ref -> {gender} ;
			my $l99NO      = $ref -> {longNO} ;
			$ref_accountId -> {gender}  = $gender ;
			$ref_accountId -> {l99NO}   = $l99NO ;
	}
	$sth_user -> finish ;
	return $ref_accountId ;
}

sub get_accountId_from_l99NO
{
	my ($dbh,$l99NO) = @_ ;
	my $accountId ;
	my $sth = $dbh -> prepare(" SELECT accountId FROM account WHERE l99NO = $l99NO ") ;
	$sth -> execute();
	while (my $ref = $sth -> fetchrow_hashref())
	{
		$accountId = $ref -> {accountId};
	}
	$sth -> finish ;
	return $accountId ;
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

# --------------------------------------------------------------------------------------------
# 根据 lat&lng 取 countryId_province
# 这里用的是扫本地库POI的方式来判定，虽然正确率不会达到严格意义的100%，但是不用走webAPI，可持续性更好
# 本算法的正确率依赖于本地数据库中POI的分布密度与POI省份字段的正确率，目前大致可用
# --------------------------------------------------------------------------------------------
sub getProvince
{
    my ($lat_cs,$lng_cs) = @_ ;
    return 0 if $lat_cs == 0 && $lng_cs == 0 ;
    my $lat = substr($lat_cs,0,10) ;
    my $lng = substr($lng_cs,0,10) ;
    my $step = 0.003 ;
    my %city ;
    while (! ~~ keys %city)
    {
        my $sql = "SELECT cityId FROM townfile_local WHERE lat > $lat - $step and lat < $lat + $step and lng > $lng - $step and lng < $lng + $step ;" ;
        my $sth = $dbh_wwere -> prepare($sql);
        $sth -> execute();
        while (my $ref = $sth -> fetchrow_hashref()){
            my $cityId = $ref -> {'cityId'} ;
            $city{$cityId} ++ ;
        }
        $step += 0.01;
    }
    my ($countryId,$province) ;
    my ($cityId) = map {$_} sort {$city{$b} <=> $city{$a}} keys %city ;
    
    my $sth_city = $dbh_wwere -> prepare("SELECT countryId,province FROM city where cityId = $cityId") ;
    $sth_city -> execute();
    while (my $ref = $sth_city -> fetchrow_hashref()){
	$countryId = $ref -> {countryId} ;
	$province  = $ref -> {province} ;
    }
    $sth_city -> finish() ;
    return $countryId.'_'.$province ;
}
