#!/usr/bin/env perl 
# ==============================================================================
# Author: 	    kiwi
# createTime:	2014.10.31
# ==============================================================================
use 5.10.1 ;
use utf8 ;
use Data::Dumper ;
use MaxMind::DB::Reader ;
use DBI ;
use Redis;
use MongoDB ;
use Config::Tiny ;
use JSON::XS ;
use Time::Local ;
use POSIX qw(strftime);
use Unicode::UTF8 qw[decode_utf8 encode_utf8];
binmode(STDIN, ':encoding(utf8)');
binmode(STDOUT, ':encoding(utf8)');
binmode(STDERR, ':encoding(utf8)');
# ----------------------------------------------------------------------


# -----------------------------------------------------------------------
# read config.ini
# -----------------------------------------------------------------------
my $Config = Config::Tiny->new;
$Config = Config::Tiny->read( '/home/DA/DataAnalysis/config.ini', 'utf8' );
my $version = $Config -> {_} -> {version};

my $redis_host = $Config -> {REDIS} -> {host};
my $redis_port = $Config -> {REDIS} -> {port};
my $mongo_host = $Config -> {MONGODB} -> {host};
my $mongo_port = $Config -> {MONGODB} -> {port};

my $log_dir_FT = $Config -> {FT} -> {log_dir} ;         # 第一次 APP应用日志路径
my $file_team  = $Config -> {FT} -> {team_file} ;	    # 公司同事的龙号/acoountId 文本

my $L06_host     = $Config -> {L06_DB} -> {host};
my $L06_db       = $Config -> {L06_DB} -> {database} ;
my $L06_usr      = $Config -> {L06_DB} -> {username};
my $L06_password = $Config -> {L06_DB} -> {password};

my $FT_host      = $Config -> {FT_DB} -> {host};
my $FT_db        = $Config -> {FT_DB} -> {database} ;
my $FT_usr       = $Config -> {FT_DB} -> {username} ;
my $FT_password  = $Config -> {FT_DB} -> {password};

my $time_step = $Config -> {time} -> {step} ;		    # 设置为往前推 N天 统计，默认 N = 1 ;
my $timestamp_now = int scalar time ;
my $timestamp_start = $timestamp_now - 86400 * $time_step  ;
my $day_yest  = strftime("%Y-%m-%d",localtime(time() - 86400 ));
my $day_start = strftime("%Y-%m-%d 00:00:00",localtime(time() - 86400 * $time_step));

# ---------------------------------------------------------------------
# connect to mysql
# ---------------------------------------------------------------------
my $dsn_v506 = "DBI:mysql:database=$L06_db;host=$L06_host" ;
my $dbh_v506 = DBI -> connect($dsn_v506, $L06_usr, $L06_password, {'RaiseError' => 1} ) or die "Can't connect: ", DBI->errstr;
$dbh_v506 -> do ("SET NAMES UTF8");

my $dsn_FT = "DBI:mysql:database=$FT_db;host=$FT_host" ;
my $dbh_FT = DBI -> connect($dsn_FT, $FT_usr, $FT_password, {'RaiseError' => 1} ) or die "Can't connect: ", DBI->errstr;
$dbh_FT -> do ("SET NAMES UTF8");

# ---------------------------------------------------------------------
# connect to Redis & mongoDB
# ---------------------------------------------------------------------
my $redis    = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
my $redis_ip = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
$redis_ip -> select(1) ;
my $mongo = MongoDB::MongoClient->new(host => $mongo_host , port => $mongo_port);

# ---------------------------------------------------------------------
my %num2month = (
	'01' => "Jan" , '02' => "Feb" , '03' => "Mar" , '04' => "Apr" ,
	'05' => "May" , '06' => "Jun" , '07' => "Jul" , '08' => "Aug" ,
	'09' => "Sep" , '10' => "Oct" , '11' => "Nov" , '12' => "Dec"
);
my %month2num = reverse %num2month ;


# ----------------------------------------------------------------------
my @team ;
open my $fh_team , "<:utf8" , $file_team ;
while (<$fh_team>) {
    s/[\s\r\n]//g ;
    my ($l99NO,$accountId) = split ',' , $_ ;
    push @team , $accountId ;
}
my %majias = %{get_majia($dbh_FT,'ft_majia')} ;
$majias{$_} = 1 for @team ;

#=pod
# ----------------------------------------------------------------------------
# FT中话题的记录 
# ----------------------------------------------------------------------------
say "-> Redis.FT::content::topic::N_TIME " ;

my $sth_topic = $dbh_FT -> prepare(" SELECT * FROM ft_topic ") ;
$sth_topic -> execute();
while (my $ref = $sth_topic -> fetchrow_hashref())
{
    my %topic ;
    my $id = $ref -> {id} ;
    my $status = $ref -> {status} ;
    next unless $status == 1 ;
    $redis -> exists('FT::content::topic::'.$id.'_'.$day_yest ) && next ;
    $topic{id} = $id;
    $topic{name} = decode_utf8 $ref -> {topic_name} ;
    $topic{hot}  = $ref -> {visit_num} ;
    $topic{createTime} = $ref -> {create_time} ;
    $topic{image_prefix} = $ref -> {image_prefix} ;
    
    my $content_num = 0;
    my $sth_content = $dbh_FT -> prepare(" SELECT accountId FROM ft_content WHERE typeId = $id and deleteFlag = 0 and permissionType = 40 ") ;
    $sth_content -> execute();
    while (my $ref = $sth_content -> fetchrow_hashref())
    {
	my $accountId = $ref -> {accountId} ;
	$content_num ++ unless exists $majias{$accountId} ;
    }
    $topic{content_num} = $content_num ;
    
    my $topic_info = encode_json \%topic;
    insert_redis_scalar('FT::content::topic::'.$id.'_'.$day_yest  , $topic_info );
}
$sth_topic -> finish ;
#=cut

# -----------------------------------------------------------------------------
# 新增用户 记录 (存储于 MongoDB)
# -----------------------------------------------------------------------------
#=pod

say "-> MongoDB.ft.user " ;

my $database   = $mongo -> get_database( 'ft' );
my $collection = $database -> get_collection( 'user' );
	
my $sth_user = $dbh_v506 -> prepare("
					SELECT * FROM
					account_log
					WHERE
					createTime > '$day_start' and market like '%irstTime%' 
				    ");
$sth_user -> execute();
while (my $ref = $sth_user -> fetchrow_hashref())
{
	my $accountId  = $ref -> {accountId} ;
	next if $collection->find_one({ accountId => $accountId }) ;
	
	my $createTime    = $ref -> {createTime} ;
	my $machineCode   = $ref -> {machineCode} ;
	my $ip            = $ref -> {accountIP} ;
	my $market_raw    = $ref -> {market} ;
	
	my $ref_machine = get_user_from_mid($dbh_FT,$machineCode) ;
	my ($market,$client,$active_time,$version) = (
						      $ref_machine->{market},
						      $ref_machine->{client},
						      $ref_machine->{active_time},
						      $ref_machine->{version}
						      ) ;
	$client = 'iPhone' if $market_raw =~ /iPhone/ ;
	$client = 'Android' if $market_raw =~ /Android/ ;
	$version = '2' unless $version ;	
	
	my $ref_account = get_user_from_accountId($dbh_v506,$accountId) ;
	my ($l99NO,$name) = ($ref_account->{l99NO} , $ref_account->{name}) ;
	
	my $ref_account_2 = get_user_from_accountId_2($dbh_v506,$accountId) ;
	my $gender = $ref_account_2 -> {gender} ;
	
	my $name_utf8 = decode_utf8 $name ;
	
	$collection -> insert({
		"accountId" => $accountId , "l99NO" => $l99NO , "name" => $name_utf8 ,
		"gender" => $gender , "ip" => $ip , "market" => $client.'_'.$market ,
		"active_time" => $active_time , "time" => $createTime , "version" => $version
	});
}
$sth_user -> finish();
#=cut

#=pod
# -----------------------------------------------------------------------------
# 新增 device 
# -----------------------------------------------------------------------------
say '-> Redis.FT::user::new::device*_TIME ' ;

for ( 1 .. $time_step)
{
    my $days = $_ ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days)) ;
    my ($key_month,$key_day_numbers) = $key_day =~ /(\d+-\d+)-(\d+)/ ; 
    
    my $sth_d = $dbh_FT -> prepare(" SELECT * FROM device where active_time between '$key_day 00:00:00' and '$key_day 23:59:59' ") ;
    $sth_d -> execute();
    while (my $ref = $sth_d -> fetchrow_hashref())
    {
	my $mid  = $ref -> {device_id} ;
	next unless $mid ;		# 防止数据库中存0或空的device_id
	
	my $time = $ref -> {active_time} ;
	my ($day,$hour) = $time =~ /^(\d+-\d+-\d+) (\d+):/ ;
	
	my $market  = $ref -> {market} ;
	my $version = $ref -> {api_version} ;
	
	$redis -> sadd( 'FT::user::new::device_'.$key_day   , $mid ) ;
	$redis -> sadd( 'FT::user::new::device_'.$key_month , $mid ) ;
	$redis -> sadd( 'FT::user::new::device::hour'.$hour.'_'.$key_day , $mid ) ;
	$redis -> sadd( 'FT::user::new::device::hour'.$hour.'_'.$key_month , $mid ) ;
	
	$redis -> sadd( 'FT::user::new::device::version::'.$version.'_'.$key_day   , $mid ) if $version > 0;
	$redis -> sadd( 'FT::user::new::device::version::'.$version.'_'.$key_month , $mid ) if $version > 0;
	$redis -> sadd( 'FT::user::new::device::version::'.$version.'::hour'.$hour.'_'.$key_day   , $mid ) if $version > 0;
	$redis -> sadd( 'FT::user::new::device::version::'.$version.'::hour'.$hour.'_'.$key_month , $mid ) if $version > 0;
	
	$redis -> sadd( 'FT::user::new::device::market::'.$market.'_'.$key_day   , $mid ) if length($market) > 0;
	$redis -> sadd( 'FT::user::new::device::market::'.$market.'_'.$key_month , $mid ) if length($market) > 0;
	$redis -> sadd( 'FT::user::new::device::market::'.$market.'::hour'.$hour.'_'.$key_day   , $mid ) if length($market) > 0;
	$redis -> sadd( 'FT::user::new::device::market::'.$market.'::hour'.$hour.'_'.$key_month , $mid ) if length($market) > 0;
	
	$redis -> sadd( 'FT::user::new::device::market::'.$market.'::version::'.$version.'_'.$key_day   , $mid ) if $version > 0 && length($market) > 0;
	$redis -> sadd( 'FT::user::new::device::market::'.$market.'::version::'.$version.'_'.$key_month , $mid ) if $version > 0 && length($market) > 0;
	$redis -> sadd( 'FT::user::new::device::market::'.$market.'::version::'.$version.'::hour'.$hour.'_'.$key_day   , $mid ) if $version > 0 && length($market) > 0;
	$redis -> sadd( 'FT::user::new::device::market::'.$market.'::version::'.$version.'::hour'.$hour.'_'.$key_month , $mid ) if $version > 0 && length($market) > 0;
    
    }
    $sth_d -> finish ;

}
#=cut

#=pod
# ----------------------------------------------------------------------
# FT::user::active_TIME 
# ----------------------------------------------------------------------

say "-> Redis.FT::user::active_TIME " ;
my $database_ft = $mongo -> get_database( 'ft' );
my $collection_user = $database_ft -> get_collection( 'user' );

for ( 1 .. $time_step)
{
    my $days = $_ ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days)) ;
    my ($yy,$mm,$dd) = $key_day =~ /^(\d+)-(\d+)-(\d+)$/ ;
    
    my $log_day = strftime("%Y%m%d", localtime(time()- 86400 * $days)) ;
    my ($key_month,$key_day_numbers) = $key_day =~ /(\d+-\d+)-(\d+)/ ; 
    
    $redis -> exists('FT::A::content::article::top1_' .$key_day ) && next ;
  
    my %active ;			# 活跃的注册用户（龙号）
    my (%active_post,%active_hot) ; 	# 较活跃 & 很活跃
    my (%active_market,%active_post_market,%active_hot_market) ;
    my %active_version ;
    my %active_market_version ;
    
    my %active_device ;			# 活跃的设备号
    my (%active_device_post,%active_device_hot) ;
    my %active_device_version ;
    my (%active_device_market,%active_device_post_market,%active_device_hot_market) ;
    my %active_device_market_version ;
    
    my %active_gender ;			# 活跃用户性别
    my %active_visit ;			# 活跃用户（设备号）
    my %active_hours ;			# 分时段活跃用户
    my %active_version_hours ;		
    my %active_market_hours ;
    my %active_ip ;			# 活跃用户的IP分布
    my %active_os ;			# 活跃用户的OS分布
    my %content_ids;			#
    
    my %devices ;			# 当天新增的device
    $devices{$_} = 1 for ($redis->smembers( 'FT::user::new::device_'.$key_day ) ); 
    
    # 扫描日志中所有的用户 (l99NO)
    my @dirs_ip = (14,69) ;		# 199.14 & 199.69
    for(@dirs_ip)
    {
	my $ip = $_ ;
	my $file_log = $log_dir_FT . $ip . '/access.firsttime.l99.com_' . $log_day . '.log.gz' ;
	open my $fh_log , "gzip -dc $file_log|" ;
	while(<$fh_log>)
	{
	    chomp ;
	    # 192.168.199.57 firsttime.l99.com 7649425 [20/Oct/2014:23:57:41 +0800] "GET /firsttime/rest/content/timeline/my?format=json&limit=20&target_id=2853957&machine_code=352107063377060&client=key%3AFirstTimeForAndroid&version=2.2&format=json&app_id=4 HTTP/1.1" 200 287 "-" "Dalvik/1.6.0 (Linux; U; Android 4.4.2; SM-G9008V Build/KOT49H)" "1.49" http 223.104.25.2 0.024 0.024 .
	    # 192.168.199.57 firsttime.l99.com - [20/Oct/2014:23:57:45 +0800] "GET /firsttime/rest/content/timeline/public?format=json&limit=20&machine_code=352107063377060&client=key%3AFirstTimeForAndroid&version=2.2&format=json&app_id=4 HTTP/1.1" 200 3758 "-" "Dalvik/1.6.0 (Linux; U; Android 4.4.2; SM-G9008V Build/KOT49H)" "4.55" http 223.104.25.2 0.113 0.113 .
	    # 192.168.199.80 firsttime.l99.com 8434035 [14/Dec/2014:01:19:31 +0800] "GET /firsttime/rest/content/timeline/public?format=json&limit=20&machine_code=866298026999261&client=key%3AFirstTimeForAndroid&version=2.2&format=json&app_id=4 HTTP/1.0" "appChannel=yingyongbao" "deviceId=866298026999261" "api_version=3" 200 28242 "-" "firsttime" "-" - 223.104.19.4 0.442 0.442 .
	    my $line = $_ ;
	    #say $line ;
	    my ($l99NO,$day,$hour,$mm_log,$ss_log,$get,$content_id,$channel,$mid,$version,$ip) ;
	    
	    ($channel) = $line =~ /"appChannel=(.*?)"/ ;
	    ($mid) = $line =~ /"deviceId=(.*?)"/ ;
	    ($version) = $line =~ /"api_version=(.*?)"/ ;
	    ($ip) = $line =~ / \S+ (\d+\.\d+\.\d+\.\d+) / ;
	    
	    if ($line =~ /firsttime.l99.com (.*?) \[(\d+)\/[a-zA-Z]+\/\d+:(\d+):(\d+):(\d+) \S+] "([\s\S]*?)"/)
	    {
		$l99NO = $1 if $1 > 1;
		($day,$hour,$mm_log,$ss_log,$get) = ($2,$3,$4,$5,$6) ;
		
		# 哈希保存所有 device 的请求时间 unixtime，提供给后面做有效激活判定
		my $unixtime = timelocal($ss_log, $mm_log, $hour, $dd, $mm -1 , $yy);
		push @{$devices{$mid}} , $unixtime if exists $devices{$mid} ;
		
		($content_id) = $get =~ /content_id=(\d+)&/ ;
	    }
	    #say join "\t" , ($l99NO,$day,$hour,$content_id,$channel,$mid,$version,$ip) ;
	    
	    # 文章被访问次数
	    $content_ids{$content_id} ++ if $content_id;
	    
	    # 活跃用户
	    $active{$l99NO} = 1 ;
	    $active_device{$mid} = 1 ;
	    $active_visit{$mid} = 1 ;
	    if($get =~ /POST \/firsttime\/rest\/content\/post/i)	# 较活跃的用户（发表文章）
	    {
		$active_post{$l99NO} = 1 if $l99NO;
		$active_post_market{$channel}{$l99NO} = 1 if length($channel) > 1 && $l99NO;
		$active_device_post{$mid} = 1 if $mid;
		$active_device_post_market{$channel}{$mid} = 1 if length($channel) > 1 && $mid;
	    }
	    
	    # 分版本的活跃用户
	    $active_version{$version}{$l99NO} = 1 if $version > 0;
	    $active_device_version{$version}{$mid} = 1 if $version > 0 && $mid;
	    
	    # 分渠道的活跃用户
	    $active_market{$channel}{$l99NO} = 1 if length($channel) > 1;
	    $active_device_market{$channel}{$mid} = 1 if length($channel) > 1 && $mid;
	    
	    # 分渠道分版本的活跃用户
	    $active_market_version{$channel.'_'.$version}{$l99NO} = 1 if $version > 0 && length($channel) > 1;
	    $active_device_market_version{$channel.'_'.$version}{$mid} = 1 if $version > 0 && length($channel) > 1 && $mid;
	    
	    # 分设备操作系统的活跃用户（android & ios）
	    my ($client) = $get =~ /client=(.*?)&/ ;
	    $active_os{'android'}{$l99NO} = 1 if $client =~ /android/i;
	    $active_os{'ios'}{$l99NO} = 1 if $client =~ /iPhone/i;
	    
	    # 分时段的活跃用户
	    $active_hours{'hour'.$hour}{$l99NO} = 1;
	    $active_version_hours{'version::'.$version.'::hour'.$hour}{$l99NO} = 1 if $version > 0;
	    $active_market_hours{'market::'.$channel.'::hour'.$hour}{$l99NO} = 1 if length($channel) > 1 ;
	    
	    # 活跃用户的ip位置分布
	    #say $ip;
	    my $city_geo = $redis_ip->get($ip) if $ip;
	    my ($country,$province) = $city_geo =~ /^(.*?)_(.*?)_.+$/ ;
	    $active_ip{$country.'_'.$province}{$l99NO} = 1 if $province ;
	    
	}    
    }
#=pod    
    # 较活跃的 用户 & 设备 （定义：发表文章）
    foreach (%active_post){
	my $l99NO = $_ ;
	$redis -> sadd( 'FT::user::active::+_'.$key_day  , $l99NO ) ;
	$redis -> sadd( 'FT::user::active::+_'.$key_month, $l99NO ) ;
    }

    foreach (%active_device_post){
	my $mid = $_ ;
	$redis -> sadd( 'FT::user::active::device::+_'.$key_day  , $mid ) ;
	$redis -> sadd( 'FT::user::active::device::+_'.$key_month, $mid ) ;
    }
    
    # 分渠道的 较活跃的 用户 & 设备 
    foreach (%active_post_market){
	my $market = $_ ;
	foreach (keys %{$active_post_market{$market}})
	{
	    $redis -> sadd( 'FT::user::active::+::market::'.$market.'_'.$key_day  , $_ ) ;
	    $redis -> sadd( 'FT::user::active::+::market::'.$market.'_'.$key_month, $_ ) ;
	}
    }
	
    foreach (%active_device_post_market){
	my $market = $_ ;
	foreach (keys %{$active_device_post_market{$market}})
	{
	    $redis -> sadd( 'FT::user::active::device::+::market::'.$market.'_'.$key_day  , $_ ) ;
	    $redis -> sadd( 'FT::user::active::device::+::market::'.$market.'_'.$key_month, $_ ) ;
	}
    }
    

    # 分客户端版本的 活跃设备
    foreach (%active_device_version)
    {
	my $version = $_ ;
	next unless $version > 0 ;
	foreach (keys %{$active_device_version{$version}})
	{
	    $redis -> sadd( 'FT::user::active::device::version::'.$version.'_'.$key_day  , $_ ) ;
	    $redis -> sadd( 'FT::user::active::device::version::'.$version.'_'.$key_month, $_ ) ;
	}
    }
    
    # 每个渠道的活跃设备
    foreach (%active_device_market)
    {
	my $market = $_ ;
	foreach (keys %{$active_device_market{$market}})
	{
	    $redis -> sadd( 'FT::user::active::device::market::'.$market.'_'.$key_day  , $_ ) ;
	    $redis -> sadd( 'FT::user::active::device::market::'.$market.'_'.$key_month, $_ ) ;
	}
    }
    
    # 各渠道各版本的活跃设备
    foreach (%active_device_market_version)
    {
	my ($channel,$version) = split '_' , $_ ; ;
	foreach (keys %{$active_device_market_version{$channel.'_'.$version}})
	{
	    my $mid = $_ ;
	    $redis -> sadd( 'FT::user::active::device::market::'.$channel.'::version::'.$version.'_'.$key_day  , $mid ) ;
	    $redis -> sadd( 'FT::user::active::device::market::'.$channel.'::version::'.$version.'_'.$key_month, $mid ) ;
	}
    }
 
    # 每个客户端版本的活跃用户
    foreach (%active_version)
    {
	my $version = $_ ;
	next unless $version > 0 ;
	foreach (keys %{$active_version{$version}}){
	    my $l99NO = $_ ;
	    $redis -> sadd( 'FT::user::active::version::'.$version.'_'.$key_day  , $l99NO ) ;
	    $redis -> sadd( 'FT::user::active::version::'.$version.'_'.$key_month, $l99NO ) ;
	}
    }
    
    # 每个渠道的活跃用户
    foreach (%active_market)
    {
	my $market = $_ ;
	foreach (keys %{$active_market{$market}}){
	    my $l99NO = $_ ;
	    $redis -> sadd( 'FT::user::active::market::'.$market.'_'.$key_day  , $l99NO ) ;
	    $redis -> sadd( 'FT::user::active::market::'.$market.'_'.$key_month, $l99NO ) ;
	}
    }
    
    # 各渠道各版本的活跃用户
    foreach (%active_market_version)
    {
	my ($channel,$version) = split '_' , $_ ; ;
	foreach (keys %{$active_market_version{$channel.'_'.$version}}){
	    my $l99NO = $_ ;
	    $redis -> sadd( 'FT::user::active::market::'.$channel.'::version::'.$version.'_'.$key_day  , $l99NO ) ;
	    $redis -> sadd( 'FT::user::active::market::'.$channel.'::version::'.$version.'_'.$key_month, $l99NO ) ;
	}
    }
   
    # 新增设备中的有效激活设备判定（间隔 N 秒以上的请求存在性，N = 20）
    foreach (keys %devices)
    {
	my $mid = $_ ;
	my @times = sort @{$devices{$mid}} ;
	if (abs($times[0] - $times[-1]) > 20) {
	    #say $mid."\t".join(",",@times) ;
	    $redis -> sadd( 'FT::user::new::device::valid_'.$key_day   , $mid ) ;
	    $redis -> sadd( 'FT::user::new::device::valid_'.$key_month , $mid ) ;
	}
    }
    
   
    # 活跃的设备
    my $rediskey_active_device_day   = 'FT::user::active::device_' .$key_day  ;
    my $rediskey_active_device_month = 'FT::user::active::device_' .$key_month ;
    foreach (keys %active_device)
    {
	my $mid = $_ ;
	$redis -> sadd( $rediskey_active_device_day  , $mid ) ;
	$redis -> sadd( $rediskey_active_device_month, $mid ) ;
    }
  
    # 活跃的注册用户
    my $rediskey_active_day   = 'FT::user::active_' .$key_day  ;
    my $rediskey_active_month = 'FT::user::active_' .$key_month ;
    foreach (keys %active)
    {
	my $l99NO = $_ ;
	$redis -> sadd( $rediskey_active_day  , $l99NO ) ;
	$redis -> sadd( $rediskey_active_month, $l99NO ) ;
    }
    

    # 活跃用户区分设备系统  android & ios
    my $rediskey_user_active_android   = 'FT::A::user::active::android_'.$key_day ;
    my $redisvalue_user_active_android = scalar keys %{$active_os{'android'}} ;
    insert_redis_scalar($rediskey_user_active_android => $redisvalue_user_active_android) ;
    
    my $rediskey_user_active_ios   = 'FT::A::user::active::ios_'.$key_day ;
    my $redisvalue_user_active_ios = scalar keys %{$active_os{'ios'}} ;
    insert_redis_scalar($rediskey_user_active_ios => $redisvalue_user_active_ios) ;

    # 文章被访问次数
    my $rediskey_article   = 'FT::content::article_' .$key_day ;
    my $redisvalue_article = join ";" , map {"$_,$content_ids{$_}"} keys %content_ids ;
    insert_redis_scalar( $rediskey_article => $redisvalue_article );
    
    # 活跃用户IP位置分布
    my %user_ip_temp;
    foreach(keys %active_ip){
	my $p = $_ ;
	my $n = scalar keys %{$active_ip{$p}} ;
	$user_ip_temp{$p} = $n ;
    }
    my $rediskey_active_ip   = 'FT::A::user::active::ip_'.$key_day;
    my $redisvalue_active_ip = join ';' , map { $_ . ',' . $user_ip_temp{$_} } sort { $user_ip_temp{$b} <=> $user_ip_temp{$a} } keys %user_ip_temp ;
    insert_redis_scalar($rediskey_active_ip => $redisvalue_active_ip) ;
     
    # 活跃用户分时段
    foreach (keys %active_hours)
    {
	my $hour = $_ ;
	my $rediskey_active_hours   = 'FT::A::user::active::' . $hour . '_' . $key_day ;
	my $redisvalue_active_hours = scalar keys %{$active_hours{$hour}} ;
	insert_redis_scalar($rediskey_active_hours => $redisvalue_active_hours) if $redisvalue_active_hours;
	
	my $nums_gender_0 = 0 ;
	foreach(keys %{$active_hours{$hour}}){
	    my $l99NO = $_ ;
	    my $ref = get_user_from_l99NO($collection_user,$l99NO) ;
	    my $gender = $ref -> {gender} ;
	    $nums_gender_0 ++ if $gender eq '0' ;
	}
	my $rediskey_active_hours_gender_0 = 'FT::A::user::active::' . $hour . '::gender::0_' . $key_day ;
	insert_redis_scalar($rediskey_active_hours_gender_0 => $nums_gender_0) if $nums_gender_0;
	my $rediskey_active_hours_gender_1 = 'FT::A::user::active::' . $hour . '::gender::1_' . $key_day ;
	my $nums_gender_1 = $redisvalue_active_hours - $nums_gender_0 ;
	insert_redis_scalar($rediskey_active_hours_gender_1 => $nums_gender_1) if $nums_gender_1;
    }
    
    # 分客户端版本的每时段活跃用户数
    foreach (%active_version_hours)
    {
	next unless /version/ ;
	my $k = $_ ;			# 'version::'.$version.'::hour'.$hour
	my $num = scalar keys %{$active_version_hours{$k}} ;
	insert_redis_scalar('FT::A::user::active::'.$k.'_'.$key_day , $num) ;
    }
    
    # 分渠道市场的每小时活跃用户数
    foreach (%active_market_hours)
    {
	next unless /market/ ;
	my $k = $_ ;			# 'market::'.$channel.'::hour'.$hour
	my $num = scalar keys %{$active_market_hours{$k}} ;
	insert_redis_scalar('FT::A::user::active::'.$k.'_'.$key_day , $num) ;
    }
    
}
#=cut

# ==================================== functions =========================================

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

sub get_majia
{
    my ($dbh,$table) = @_ ;
    my %majias ;
    my $sth = $dbh -> prepare(" SELECT accountIds FROM $table ") ;
    $sth -> execute();
    while (my $ref = $sth -> fetchrow_hashref())
    {
	my $accountId = $ref -> {accountIds} ;
	$majias{$accountId} = 1 if $accountId ;
    }
    return \%majias ;
}


# -----------------------------------------------------------------------------------------
# get user_info.[] from machineCode  --  TABLE:device
# -----------------------------------------------------------------------------------------
sub get_user_from_mid
{
    my ($dbh_ft,$mid) = @_ ;
    my $ref_mid ;
    my $sql = " SELECT * FROM device where device_id = '$mid' " ;
    my $sth_d = $dbh_ft -> prepare($sql) ;
    $sth_d -> execute();
    while (my $ref = $sth_d -> fetchrow_hashref())
    {
	my $market = $ref -> {market} ;
	my $client = $ref -> {client} ;
	$client =~ s/^.*?For// ;
	my $active_time = $ref -> {active_time} ;
	my $version = $ref -> {api_version} ;
	$ref_mid -> {market} = $market ;
	$ref_mid -> {client} = $client ;
	$ref_mid -> {active_time} = $active_time ;
	$ref_mid -> {version} = $version ;
    }
    $sth_d -> finish ;
    return $ref_mid ;
}

# -----------------------------------------------------------------------------------------
# get user_info.[userl99No,username] from accountId -- TABLE:account
# -----------------------------------------------------------------------------------------
sub get_user_from_accountId
{
	my ($dbh,$accountId) = @_ ;
	my $ref_accountId ;
	my $sth_account = $dbh -> prepare(" SELECT accountId,l99NO,name FROM account WHERE accountId = $accountId ") ;
	$sth_account -> execute();
	while (my $ref_account = $sth_account -> fetchrow_hashref())
	{
		my $accountId = $ref_account -> {accountId};
		my $l99NO     = $ref_account -> {l99NO};
		my $name      = $ref_account -> {name};
		$ref_accountId -> {l99NO} = $l99NO ;
		$ref_accountId -> {name}  = $name ;
	}
	$sth_account -> finish ;
	return $ref_accountId ;
}

# -----------------------------------------------------------------------------------------
# get user_info.[gender] from accountId  --  TABLE:account_profile
# -----------------------------------------------------------------------------------------
sub get_user_from_accountId_2
{
	my ($dbh,$accountId) = @_ ;
	my $ref_accountId ;
	my $sth_account = $dbh -> prepare(" SELECT accountId,gender FROM account_profile WHERE accountId = $accountId ") ;
	$sth_account -> execute();
	while (my $ref_account = $sth_account -> fetchrow_hashref())
	{
		my $accountId = $ref_account -> {accountId};
		my $gender    = $ref_account -> {gender};
		$ref_accountId -> {gender} = $gender ;
	}
	$sth_account -> finish ;
	return $ref_accountId ;
}

# -----------------------------------------------------------------------------------------
# get user_info.[gender] from l99NO  -- MongoDB
# -----------------------------------------------------------------------------------------
sub get_user_from_l99NO
{
	my ($coll,$l99NO) = @_ ;
	my $data_user = $coll->find_one({'l99NO' => $l99NO});
        my $ref ;
	$ref->{gender} = $data_user -> {gender} ;
	return $ref ;
}
