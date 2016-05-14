#!/usr/bin/env perl 
# ==============================================================================
# function:		床上项目数据统计
# Author:		kiwi
# createTime:	2015.4.7
# ==============================================================================
use 5.10.1 ;

BEGIN
{
    my @PMs = (
		   #'Redis' ,
           #'MongoDB' ,
		   #'Config::Tiny' ,
		   #'DBI' ,
		   #'JSON::XS' ,
		   #'Date::Calc::XS' ,
		   #'Time::Local'
	) ;
    foreach(@PMs)
	{
            my $pm = $_ ;
            eval {require $pm;};
            if ($@ =~ /^Can't locate/)
			{
                        print "install module $pm";
                        # 需要Linux事先配置好cpanm环境
						# curl -L http://cpanmin.us | perl - --sudo App::cpanminus
						`cpanm $pm`;			
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
use File::Lockfile ; 
use POSIX qw(strftime);
use Time::Local ;
use Date::Calc::XS qw (Date_to_Time Time_to_Date);
use Unicode::UTF8 qw (decode_utf8 encode_utf8);
binmode(STDIN,  ':encoding(utf8)');
binmode(STDOUT, ':encoding(utf8)');
binmode(STDERR, ':encoding(utf8)');
# ------------------------------------------------------------------------------

#---------------------------------------------------------
# create file lock to make sure this script only run 1 copy
#---------------------------------------------------------
#my $lockfile = File::Lockfile->new('cs.lock' , '/home/DA/DataAnalysis');
#if ( my $pid = $lockfile->check ) {
#        say "Seems that program is already running with PID: $pid";
#        exit;
#}
#$lockfile->write;

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
my $CS_user_host = $Config -> {CS_DB}  -> {host_user};
my $CS_db        = $Config -> {CS_DB}  -> {database};
my $CS_usr       = $Config -> {CS_DB}  -> {username};
my $CS_password  = $Config -> {CS_DB}  -> {password};
my $CS_user_dbs  = $Config -> {CS_DB}  -> {database_account} ;
my $CS_db_comment= $Config -> {CS_DB}  -> {database_comment} ;
my $CS_log_dir   = $Config -> {CS_LOG} -> {dir} ;
my $article_topN = $Config -> {CS_LOG} -> {topN} ;

my $OF_host     = $Config -> {CS_DB}  -> {host_openfire} ;
my $OF_db       = $Config -> {CS_DB}  -> {database_openfire} ;
my $OF_usr      = $Config -> {CS_DB}  -> {username_openfire} ;
my $OF_password = $Config -> {CS_DB}  -> {password_openfire} ;

my $Wwere_host     = $Config -> {Wwere_DB} -> {host};
my $Wwere_db       = $Config -> {Wwere_DB} -> {database} ;
my $Wwere_usr      = $Config -> {Wwere_DB} -> {username};
my $Wwere_password = $Config -> {Wwere_DB} -> {password};

#my $time_step = 1000 ;
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
my $redis_db2 = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
$redis_db2 -> select(2) ;
my $redis_market = Redis->new(server => "$redis_host_market:$redis_port",reconnect => 10, every => 2000);
$redis_market -> select(6) ;
my $redis_active = Redis->new(server => "192.168.201.57:6379",reconnect => 10, every => 2000);
my $mongo = MongoDB::MongoClient->new(host => $mongo_host, port => $mongo_port , query_timeout => 1000000);
my $collection_present = $mongo -> get_database( 'cs' ) -> get_collection( 'presents' );
my $collection_payin   = $mongo -> get_database( 'cs' ) -> get_collection( 'payin' );

# ------------------------------------------------------------------
# connect to mysql
# ------------------------------------------------------------------
my $dsn   = "DBI:mysql:database=$CS_db;host=$CS_host" ;
my $dbh   = DBI -> connect($dsn, $CS_usr, $CS_password, {'RaiseError' => 1} ) ;
$dbh -> do ("SET NAMES UTF8");

my $dsn_openfire = "DBI:mysql:database=$OF_db;host=$OF_host" ;
my $dbh_openfire   = DBI -> connect($dsn_openfire, $OF_usr, $OF_password, {'RaiseError' => 1} ) ;
$dbh_openfire -> do ("SET NAMES UTF8");

my $dsn_v506 = "DBI:mysql:database=$L06_db;host=$L06_host" ;
my $dbh_v506 = DBI -> connect($dsn_v506, $L06_usr, $L06_password, {'RaiseError' => 1} ) ;
$dbh_v506 -> do ("SET NAMES UTF8");

my $dsn_v506_user = "DBI:mysql:database=$L06_db;host=$CS_user_host" ;
my $dbh_v506_user = DBI -> connect($dsn_v506_user , $L06_usr, $L06_password, {'RaiseError' => 1} ) ;
$dbh_v506_user -> do ("SET NAMES UTF8");

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


=pod
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
		
		my $ref_account = get_user_from_accountId($dbh_v506_user,$accountId) ;
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
=cut

#=pod
# ----------------------------------------------------------------
# 新增用户统计	CS::A::user::new
# ----------------------------------------------------------------
say "-> Redis.CS::A::user::new*_DAY" ;

my %auth_type = (
	'10'  => 'Email'	, '20' => '手机号' ,
	'110' => 'QQ微博'	, '111' => 'QQ'   ,
	'120' => '新浪微博'	, '130' => '搜狐微博' ,
	'240' => '微信'
) ;
for ( 1 .. $time_step + 1 )
{
    my $days_step = $_ - 1 ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
	say $key_day ;
    
	my $num_new ;
    my ($num_gender_0,$num_gender_1) ;			# 新增用户分性别数量
    my ($num_ios,$num_android) ;				# 新增用户分系统数量
    my $num_new_block;
    my ($num_gender_0_block,$num_gender_1_block) ;
    my ($num_ios_block,$num_android_block) ;
    my %user_new_hours ;
	
	my $ref_user_version = get_user_version($redis_active,$key_day) ;
	
	my $timestamp_l = local2unix($key_day.' 00:00:00') * 1000 ;
	my $timestamp_r = local2unix($key_day.' 23:59:59') * 1000 ;
	my %temp ;
	
	my $sth_auth = $dbh_v506_user -> prepare("
											SELECT r.authId,r.accountId,r.authType,l.market,r.eventTime
											FROM account_log l left join account_authentication r 
											on l.accountId = r.accountId
											WHERE
											r.eventTime between $timestamp_l and $timestamp_r
										") ;
	$sth_auth -> execute();
	while (my $ref = $sth_auth -> fetchrow_hashref())
	{
		my $authId     = $ref -> {authId} ;
		my $accountId  = $ref -> {accountId} ;
		# 非床上用户忽略
		my @row_ary = $dbh->selectrow_array("select 1 from nyx_1.account where accountId = $accountId limit 1");
		next unless scalar(@row_ary) ;
        
        my $market_auth = $ref -> {market} ;
        my $event_time  = $ref -> {eventTime} ;
		my $time = unix2local($event_time / 1000) ;
		my ($hour) = $time =~ / (\d+):\d+:\d+$/ ;
        my $authType = $ref -> {authType} ;
        if ($authType == 20)
        {
                # get the user's gender and l99NO
                my $ref_account = get_gender_from_accountId($dbh , 'account' , $accountId) ;
                my $gender = $ref_account->{gender} ;
                my $l99NO  = $ref_account->{l99NO}  ;
            
                my $version = $ref_user_version -> {$l99NO} ;
                my $ref_temp = get_user_market2_from_accountId($dbh_v506_user,$accountId) ;
                my $market = $ref_temp -> {market} ;
                my $ref_account_os = get_user_market_from_accountId($dbh_v506_user,$accountId) ;
                my $os = $ref_account_os -> {market} ;
                
                #my $ref_account_status = get_user_from_accountId($dbh_v506_user,$accountId) ;
                #my $status = $ref_account_status -> {status}  ;
                my $isBlock = isBlock($dbh,$accountId) ;
        
                if ( $isBlock )
                {
                    $redis_active -> sadd('STORM::CS::user::new::block_'.$key_day , $l99NO );
            
                    $user_new_hours{'CS::A::user::new::block::hour'.$hour.'::gender::'.$gender.'::'.$os.'_'.$key_day} ++ if $os;
                    $user_new_hours{'CS::A::user::new::block::hour'.$hour.'::gender::'.$gender.'_'.$key_day} ++ ;
                    $user_new_hours{'CS::A::user::new::block::hour'.$hour.'::'.$os.'_'.$key_day} ++ if $os;	
                    $user_new_hours{'CS::A::user::new::block::gender::'.$gender.'::'.$os.'_'.$key_day} ++ if $os;
                    $user_new_hours{'CS::A::user::new::block::hour'.$hour.'_'.$key_day} ++ ;
                    $user_new_hours{'CS::A::user::new::block::gender::'.$gender.'::version::'.$version.'_'.$key_day} ++ if $version;
                    $user_new_hours{'CS::A::user::new::block::gender::'.$gender.'::market::'.$market.'_'.$key_day} ++ if $market;
                    $user_new_hours{'CS::A::user::new::block::gender::'.$gender.'::market::'.$market.'::version::'.$version.'_'.$key_day} ++ if $market && $version;
                    $user_new_hours{'CS::A::user::new::block::hour'.$hour.'::market::'.$market.'_'.$key_day} ++ if $market;
                    $user_new_hours{'CS::A::user::new::block::hour'.$hour.'::gender::'.$gender.'::market::'.$market.'_'.$key_day} ++ if $market;
                    $user_new_hours{'CS::A::user::new::block::hour'.$hour.'::market::'.$market.'::version::'.$version.'_'.$key_day} ++ if $market && $version ;
                    $user_new_hours{'CS::A::user::new::block::hour'.$hour.'::gender::'.$gender.'::market::'.$market.'::version::'.$version.'_'.$key_day} ++ if $market && $version ;
                    $user_new_hours{'CS::A::user::new::block::market::'.$market.'_'.$key_day} ++ if $market ;				    # 分渠道
                    $user_new_hours{'CS::A::user::new::block::market::'.$market.'::'.$os.'_'.$key_day} ++ if $market && $os ;
                    $user_new_hours{'CS::A::user::new::block::market::'.$market.'::version::'.$version.'_'.$key_day} ++ if $market && $version ;
                    $user_new_hours{'CS::A::user::new::block::version::'.$version.'_'.$key_day} ++ if $version ;
                    $num_new_block ++ ;
                    $num_gender_0_block ++  if $gender == 0 ;
                    $num_ios_block ++ if $os eq 'ios' ;
            
                }
                
                # 记录注册用户列表
                $redis_active -> sadd('STORM::CS::user::new_'.$key_day , $l99NO );
                
                $user_new_hours{'CS::A::user::new::hour'.$hour.'::gender::'.$gender.'::'.$os.'_'.$key_day} ++ if $os;
                $user_new_hours{'CS::A::user::new::hour'.$hour.'::gender::'.$gender.'_'.$key_day} ++ ;
                $user_new_hours{'CS::A::user::new::hour'.$hour.'::'.$os.'_'.$key_day} ++ if $os;	
                $user_new_hours{'CS::A::user::new::gender::'.$gender.'::'.$os.'_'.$key_day} ++ if $os;
                $user_new_hours{'CS::A::user::new::hour'.$hour.'_'.$key_day} ++ ;
                $user_new_hours{'CS::A::user::new::gender::'.$gender.'::version::'.$version.'_'.$key_day} ++ if $version;
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
                $num_ios ++ if $os eq 'ios' ;
                
                my $sth_auth2 = $dbh_v506_user -> prepare("
                                                            SELECT authType from account_authentication
                                                            WHERE accountId = $accountId and authType <> 20
                                                    ");
                $sth_auth2 -> execute();
                while (my $ref2 = $sth_auth2 -> fetchrow_hashref())
                {
                        my $type = $ref2 -> {authType} ;
                    	$authType = $type if $type ;
                }
                $sth_auth2 -> finish ;
                
                my $type_name = $auth_type{$authType} ;
                $temp{$type_name} ++ if $market_auth =~ /Bed/ ;
        }
        else
        {
            	
        }
        
	}
	$sth_auth -> finish ;

    $num_gender_1_block = $num_new_block - $num_gender_0_block ;
    $num_android_block = $num_new_block - $num_ios_block ;
    insert_redis_scalar('CS::A::user::new::block::gender::0_'.$key_day , $num_gender_0_block) if $num_gender_0_block;
    insert_redis_scalar('CS::A::user::new::block::gender::1_'.$key_day , $num_gender_1_block) if $num_gender_1_block;
    insert_redis_scalar('CS::A::user::new::block_'.$key_day , $num_new_block) if $num_new_block;
    insert_redis_scalar('CS::A::user::new::block::ios_'    .$key_day , $num_ios_block)     if $num_ios_block ;
    insert_redis_scalar('CS::A::user::new::block::android_'.$key_day , $num_android_block) if $num_android_block ;
    
    
    $num_gender_1 = $num_new - $num_gender_0 ;
    $num_android = $num_new - $num_ios ;
    insert_redis_scalar('CS::A::user::new::gender::0_'.$key_day , $num_gender_0) if $num_gender_0;
    insert_redis_scalar('CS::A::user::new::gender::1_'.$key_day , $num_gender_1) if $num_gender_1;
    insert_redis_scalar('CS::A::user::new_'.$key_day , $num_new) if $num_new;
    insert_redis_scalar('CS::A::user::new::ios_'    .$key_day , $num_ios)     if $num_ios ;
    insert_redis_scalar('CS::A::user::new::android_'.$key_day , $num_android) if $num_android ;
    
    insert_redis_hash(\%user_new_hours) ;

	my $auth_info = encode_json \%temp ;
	insert_redis_scalar('CS::A::user::new::auth_'.$key_day , $auth_info) ;
}
#=cut

=pod

# 这里有个需求是没有绑定手机号但是却活跃的用户
# 逻辑是没有绑定手机号的用户中去判断 nyx_1.account 的 updateTime > createTime，则符合条件
for ( 1 .. $time_step + 2 )
{
        my $days_step = $_ - 1 ;
        my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    	say $key_day ;
        my $timestamp_l = local2unix($key_day.' 00:00:00') * 1000 ;
        my $timestamp_r = local2unix($key_day.' 23:59:59') * 1000 ;
    	
        my %temp;
        my $sth_auth = $dbh_v506_user -> prepare("
											SELECT r.authId,r.accountId,r.authType,l.market,r.eventTime
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
            my $type = $ref -> {authType} ;
            # the user must be in cs
            my @row_ary = $dbh->selectrow_array("select 1 from nyx_1.account where accountId = $accountId limit 1");
            next unless scalar(@row_ary) ;
            #say "$accountId \t $type ";
            $temp{$accountId} = 1 unless $type == 20 ;
		}
		$sth_auth -> finish ;
    
        my $num ;
        foreach (keys %temp)
        {
            my $accountId = $_ ;
            next unless $temp{$accountId} == 1 ;
            my $sth_user = $dbh -> prepare("
                                            SELECT createTime,updateTime
                                            FROM
                                            nyx_1.account
                                            WHERE
                                            accountId = $accountId
                                        ");
			$sth_user -> execute();
			while (my $ref = $sth_user -> fetchrow_hashref())
			{
                my $cTime = $ref -> {createTime} ;
                my $uTime = $ref -> {updateTime} ;
                #say "$cTime \t $uTime" ;
                $num ++ if $uTime ne $cTime ;
            }
            $sth_user -> finish ;
        }
        insert_redis_scalar('CS::A::user::active::nophonenumber_'.$key_day , $num) ;
}

#=pod
# 用户上传头像
# --------------------------------------------------------------------------------
my %paths_d ;
my $sth_avatar_gallery = $dbh -> prepare(" SELECT * FROM avatar_gallery ");
$sth_avatar_gallery -> execute();
while (my $ref = $sth_avatar_gallery -> fetchrow_hashref())
{
    my $path = $ref -> {path} ;
    $paths_d{$path} = 1 ;
}
$sth_avatar_gallery -> finish ;
my @path_temp = ("6ca/1445571242245_czvy73.png" ,
                 "640/1445571314163_45i3zj.png" ,
                 "e10/1445571285082_4b15fd.png" ,
                 "02/MjAxNDA0MjcxMjQ0MTBfMTE4LjI0NC4yNTUuMTExXzc2MjExOA==.jpg" ,
                 "male.jpg" , "female.jpg" ) ;
$paths_d{$_} = 1 for @path_temp ;


for ( 1 .. $time_step + 1 )
{
    my $days_step = $_ - 1 ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    say $key_day ;
    my %temp ;
    my $sth_avatar = $dbh_v506_user -> prepare("
											SELECT accountId,path
                                            FROM
                                            account_avatar
                                            WHERE
                                            updateTime between '$key_day 00:00:00' and '$key_day 23:59:59'
										");
	$sth_avatar -> execute();
    my $num ;
	while (my $ref = $sth_avatar -> fetchrow_hashref())
	{
			my $accountId   = $ref -> {accountId} ;
            my $avatarsPath = $ref -> {path} ;
            $avatarsPath =~ s/\s+$//g ;
            next if $avatarsPath =~ /^http/ ;               #第三方头像排除，地址以‘http’开头
			next if exists $paths_d{$avatarsPath} ;         #系统头像也排除
            
            my @row_ary = $dbh->selectrow_array("select 1 from nyx_1.account where accountId = $accountId limit 1");
            next unless scalar(@row_ary) ;
        
            my $ref_temp = get_user_market2_from_accountId($dbh_v506_user,$accountId) ;
            my $market = $ref_temp -> {market} ;
            
            $temp{'CS::A::content::avatar_'.$key_day} ++ if $market;
            $temp{'CS::A::content::avatar::market::'.$market.'_'.$key_day} ++ if $market;
	}
	$sth_avatar -> finish ;
    insert_redis_hash(\%temp) ;
    
}

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
#=cut


say "-> Redis.CS::A::user::active::device_TIME" ;
for ( 1 .. $time_step)
{
    my $days_step = $_ ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    
    my $num_device_android = $redis_db2 -> scard( 'CS::user::active::device::android_'.$key_day );
    my $num_device_iphone  = $redis_db2 -> scard( 'CS::user::active::device::iphone_'.$key_day );
    my $num_device = $num_device_android + $num_device_iphone ;
    insert_redis_scalar('CS::A::user::active::device::android_'.$key_day , $num_device_android) ;
    insert_redis_scalar('CS::A::user::active::device::iphone_' .$key_day , $num_device_iphone ) ;
    insert_redis_scalar('CS::A::user::active::device_' .$key_day , $num_device ) ;
    
}

#=pod
# -----------------------------------------------------------------------
# CS::A::content::article 文章阅读排行
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
#=cut

#=pod
# ---------------------------------------------------------------------------------------
# 床上用户内容被选入各版块的数量
# ---------------------------------------------------------------------------------------
say "-> Redis.CS::A::content::guide" ;
for ( 1 .. $time_step + 1)
{
	my $days_step = $_ - 1 ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    
    my $hour_now = strftime( "%H" , localtime(time()) ) ;
    next if $days_step == 1 and $hour_now > 0 ;
    
    say $key_day ;
	my %guides ;
	my $sth_guide = $dbh -> prepare("
                                        SELECT typeId,dashboardId FROM
                                        content_guide
                                        WHERE
                                        updateTime between '$key_day 00:00:00' and '$key_day 23:59:59'
                                    ");
	$sth_guide -> execute();
	while (my $ref = $sth_guide -> fetchrow_hashref())
	{
		my $typeId = $ref -> {typeId} ;
		$guides{$typeId} ++ ;
	}
	my $guide_info = encode_json \%guides;
	insert_redis_scalar('CS::A::content::guide_'.$key_day , $guide_info) ;
}

#=pod

# -----------------------------------------------------------
# 用户发布内容
# -----------------------------------------------------------
say "-> Redis.CS::A::content::article_TIME" ;
for ( 1 .. $time_step + 1 )
{
	my $days_step = $_ - 1 ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    my $hour_now = strftime( "%H" , localtime(time()) ) ;
    next if $days_step == 1 and $hour_now > 0 ;
    
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
		
		my $ref_temp = get_user_market2_from_accountId($dbh_v506_user , $accountId) ;
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

#=cut

# --------------------------------------------------
# 用户发布照片
# --------------------------------------------------
say "-> Redis.CS::A::content::photo_TIME" ;
for ( 1 .. $time_step+1 )
{
	my $days_step = $_ - 1 ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    # 下面这2行的目的：在非回滚状态时，只在今天1点前重复扫描昨天的数据
    my $hour_now = strftime( "%H" , localtime(time()) ) ;
    next if $days_step == 1 and $hour_now > 0 ;
    
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
		
		my $ref_temp = get_user_market2_from_accountId($dbh_v506_user , $accountId) ;
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

#=pod
# ---------------------------------------------------------
# 用户发布评论
# ---------------------------------------------------------
say "-> Redis.CS::A::content::comment" ;
for ( 1 .. $time_step+1 )
{
	my $days_step = $_ - 1 ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    my $hour_now = strftime( "%H" , localtime(time()) ) ;
    next if $days_step == 1 and $hour_now > 0 ;
    
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
		
		my $ref_temp = get_user_market2_from_accountId($dbh_v506_user , $accountId) ;
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


say "-> Redis.CS::A::content::chat::times_TIME" ;
for ( 1 .. $time_step + 1)
{
	my $days_step = $_ - 1 ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
	
    my $hour_now = strftime( "%H" , localtime(time()) ) ;
    next if $days_step == 1 and $hour_now > 0 ;
    say $key_day ;
    
	my $timestamp_l = local2unix($key_day.' 00:00:00') * 1000 ;
	my $timestamp_r = local2unix($key_day.' 23:59:59') * 1000 + 999 ;
	
	#if( $days_step == 0 ){
	#	my $start = strftime("%Y-%m-%d %H:%M:%S", localtime(time() - 1000));
	#	$timestamp_l = local2unix($start) * 1000 ;
	#}
	my $num_chat ;
	my $sth_chat = $dbh_openfire -> prepare("
												SELECT Id,fromJID,toJID,createtime
												FROM
												chatrecord
												WHERE
												createtime between $timestamp_l and $timestamp_r
											");
	$sth_chat -> execute();
	while (my $ref = $sth_chat -> fetchrow_hashref())
	{
		my $id = $ref -> {Id} ;
		my $fromId = $ref -> {fromJID} ;
		my $toId   = $ref -> {toJID} ;
		$fromId =~ s/\@chuangliaoapp.com$// ;
		$toId   =~ s/\@chuangliaoapp.com$// ;
		#my $createtime = $ref -> {createtime} ;
		#my $time = unix2local($createtime / 1000) ;
		$num_chat ++ ;
		#say "$fromId => $toId \t $time" ;
		#my ($hour) = $time =~ / (\d+):\d+:\d+$/ ;
		$redis_db2 -> sadd( 'CS::content::chat::from_'.$key_day , $fromId ) ;
		$redis_db2 -> sadd( 'CS::content::chat::to_'.$key_day   , $toId   ) ;

	}
	$sth_chat -> finish ;
	
	insert_redis_scalar( 'CS::A::content::chat::times_'.$key_day , $num_chat ) if $num_chat ;
	
	my $uv_from = $redis_db2 -> scard('CS::content::chat::from_'.$key_day) ;
	my $uv_to   = $redis_db2 -> scard('CS::content::chat::to_'  .$key_day) ;
	insert_redis_scalar( 'CS::A::content::chat::from_'.$key_day , $uv_from ) if $uv_from;
	insert_redis_scalar( 'CS::A::content::chat::to_'  .$key_day , $uv_to ) if $uv_to;
}
#=cut

say "-> Redis.CS::A::content::broadcast::N_TIME" ;
for ( 1 .. $time_step + 1 )
{
	my $days_step = $_ - 1 ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
	my $hour_now = strftime( "%H" , localtime(time()) ) ;
    next if $days_step == 1 and $hour_now > 0 ;
    say $key_day ;
    
	my %temp ;
	my $sth_broadcast = $dbh -> prepare("
                                        SELECT
										bId,type
										FROM
										broadcast
										WHERE
										createTime between '$key_day 00:00:00' and '$key_day 23:59:59'
									");
	$sth_broadcast -> execute();
	
	while (my $ref = $sth_broadcast -> fetchrow_hashref())
	{
		my $bId  = $ref -> {bId} ;
		my $type = $ref -> {type} ;
		$temp{'CS::A::content::broadcast::' . $type . '_' . $key_day} ++ ;
	}
	
	$sth_broadcast -> finish ;
	insert_redis_hash(\%temp) ;
	
}



#=pod
say "-> Redis.CS::A::content::gamemoon::levels_TIME" ;
my $sth_game = $dbh -> prepare(" SELECT accountId,level FROM moon ");
$sth_game -> execute();
my %levels ;
my $uv_game ;
while ( my $ref = $sth_game -> fetchrow_hashref() )
{
		my $id    = $ref -> {accountId} ;
		my $level = $ref -> {level} ;
		$levels{$level} ++ ;
		$uv_game ++ ;
}
$sth_game -> finish ;

my $game_temp = encode_json \%levels;
insert_redis_scalar('CS::A::content::gamemoon::levels' , $game_temp) ;
insert_redis_scalar('CS::A::content::gamemoon::uv' , $uv_game) ;

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
			my $ref_account = get_user_from_accountId($dbh_v506_user,$accountId) ;
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
    my $hour_now = strftime( "%H" , localtime(time()) ) ;
    next if $days_step == 1 and $hour_now > 0 ;
    
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
		my $ref_account_l99NO = get_user_from_accountId($dbh_v506_user,$fromId) ;
		my $l99NO = $ref_account_l99NO->{l99NO} ;
		
		my $version = $ref_user_version -> {$l99NO} ;
		
		#my $market = $redis_market->get('STORM::CS::user::market::'.$l99NO) ;	
		my $ref_temp = get_user_market2_from_accountId($dbh_v506_user,$fromId) ;
		my $market = $ref_temp -> {market} ;
		
		my $ref_account = get_user_market_from_accountId($dbh_v506_user,$fromId) ;
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
		my $present_market = $_ ;	# e.g.  present::55::market::360
		my ($presentId) = $present_market =~ /present::(\d+)::/ ;
		my $name  = $present_info{$presentId}{name} ;
		my $price = $present_info{$presentId}{price} ;
		my $count = $present_market{$present_market} ;
		
		my %temp = ('name' => $name , 'price' => $price , 'count' => $count) ;
		my $present_temp = encode_json \%temp;
		insert_redis_scalar('CS::A::payin::'.$present_market.'_'.$key_day , $present_temp) if $count;
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

my $sth_pay = $dbh_v506 -> prepare("
									SELECT logId,accountId,userMoney,changeType,changeDesc,changeTime
									FROM
									pay_account_log
									WHERE
									changeType in (5,8,16,17,18,19,20,21,24,25,28,29)
									and
									changeTime > $timestamp_start
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
	
	my $pay_type ;
	if ($changeType == 8 )									
	{
		$pay_type = 'present::fail'  if $changeDesc =~ /礼物过期/ ;
		$pay_type = 'present::back'  if $changeDesc =~ /礼物用户提成/ ;
		$pay_type = 'hotlist::fail'  if $changeDesc =~ /未通过/ ;
		$pay_type = 'recharge'       if $changeDesc =~ /床上充值/ ;
		$pay_type = 'recharge::back' if $changeDesc =~ /龙币充值返赠/ ;
		$pay_type = 'packs::zhongqiu' if $changeDesc =~ /中秋快乐/ ;
		$pay_type = 'packs::guoqing'  if $changeDesc =~ /国庆快乐/ ;
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
	elsif($changeType == 24)								# 
	{
		$pay_type = 'mora::start' if $changeDesc =~ /发起猜拳游戏消耗/ ;
		$pay_type = 'mora::join'  if $changeDesc =~ /参与猜拳游戏消耗/ ;
	}
	elsif($changeType == 25)								# 系统给马甲充值
	{
		$pay_type = 'recharge::service'       if $changeDesc =~ /客服充值\s*$/ ;
		$pay_type = 'recharge::servicegiving' if $changeDesc =~ /客服充值赠送\s*$/ ;
		$pay_type = 'recharge::majia'         if $changeDesc =~ /马甲充值\s*$/ ;
		$pay_type = 'recharge::test'          if $changeDesc =~ /测试账号充值\s*$/ ;
	}
	elsif($changeType == 28)
	{
		$pay_type = 'gamelife' if $changeDesc =~ /获取游戏/ ;
	}
	elsif($changeType == 29)
	{
		$pay_type = 'broadcast'    if $changeDesc =~ /广播/ ;
		$pay_type = 'chat::expand' if $changeDesc =~ /聊天室扩张人数/ ;
	}
	
	next unless $pay_type ;
	my $redis_key = 'CS::payin::'.$pay_type.'::user::'.$accountId . '::uuid::' . $logId . '_' .$time ;
	
    # mongoDB & Redis 双写，redis会定期清理key，后面的payin类型同逻辑
    unless ($collection_payin->find_one({ "rediskey" => $redis_key }))
    {
        $collection_payin -> insert( {"rediskey" => $redis_key , "redisvalue" => $userMoney} );
        say "MongoDB: $redis_key => $userMoney" ;
    }
    
	$redis_db2 -> exists( $redis_key ) && next ;
	$redis_db2 -> set($redis_key , $userMoney);
    say "Redis: $redis_key => $userMoney" ;
    my $expire_time = 60 * 24 * 3600 ;			# 60 days expire
	$redis_db2 -> expire($redis_key ,  $expire_time) ;
	
}
$sth_pay -> finish ;
#=cut

#=pod
# ----------------------------------------------------------------------------------------------
# 床点消费
# 1：充值  2：充值  3：送红包  4：获得床点  5：龙币转床点  6：退还床点
# 7：购买置顶位  8：龙币兑换床点  9：附近排行榜  10：床点礼物提成  11：购买礼物  12：体验新钱包送床点
# ----------------------------------------------------------------------------------------------
say "-> Redis.CS::payin::bedpoint::*_TIME" ;
for ( 1 .. $time_step + 1 )
{
	my $days_step = $_ - 1 ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
	
	my $timestamp_l = local2unix($key_day.' 00:00:00') ; 
	my $timestamp_r = local2unix($key_day.' 23:59:59') ;

	my $sth_pay_point = $dbh_v506 -> prepare("
										SELECT logId,accountId,bedMoney,changeTime,changeDesc,changeType 
										FROM
										bed_pay_account_log
										WHERE
										changeTime between $timestamp_l and $timestamp_r
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
				
				$pay_type = 'bedpoint::shicaigame' if $changeDesc =~ /时彩游戏.*?赢了/ ;
				
        }
        elsif($changeType == 5 ){ $pay_type = 'bedpoint::fromlong' }		
        elsif($changeType == 6 ){ $pay_type = 'bedpoint::back' }		
        elsif($changeType == 7 ){ $pay_type = 'bedpoint::top' }
        elsif($changeType == 9 ){ $pay_type = 'bedpoint::nearbylist' }		
        elsif($changeType == 10){ $pay_type = 'bedpoint::present::back' }
        elsif($changeType == 11){ $pay_type = 'bedpoint::present' }	
        elsif($changeType == 12){ $pay_type = 'bedpoint::wallet' }
		elsif($changeType == 13)
		{
			$pay_type = 'bedpoint::mora::start' if $changeDesc =~ /发起猜拳游戏消耗/ ;
			$pay_type = 'bedpoint::mora::join'  if $changeDesc =~ /参与猜拳游戏消耗/ ;
		}
		elsif($changeType == 14)
		{
			$pay_type = 'bedpoint::broadcast' if $changeDesc =~ /聊天室信息分享/ ;
			$pay_type = 'bedpoint::broadcast' if $changeDesc =~ /发普通广播/ ;
			$pay_type = 'bedpoint::shicaigame' if $changeDesc =~ /时彩游戏下注/ ;
            $pay_type = 'bedpoint::chatroom' if $changeDesc =~ /创建.*?聊天室/ ;
			
		}
	
		next unless $pay_type ;
        my $redis_key = 'CS::payin::'.$pay_type.'::user::'.$accountId . '::uuid::' . $logId . '_' .$time ;
		
        unless ($collection_payin->find_one({ "rediskey" => $redis_key }))
        {
            $collection_payin -> insert( {"rediskey" => $redis_key , "redisvalue" => $point} );
            say "MongoDB: $redis_key => $point" ;
        }
    
        $redis_db2 -> exists( $redis_key ) && next ;
        $redis_db2 -> set($redis_key , $point);
        say "Redis: $redis_key => $point" ;
        my $expire_time = 60 * 24 * 3600 ;			# 60 days expire
        $redis_db2 -> expire($redis_key ,  $expire_time) ;
	
	}
    
	$sth_pay_point -> finish;
}

#=cut

#=pod
# -----------------------------------------------------------------------------------
# 床上商城走L06的商城API，在这里单独统计，牵扯较多的业务逻辑，这里注明一下
# sourceType可能值为0，1，2   -- 0表示来自web端，1表示来自猜比赛，2表示来自床上
#
# ordersStatus字段表示订单的状态，可能取值如下(其中1、2、3、20、100都表示订单已经支付)：
#
# public static final Integer NOT_PAID = 0;				# /**买家未付款。*/
# public static final Integer TO_BE_SHIPPED = 1;		# /**买家已付款，等待卖家发货。*/
# public static final Integer TO_BE_RECEIVED = 2;		# /**卖家已发货，等待买家确认收货。*/
# public static final Integer RECEIVED = 3;				# /**买家已确认收货。*/
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
	my $redisvalue = $goods + $postage ;
	
    unless ($collection_payin->find_one({ "rediskey" => $redis_key }))
    {
            $collection_payin -> insert( {"rediskey" => $redis_key , "redisvalue" => $redisvalue} );
            say "MongoDB: $redis_key => $redisvalue" ;
    }
        
	$redis_db2 -> exists( $redis_key ) && next ;
	$redis_db2 -> set($redis_key , $redisvalue);
	say "Redis: $redis_key => $redisvalue" ;
    my $expire_time = 60 * 24 * 3600 ;			# 60 days expire
	$redis_db2 -> expire($redis_key ,  $expire_time) ;

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
	my $userMoney = $ref -> {ordersAmount} ;
	my $time    = $ref -> {cancelTime} ;	
	$time =~ s/ /_/ ;

	my $redis_key = 'CS::payin::mall::back::user::'.$accountId. '::uuid::' . $ordersId . '_' .$time ;
	
    unless ($collection_payin->find_one({ "rediskey" => $redis_key }))
        {
            $collection_payin -> insert( {"rediskey" => $redis_key , "redisvalue" => $userMoney} );
            say "MongoDB: $redis_key => $userMoney" ;
        }
    
        $redis_db2 -> exists( $redis_key ) && next ;
        $redis_db2 -> set($redis_key , $userMoney);
        say "Redis: $redis_key => $userMoney" ;
        my $expire_time = 60 * 24 * 3600 ;			# 60 days expire
        $redis_db2 -> expire($redis_key ,  $expire_time) ;
    
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
		
        unless ($collection_payin->find_one({ "rediskey" => $redis_key }))
        {
            $collection_payin -> insert( {"rediskey" => $redis_key , "redisvalue" => $num} );
            say "MongoDB: $redis_key => $num" ;
        }
        
		$redis_db2 -> exists( $redis_key ) && next ;
		$redis_db2 -> set($redis_key , $num);
		say "Redis: $redis_key => $num" ;
        my $expire_time = 60 * 24 * 3600 ;			# 60 days expire
        $redis_db2 -> expire($redis_key ,  $expire_time) ;
		
	}
	$sth_point_mall -> finish ;
	
}
$dbh -> disconnect ;


$lockfile->remove;

=cut



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

sub isBlock
{
    my ($dbh,$accountId) = @_ ;
    my $is ;
    my $sth = $dbh -> prepare(" SELECT accountId,blockFlag FROM nyx_1.account WHERE accountId = $accountId ") ;
    $sth -> execute();
	while (my $ref = $sth -> fetchrow_hashref())
	{
		my $block = $ref -> {blockFlag} ;
        $is = 1 if $block == 1 ;
	}
	$sth -> finish ;
	return $is ;
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
#=pod
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
#=cut