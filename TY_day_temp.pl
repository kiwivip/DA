#!/usr/bin/env perl 
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
                        `cpanm $pm`;
                }
        }
}

use utf8 ;
use Redis;
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
my $L06_db       = $Config -> {L06_DB} -> {database};
my $L06_usr      = $Config -> {L06_DB} -> {username};
my $L06_password = $Config -> {L06_DB} -> {password};

my $TY_host     = $Config -> {TY_DB} -> {host};
my $TY_db       = $Config -> {TY_DB} -> {database} ;
my $TY_usr      = $Config -> {TY_DB} -> {username};
my $TY_password = $Config -> {TY_DB} -> {password};

my $time_step = 700 ;
#my $time_step = $Config -> {time} -> {step} ;													# 设置为往前推 N天 统计，默认 N = 1 	
my $day_start = strftime( "%Y-%m-%d 00:00:00" , localtime(time() - 86400 * $time_step) );		# %Y-%m-%d %H:%M:%S
my ($sec,$min,$hour,$dday,$mmon,$yyear,$wday,$yday,$isdst) = localtime(time - 86400 * $time_step);    
$yyear += 1900;    
my $timestamp_start = timelocal(0,  0,  0 , $dday , $mmon, $yyear);
my $num_month_ago = $time_step / 30 + 1;

my $dsn_ty = "DBI:mysql:database=$TY_db;host=$TY_host" ;
my $dbh_ty = DBI -> connect($dsn_ty, $TY_usr, $TY_password, {'RaiseError' => 1} ) ;
$dbh_ty -> do ("SET NAMES UTF8");

my $dsn_v506 = "DBI:mysql:database=$L06_db;host=$L06_host" ;
my $dbh_v506 = DBI -> connect($dsn_v506, $L06_usr, $L06_password, {'RaiseError' => 1} ) ;
$dbh_v506 -> do ("SET NAMES UTF8");

my $redis = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);


# ----------------------------------------------------------------
# 新增用户统计	TY::A::user::new
# ----------------------------------------------------------------
say "-> Redis.TY::A::user::new*_DAY" ;
for ( 1 .. $time_step + 1)
{
    my $days_step = $_ - 1 ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    
	my $num_new ;
    my ($num_gender_0,$num_gender_1) ;			# 新增用户分性别数量
    my ($num_ios,$num_android) ;				# 新增用户分系统数量
    my %user ;
    
    my $sth_user = $dbh_v506 -> prepare("
                                        SELECT accountId,createTime,market
                                        FROM 
                                        account_log
                                        where
                                        market regexp 'TiyuFor' and
                                        createTime between '$key_day 00:00:00' and '$key_day 23:59:59'
                                    ");
        $sth_user -> execute();
        while (my $ref = $sth_user -> fetchrow_hashref())
        {
        	my $accountId  = $ref -> {accountId} ;
        	my $time       = $ref -> {createTime} ;
            my $market     = $ref -> {market} ;
        	my ($hour)     = $time =~ / (\d+):\d+:\d+$/ ;
            
            my $ref_account = get_gender($dbh_ty , 'user_tiyu' , $accountId) ;
            my $gender = $ref_account->{gender} ;
            my $l99NO  = $ref_account->{l99NO}  ;
            
        	$user{'TY::A::user::new::hour'.$hour.'::gender::'.$gender.'::'.$os.'_'.$key_day} ++ if $os;
        	$user{'TY::A::user::new::hour'.$hour.'::gender::'.$gender.'_'.$key_day} ++ ;
        	$user{'TY::A::user::new::hour'.$hour.'::'.$os.'_'.$key_day} ++ if $os;	
			
			$num_new ++ ;
			$num_gender_0 ++  if $gender == 0 ;
        	$num_gender_1 = $num_new - $num_gender_0 ;
        	$num_ios ++ if $market =~ /TiyuForIphone/i ;
        	$num_android = $num_new - $num_ios ;
	    
        }
    $sth_user -> finish ;
    
    insert_redis_scalar('TY::A::user::new::gender::0_'.$key_day , $num_gender_0) if $num_gender_0;
    insert_redis_scalar('TY::A::user::new::gender::1_'.$key_day , $num_gender_1) if $num_gender_1;
    insert_redis_scalar('TY::A::user::new_'.$key_day , $num_new) if $num_new;
    insert_redis_scalar('TY::A::user::new::ios_'    .$key_day , $num_ios)     if $num_ios ;
    insert_redis_scalar('TY::A::user::new::android_'.$key_day , $num_android) if $num_android ;
    
    insert_redis_hash(\%user) ;	
  
}

say "-> Redis.CS::A::user::new*_MONTH" ;

for ( 1 .. $num_month_ago )	
{
	my $month_ago = $_ - 1 ;
	my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	say $month ;
	
	my %user_new_month ;
	foreach( $redis->keys( 'TY::A::user::new*'.'_'.$month.'-*' ) )
	{
		my $key = $_ ;		
		my $type ;
		if ($key =~ /^TY::A::user::new(.*?)_[\d\-]+$/ )
		{
			$type = $1 ;
			next if $type =~ /auth/ ;
			my $num = $redis->get($key);
			$user_new_month{'TY::A::user::new'.$type.'_'.$month  } += $num ;
		}
	}

	insert_redis_hash(\%user_new_month) ;
}




say '-> Redis.TY::A::user::new::auth_TIME' ;
my %auth_type = (
	'10' => 'Email' , '20' => '手机号'  , '110' => 'QQ微博' , '111' => 'QQ' ,
	'120' => '新浪微博' , '130' => '搜狐微博' , '240' => '微信'
) ;
for ( 1 .. $time_step + 1 )
{
    my $days_step = $_ - 1 ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
	
	my $timestamp_l = local2unix($key_day.' 00:00:00') * 1000 ;
	my $timestamp_r = local2unix($key_day.' 23:59:59') * 1000 ;
	my %temp ;
	my $sth_auth = $dbh_v506 -> prepare("
										SELECT r.authId,r.accountId,r.authType,l.market
										FROM
                                        account_log l left join account_authentication r 
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
		$temp{$type} ++ if $market =~ /Tiyu/i ;
	}
	$sth_auth -> finish ;
	
	my $auth_info = encode_json \%temp ;
	insert_redis_scalar('TY::A::user::new::auth_'.$key_day , $auth_info) ;
}

say '-> Redis.TY::A::user::new::auth_MONTH' ;

for ( 1 .. $num_month_ago )	
{
	my $month_ago = $_ - 1 ;
	my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	
	my %temp_month ;
	foreach( $redis->keys( 'TY::A::user::new::auth_'.$month.'-*' ) )
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
	insert_redis_scalar('TY::A::user::new::auth_'.$month , $info) ;
}



# =================================== functions =============================================

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

sub get_gender
{
	my ($dbh,$table,$accountId) = @_ ;
	my $ref_accountId ;
	my $sth_user = $dbh -> prepare(" SELECT userId,gender,userNO FROM $table WHERE userId = $accountId ");
    $sth_user -> execute();
    while (my $ref = $sth_user -> fetchrow_hashref())
    {
        	my $accountId  = $ref -> {userId} ;
        	my $gender     = $ref -> {gender} ;
			my $l99NO      = $ref -> {userNO} ;
			$ref_accountId -> {gender}  = $gender ;
			$ref_accountId -> {l99NO}   = $l99NO ;
	}
	$sth_user -> finish ;
	return $ref_accountId ;
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