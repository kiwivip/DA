#!/usr/bin/env perl 
# ==============================================================================
# function:	设计师
# Author: 	kiwi
# createTime:	2015.3.17
# ==============================================================================
use 5.10.1 ; 
use utf8 ;
use DBI ;
use Redis;
use Config::Tiny ;
use List::Util qw(first max maxstr min minstr sum);
use POSIX qw(strftime);
use Unicode::UTF8 qw[decode_utf8 encode_utf8];
binmode(STDIN, ':encoding(utf8)');
binmode(STDOUT, ':encoding(utf8)');
binmode(STDERR, ':encoding(utf8)');
$| = 1;
# -----------------------------------------------------------------------
# read config.ini
# -----------------------------------------------------------------------
my $Config = Config::Tiny->new;
$Config = Config::Tiny->read( '/home/DA/DataAnalysis/config.ini', 'utf8' );
my $version = $Config -> {_} -> {version};

my $redis_host = $Config -> {REDIS} -> {host};
my $redis_port = $Config -> {REDIS} -> {port};
my $L06_host     = $Config -> {L06_DB} -> {host};
my $L06_db       = $Config -> {L06_DB} -> {database} ;
my $L06_usr      = $Config -> {L06_DB} -> {username};
my $L06_password = $Config -> {L06_DB} -> {password};

my $DIR = '/home/DA/DataAnalysis/' ;
my $time_step = $Config -> {time} -> {step} ;	

my $time_now = strftime("%Y-%m-%d %H:%M:%S", localtime(time));     
my $day_yest = strftime("%Y-%m-%d", localtime(time() - 86400));	
my ($month_yest) = $day_yest =~ /^(\d+-\d+)-\d+/ ;				
my $redis = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);

# -------------------------------------------------------------------
# connect to mysql
# -------------------------------------------------------------------
my $dsn_v506 = "DBI:mysql:database=$L06_db;host=$L06_host" ;
my $dbh_v506 = DBI -> connect($dsn_v506, $L06_usr, $L06_password, {'RaiseError' => 1} ) ;
$dbh_v506 -> do ("SET NAMES UTF8");

# --------------------------------------------------------------------------------
for ( 1 .. $time_step)
{
    my $days_step = $_ ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    
    my %user_new ;
    
    my $sth_user = $dbh_v506 -> prepare("
                                        SELECT * FROM account_log WHERE
                                        createTime between '$key_day 00:00:00' and '$key_day 23:59:59'
                                        and market like '%esigner%' 
                                ");
    $sth_user -> execute();
    while (my $ref = $sth_user -> fetchrow_hashref())
    {
    	my $accountId = $ref -> {accountId} ;
        my $market    = $ref -> {market} ;
        my $time      = $ref -> {createTime} ;
        
        my ($day,$hour) = $time =~ /^(\d+-\d+-\d+) (\d+):/ ;
        my $ref_account_2 = get_user_from_accountId_2($dbh_v506,$accountId) ;
	my $gender = $ref_account_2 -> {gender} ;
        
        my $rediskey_user_new               = "DS::A::user::new_".$day ;
        my $rediskey_user_new_gender        = 'DS::A::user::new::gender::'.$gender.'_'.$day ;
        my $rediskey_user_new_android       = 'DS::A::user::new::android_'.$day ;
        my $rediskey_user_new_ios           = 'DS::A::user::new::ios_'.$day ;
        my $rediskey_user_new_hours         = 'DS::A::user::new::hour'.$hour.'_'.$day ;
        my $rediskey_user_new_hours_gender  = 'DS::A::user::new::hour'.$hour.'::gender::'.$gender.'_'.$day ;
        
        $user_new{$rediskey_user_new} ++ ;
	$user_new{$rediskey_user_new_gender} ++ ;
        $user_new{$rediskey_user_new_android} ++ if $market =~ /ForGPhone/i ;
        $user_new{$rediskey_user_new_ios} ++ if $market =~ /ForiPhone/i ;
	
        $user_new{$rediskey_user_new_hours} ++;
	$user_new{$rediskey_user_new_hours_gender} ++ ;
        
    }
    $sth_user -> finish ;
    
    # -> Redis.DS::A::user::new
    insert_redis_hash(\%user_new) ;
}

say '-> Redis.DS::A::user::new_MONTH ' ;
my %user_new_month ;

foreach($redis->keys( 'DS::A::user::new*' ) )
{
    my $key = $_ ;
    my $num = $redis -> get($key) ;
    
    my ($rediskey_user_new_month,$rediskey_user_new_all) ;
    if ($key =~ /^DS::A::user::new_(\d+-\d+)-\d+$/) {
	my $month = $1;
	$rediskey_user_new_month = "DS::A::user::new_" . $month ;
	$rediskey_user_new_all   = "DS::A::user::new" ;
	$user_new_month{$rediskey_user_new_month} += $num ;
	$user_new_month{$rediskey_user_new_all}   += $num ;
    }
    elsif($key =~ /^DS::A::user::new::(.*?)_(\d+-\d+)-\d+$/)
    {
	my ($key_type,$month) = ($1,$2) ;

	$rediskey_user_new_month = "DS::A::user::new::".$key_type.'_'.$month ;
	$rediskey_user_new_all   = 'DS::A::user::new::'.$key_type ;
	$user_new_month{$rediskey_user_new_month} += $num ;
	$user_new_month{$rediskey_user_new_all} += $num   ;
    }
    next ;
}
insert_redis_hash(\%user_new_month) ;

# =======================================================================================================
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