#!/usr/bin/env perl 
# ==============================================================================
# function:	*
# Author: 	kiwi
# createTime:	2014.6.18
# ==============================================================================
use 5.10.1 ;

BEGIN {
        my @PMs = (
		   #'Config::Tiny' ,
		   #'Date::Calc::XS'
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
use POSIX qw(strftime);
use Date::Calc::XS qw (Date_to_Time Time_to_Date);
use Unicode::UTF8 qw (decode_utf8 encode_utf8);
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

my $CBS_host      = $Config -> {CBS_DB}  -> {host};
my $CBS_db_match  = $Config -> {CBS_DB}  -> {database_match};
my $CBS_db_pay    = $Config -> {CBS_DB}  -> {database_pay};
my $CBS_usr       = $Config -> {CBS_DB}  -> {username};
my $CBS_password  = $Config -> {CBS_DB}  -> {password};

my $redis_host = $Config -> {REDIS} -> {host};
my $redis_port = $Config -> {REDIS} -> {port};

my $time_step = $Config -> {time} -> {step} ;	# 设置为往前推 N天 统计，默认 N = 1 ;
my $timestamp_now = int scalar time ;
my $timestamp_start = $timestamp_now - 86400 * $time_step  ;			
#my $time = strftime("%Y-%m-%d %H:%M:%S", localtime(time));
my $day_start = strftime("%Y-%m-%d 00:00:00",localtime(time() - 86400 * $time_step));

# ---------------------------------------------------------------------
# connect to redis
# ---------------------------------------------------------------------
#my $redis = Redis->new();
my $redis = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);

# ---------------------------------------------------------------------
# connect to mysql
# ---------------------------------------------------------------------
my $dsn_match = "DBI:mysql:database=$CBS_db_match;host=$CBS_host" ;
my $dbh_match = DBI -> connect($dsn_match, $CBS_usr, $CBS_password, {'RaiseError' => 1} ) ;
$dbh_match -> do ("SET NAMES UTF8");

my $dsn_pay  = "DBI:mysql:database=$CBS_db_pay;host=$CBS_host" ;
my $dbh_pay = DBI -> connect($dsn_pay, $CBS_usr, $CBS_password, {'RaiseError' => 1} ) ;
$dbh_pay -> do ("SET NAMES UTF8");

# -----------------------------------------------------------------------------
# 每日参与竞猜的用户 记录
# -----------------------------------------------------------------------------
#=pod
my $sth_match = $dbh_match -> prepare("SELECT userId,contestType,contestId,client,hasContent,createTime
				      FROM
				      roi_content_1.roi_content
				      where type = 1 and createTime > '$day_start'");

$sth_match -> execute();
while (my $ref = $sth_match -> fetchrow_hashref())
{
	my $userId       = $ref -> {userId} ;
	my $contestType  = $ref -> {contestType} ;
	my $contestId    = $ref -> {contestId} ;
	my $client       = $ref -> {client} ;
	my $hasContent   = $ref -> {hasContent} ;
	my $time         = $ref -> {createTime} ;
	$time =~ s/ /_/ ;
	#
	my $redis_key = "CBS::match{$contestId}{'TYPE',$contestType}{'UID',$userId}{'C',$hasContent}{'CLIENT','$client'}_$time" ;
	my $redis_key_utf8 = decode_utf8 $redis_key ;
	
	insert_redis_scalar( $redis_key_utf8,1 ) unless $redis -> exists($redis_key_utf8) ;
}
$sth_match -> finish ;
#=cut

# -----------------------------------------------------------------------------
# 用户竞猜消费流水 记录
# -----------------------------------------------------------------------------
my $sth_pay = $dbh_pay -> prepare("SELECT userId,money,type,ipaddress,logTime
				  FROM
				  roi_gold_log
				  WHERE logTime > '$day_start'");

$sth_pay -> execute();
while (my $ref = $sth_pay -> fetchrow_hashref())
{
	my $userId     = $ref -> {userId} ;
	my $money      = $ref -> {money} ;
	my $type       = $ref -> {type} ;
	my $ipaddress  = $ref -> {ipaddress} ;
	my $time       = $ref -> {logTime} ;
	$time =~ s/ /_/ ;
	my $redis_key = "CBS::money{'TYPE',$type}{'UID',$userId}{'IP','$ipaddress'}_$time" ;
	my $redis_key_utf8 = decode_utf8 $redis_key ;

	insert_redis_scalar( $redis_key_utf8 => $money ) unless $redis -> exists($redis_key_utf8);

}
$sth_pay -> finish ;

$dbh_match -> disconnect ;
$dbh_pay -> disconnect ;


# ==================================== functions =========================================

sub insert_redis_scalar
{
    my ($rediskey,$redisvalue) = @_ ;
    $redis->set($rediskey,$redisvalue);
    say "$rediskey => $redisvalue" ;
}

=pod

$redis -> set('key' => 'value');
$redis -> incr($key);
$redis -> rpush($key , $value);

# hello world
