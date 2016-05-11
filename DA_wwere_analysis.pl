#!/usr/bin/env perl 
# ==============================================================================
# function:	Wwere 相关统计
# Author: 	kiwi
# createTime:	2014.9.4
# ==============================================================================

use 5.14.2 ;	# Ubuntu 12.04LTS 默认版本
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
use Redis;
use MongoDB ;
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
$Config = Config::Tiny->read( '/home/kiwi/Other/DataAnalysis/config.ini', 'utf8' );
my $version = $Config -> {_} -> {version};
my $redis_host = $Config -> {REDIS} -> {host};
my $redis_port = $Config -> {REDIS} -> {port};
my $mongo_host = $Config -> {MONGODB} -> {host};
my $mongo_port = $Config -> {MONGODB} -> {port};
my $time_step  = $Config -> {time} -> {step} ;					# 设置为往前推 N天 统计；一般地，N = 1

# -------------------------------------------------------------------
# connect to Redis & mongoDB
# -------------------------------------------------------------------
my $redis = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
my $mongo  = MongoDB::MongoClient->new(host => $mongo_host, port => $mongo_port ,query_timeout => 100000);	# 100s timeout
my $db_wwere = $mongo -> get_database( 'wwere' ) ;
my $collection_poi = $db_wwere -> get_collection( 'poi' );

# -------------------------------------------------------------------
# WWERE::A::poi_time  月度POI增长数量
# -------------------------------------------------------------------
say "-> WWERE::A::poi Nums " ;
for ( 1 .. $time_step)
{
    my $days_step = $_ ;
    my $key_month = strftime("%Y-%m", localtime(time() - 86400 * $days_step)) ;
    my $rediskey_poi_month = 'WWERE::A::poi_' . $key_month ;
    
    # 这里统计目标月份的新增 POI 数量
    my $num = $collection_poi -> find({"createTime" => {'$gte' => $key_month . '-01 00:00:00' , '$lte' => $key_month . '-31 23:59:59'} }) -> count;
    next if $redis -> get($rediskey_poi_month) == $num ;
    insert_redis_scalar($rediskey_poi_month , $num) ;
}

# ---------------------------------------------------------------
# WWERE::A::poi::country POI的地域分布，按国家区分
# ---------------------------------------------------------------
say "-> WWERE::A::poi::country " ;
my $result_country = $db_wwere -> run_command(
    [
       "distinct" => "poi",
       "key"      => "countryId",
       "query"    => {}
    ]
);
foreach(@{$result_country->{values}}){
    my $countryId = $_ ;
    my $num = $collection_poi -> count({countryId => $countryId}) ;
    
    my $rediskey_poi_country = 'WWERE::A::poi::country_' . $countryId ;
    insert_redis_scalar($rediskey_poi_country,$num) ;
}


# ==================================== function ====================================

sub insert_redis_scalar
{
    my ($rediskey,$redisvalue) = @_ ;
    $redis->set($rediskey,$redisvalue);
    say "$rediskey => $redisvalue" ;
}

sub insert_redis
{
    my ($ref) = @_ ;
    foreach (keys %$ref){
        my $key = $_ ;
        my $value = $ref->{$key} ;
        $redis->set($key,$value);
	say "$key => $value" ;
    }
}