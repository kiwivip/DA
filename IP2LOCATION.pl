#!/usr/bin/env perl 
# ==============================================================================
# function:	*
# Author: 	kiwi
# createTime:	2014.7.17
# ==============================================================================

use 5.10.1 ;						 
use utf8 ;
use MaxMind::DB::Reader ;
use Redis;
use MongoDB;
use Config::Tiny ;
use Unicode::UTF8 qw[decode_utf8 encode_utf8];
use POSIX qw(strftime);
binmode(STDIN, ':encoding(utf8)');
binmode(STDOUT, ':encoding(utf8)');
binmode(STDERR, ':encoding(utf8)');
# ---------------------------------------------------------------------------------

my $maxmind_reader = MaxMind::DB::Reader->new( file => '/home/DA/DataAnalysis/GeoLite2-City.mmdb' );

# -----------------------------------------------------------------------
# read config.ini
# -----------------------------------------------------------------------
my $Config = Config::Tiny->new;
$Config = Config::Tiny->read( '/home/DA/DataAnalysis/config.ini', 'utf8' );
my $version = $Config -> {_} -> {version};
my $redis_host = $Config -> {REDIS} -> {host};
my $redis_port = $Config -> {REDIS} -> {port};
my $redis_ip = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
$redis_ip -> select(1) ;

#my $mongo = MongoDB::MongoClient->new(host => $mongo_host, port => $mongo_port, query_timeout => 100000);
#my $database_ip = $mongo -> get_database( 'location' );
#my $collection_ip = $database_ip -> get_collection( 'ip' );

my $LOGDIR = '/logback/tongji_data/LOG/' ;
my @hosts = ('192.168.199.14','192.168.199.69') ;

my @dirs = (
            'api.nyx.l99.com', 		# 床上
            'api.wwere.l99.com',            # 在哪
            'cbs.l99.com',                      	# 猜比赛
            'firsttime.l99.com',               # 第一次
            'www.l99.com'                       # 立方网
) ;
my @logs ;
foreach(@hosts)
{
    my $host = $_ ;    
    my ($dir_num) = $host =~ /\.(\d+)$/ ; 
    push @logs , map {$LOGDIR.$_.'/'.$dir_num.'/'.'access.'.$_.'_'  } @dirs ;
}

for ( 1 .. 1)
{
    my $time_step = $_ ;
    my $log_day = strftime( "%Y%m%d", localtime(time()- 86400 * ($time_step + 0) ) ) ;    
    my @temp = @logs ;
    foreach (@temp)
    {
	my $file_l99_log = $_.$log_day.'.log.gz' ; 
	next unless -e $file_l99_log ;
	say $file_l99_log ;
	open my $fh_log , "gzip -dc $file_l99_log|" ;
	while(<$fh_log>)
	{
	    chomp;
	    my $s = substr $_ , -40 ;
	    my ($ip) = $s =~ /(\d+\.\d+\.\d+\.\d+)/ ;
	    next unless $ip ;
	    next if $ip =~ /^0/ ;
	    next if $ip =~ /^192.168./ ;
	    
	    # insert into mongodb
	    #next if $collection_ip->find_one({ ip => $ip }) ;
	    #my $location = geoIP($ip) ;
	    #$collection_ip -> insert($location) ;
		
	    # insert into redis			
	    $redis_ip -> exists($ip) && next ;
	    my $record = $maxmind_reader -> record_for_address($ip);
	    my $geo = geoIP($record) ;
	    next unless $geo ;
	    $redis_ip -> set($ip , encode_utf8 $geo)  ;
	    my $expire_time = 10 * 24 * 3600 ;			# 10 days expire
	    $redis_ip -> expire($ip ,  $expire_time) ;
	    eval{say "$ip => ". decode_utf8 $geo };
	}
    }
}

# ================================== function =====================================

sub geoIP
{
    my ($record) = @_ ;
    my $country = $record->{country}->{names}->{en};
    my ($subdivisions,$city) ;
    if ($country eq 'China') {
	$country = '中国' ;
	$subdivisions = $record->{subdivisions}->[0]->{names}->{'zh-CN'};
	$city         = $record->{city}->{names}->{'zh-CN'};
    }else{
	$subdivisions = $record->{subdivisions}->[0]->{names}->{en};
	$city = $record->{city}->{names}->{en};
    }
    #my $ref_location = {'ip' => $ip , 'country' => $country , 'subdivisions' => $subdivisions , 'city' => $city} ;
    #return $ref_location ;
    my $geo = $country.'_'.$subdivisions.'_'.$city ;
    return if $geo eq '__' ;
    return $geo ;
}

