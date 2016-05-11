#!/usr/bin/env perl 
# ==============================================================================
# function:	设计师 项目统计
# Author: 	kiwi
# createTime:	2015.3.17
# ==============================================================================
use 5.10.1 ;
BEGIN {
    my @PMs = (
	    #'Config::Tiny' ,
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
use DBI ;
use Redis;
use Config::Tiny ;
use JSON::XS ;
use POSIX qw(strftime);
use Unicode::UTF8 qw[decode_utf8 encode_utf8];
binmode(STDIN, ':encoding(utf8)');
binmode(STDOUT, ':encoding(utf8)');
binmode(STDERR, ':encoding(utf8)');
# --------------------------------------------------------------------------------

# -----------------------------------------------------------------------
# read config.ini
# -----------------------------------------------------------------------
my $Config = Config::Tiny -> new;
$Config = Config::Tiny -> read( '/home/DA/DataAnalysis/config.ini', 'utf8' );
my $version = $Config -> {_} -> {version};
my $redis_host = $Config -> {REDIS} -> {host};
my $redis_port = $Config -> {REDIS} -> {port};

my $redis    = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);

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


# ===================================== functions =====================================

# -------------------------------------------------
# input: ref_hash
# -------------------------------------------------
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