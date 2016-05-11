#!/usr/bin/env perl 
# ==============================================================================
# function:	*
# Author: 	kiwi
# createTime:	2014.6.19
# ==============================================================================
use 5.10.1 ;
BEGIN {
    my @PMs = (
	    #'Math::GSL::Sort',
            #'Math::GSL::Statistics',
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
#use Math::GSL::Sort qw/:all/;
use Config::Tiny ;
#use Math::GSL::Statistics qw /:all/;
use POSIX qw(strftime);
use Unicode::UTF8 qw[decode_utf8 encode_utf8];
binmode(STDIN, ':encoding(utf8)');
binmode(STDOUT, ':encoding(utf8)');
binmode(STDERR, ':encoding(utf8)');

# ------------------------------------------------------------------------------

$| = 1;
# -----------------------------------------------------------------------
# read config.ini
# -----------------------------------------------------------------------
my $Config = Config::Tiny->new;
$Config = Config::Tiny->read( '/home/DA/DataAnalysis/config.ini', 'utf8' );
my $version = $Config -> {_} -> {version};
my $time_step = $Config -> {time} -> {step} ;

my $redis_host = $Config -> {REDIS} -> {host};
my $redis_port = $Config -> {REDIS} -> {port};

# ----------------------------------------------------------------
# connect to Redis 
# ----------------------------------------------------------------
my $redis = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
#my $redis = Redis->new();

# -------------------------------------------------------------------
# CBS::A::user::active & CBS::A::content::match
# -------------------------------------------------------------------
#=pod
my (%user,%match) ;
foreach($redis->keys( 'CBS::match*' ) )
{
    my $key = $_ ;
    my ($matchId,$matchType,$userId,$content,$client,$date) = $key =~ /CBS::match{(\d+)}{'TYPE',(\d)}{'UID',(\d+)}{'C',(\d+)}{'CLIENT','(.*?)'}_([-\d]+)_/ ;
    next unless $date ;
    my ($month) = $date =~ /^(\d+-\d+)-/ ;
    
    # 每日/月/总 参与竞猜的用户数量
    my $key_user = "CBS::A::user::active_".$date ;
    my $key_user_month = "CBS::A::user::active_".$month ;
    my $key_user_all = "CBS::A::user::active" ;
    
    # 每日/月/总 用户参与的竞猜比赛数量
    my $key_match = "CBS::A::content::match::type::".$matchType .'_'. $date;
    my $key_match_month = "CBS::A::content::match::type::".$matchType.'_'.$month ;
    my $key_match_all = "CBS::A::content::match::type::".$matchType ;
    
    # 每日/月/总 带有用户竞猜内容的比赛数量
    my $key_match_content = "CBS::A::content::match::yes_" . $date ;
    my $key_match_content_month = "CBS::A::content::match::yes_" . $month ;
    my $key_match_content_all = 'CBS::A::content::match::yes' ;
    my $key_match_nocontent = "CBS::A::content::match::no_" . $date ;
    my $key_match_nocontent_month = "CBS::A::content::match::no_" . $month ;
    my $key_match_nocontent_all = 'CBS::A::content::match::no' ;
    
    $user{$key_user}{$userId} = 1 ;
    $user{$key_user_month}{$userId} = 1 ;
    $user{$key_user_all}{$userId} = 1 ; 
    
    $match{$key_match}{$matchId} = 1 ;
    $match{$key_match_month}{$matchId} = 1 ;
    $match{$key_match_all}{$matchId} = 1 ;
    
    if ($content == 1) {
        $match{$key_match_content}{$matchId} = 1 ;
        $match{$key_match_content_month}{$matchId} = 1 ;
        $match{$key_match_content_all}{$matchId} = 1 ;
    }else{
	$match{$key_match_nocontent}{$matchId} = 1 ;
        $match{$key_match_nocontent_month}{$matchId} = 1 ;
        $match{$key_match_nocontent_all}{$matchId} = 1 ;
    }

}

foreach (keys %user){
    my $key = $_ ;
    my $value = ~~ keys %{$user{$key}} ;
    insert_redis_scalar($key,$value);
}
foreach (keys %match){
    my $key = $_ ;
    my $value = ~~ keys %{$match{$key}} ;
    insert_redis_scalar($key,$value);
}
#=cut

# -------------------------------------------------------------------------
# CBS::A::pay
# -------------------------------------------------------------------------
#=pod
my $key_month = strftime("%Y-%m", localtime(time() - 86400 * $time_step)) ;
my %pay ;
for ( 1 .. $time_step)
{
    my $days_step = $_ ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    foreach($redis->keys( 'CBS::money*_'.$key_day.'*' ) )
    {
	my $key = $_ ;                      # CBS::money{'TYPE',8}{'UID',1429056}{'IP','211.140.5.119'}_2014-06-19_09:52:00
	my $pay = $redis->get($key);        # 20.30
	my ($type,$userId,$ip,$date) = $key =~ /CBS::money{'TYPE',(\d)}{'UID',(\d+)}{'IP','(.*?)'}_([-\d]+)_/ ;
	next unless $date ;
	my ($month) = $date =~ /^(\d+-\d+)-/ ;
	my $key_pay_in_day  = "CBS::A::payin::match_" . $date ;
	my $key_pay_out_day = "CBS::A::payout::match_". $date ;
	my $key_pay_signout = "CBS::A::payout::sign_" . $date ;
	
	$pay{$key_pay_in_day}  += abs($pay) if $type == 3;
	$pay{$key_pay_out_day} += $pay if $type == 8;
	$pay{$key_pay_signout} += $pay if $type == 1;
    }
}
foreach (keys %pay){
    my $key = $_ ;
    my $value = sprintf('%.2f',$pay{$key}) ;
    insert_redis_scalar($key,$value);
}

my %pay_all ;
foreach($redis->keys( 'CBS::A::pay*' ) )
{
    my $key = $_ ;
    my $pay = $redis -> get($key) ;
    if ($key =~ /^CBS::A::(pay.*?)_(\d+-\d+)-\d+$/) {
	my ($type,$month) = ($1,$2) ;
	my $key_month = 'CBS::A::' . $type . '_' . $month ;
	my $key_all   = 'CBS::A::' . $type ;
	
	$pay_all{$key_month} += $pay ;
	$pay_all{$key_all} += $pay ;
    }
}
insert_redis(\%pay_all) ;

#=cut


# ===================================== functions =====================================
sub insert_redis
{
    my ($ref) = @_ ;
    foreach (keys %$ref){
        my $key = $_ ;
        my $value = $ref->{$key} ;
    
        say "$key => $value" ;
        $redis->set($key,$value);
    }
}

sub insert_redis_scalar
{
    my ($rediskey,$redisvalue) = @_ ;
    $redis->set($rediskey,$redisvalue);
    say "$rediskey => $redisvalue" ;
}
=pod
my @list = $redis->lrange( $key, 0, -1 );



sub median
{
    my ($list) = @_ ;
    my $stride = 1 ;
    my $sorted  = gsl_sort($list , 1 , $#$list + 1 );
    gsl_stats_median_from_sorted_data($sorted, $stride, $#$list + 1) ;
    
}

sub mean
{
    my ($list) = @_ ;
    gsl_stats_mean($list , 1 , $#$list + 1);
}