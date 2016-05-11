#!/usr/bin/env perl 
# ==============================================================================
# function:	FT 项目统计 （新增用户，活跃用户...
# Author: 	kiwi
# createTime:	2014.11.2
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
use MongoDB;
use LWP::Simple;
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

my $mongo_host = $Config -> {MONGODB} -> {host};
my $mongo_port = $Config -> {MONGODB} -> {port};

my $time_step = $Config -> {time} -> {step} ;
my $timestamp_now = int scalar time ;
my $timestamp_start = $timestamp_now - 86400 * $time_step  ;			

my $day_start = strftime("%Y-%m-%d 00:00:00",localtime(time() - 86400 * $time_step));
my ($month_new) = $day_start =~ /^(\d+-\d+)-\d+/ ;

# ----------------------------------------------------------------
# connect to Redis & mongoDB
# ----------------------------------------------------------------
my $redis    = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
my $redis_ip = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
$redis_ip -> select(1) ;
#my $mongo = MongoDB::MongoClient->new(host => 'localhost', port => 27017);
my $mongo = MongoDB::MongoClient->new(host => $mongo_host, port => $mongo_port, query_timeout => 100000);

my $database_ft     = $mongo -> get_database( 'ft' );
my $collection_user = $database_ft -> get_collection( 'user' );


#=pod
# ---------------------------------------------------------------
# FT中的话题 
# ---------------------------------------------------------------
say "-> Redis.FT::A::content::topic::N_TIME" ;

for ( 1 .. $time_step)
{
    my $days_step = $_ ;
    
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    my $key_day_num = strftime("%Y%m%d", localtime(time() - 86400 * $days_step)) ;
    my $time_start = strftime("%Y-%m-%d 00:00:00",localtime(time() - 86400 * $days_step));
    my $time_end   = strftime("%Y-%m-%d 23:59:59",localtime(time() - 86400 * $days_step));
    foreach($redis->keys( 'FT::content::topic::*'.'_'.$key_day ) )
    {
	my %topic ;
	my $k = $_ ;
	my $v = $redis -> get($k) ;
	my $ref_topic = decode_json $v ;
	my $id = $ref_topic->{id} ;
	
	$topic{name} = $ref_topic->{name} ;
	my $time  = $ref_topic->{createTime} ;
	my $hot   = $ref_topic->{hot};
	my $content_num = $ref_topic->{content_num} ;
	
	# 判断这条话题是否为系统话题
	my $image = $ref_topic->{image_prefix} ;
	my $sys   = $image ? '1' : '0' ;
	
	my ($visit_day,$vist_avg) ;
	my $createday = substr $time , 0 , 10 ;
	$createday =~ s/-//g ;
	
	if ($time gt $time_start && $time lt $time_end) {
	    $visit_day = $hot ;
	    $vist_avg = $hot ;
	}
	else{
	    my $key_day_yest = strftime("%Y-%m-%d", localtime(time() - 86400 * ($days_step + 1))) ;
	    my $topic_yest = $redis -> get('FT::content::topic::'.$id.'_'.$key_day_yest) ;
	    my ($hot_yest) = $topic_yest =~ /"hot":"(\d+)"/ ;
	    $visit_day = $hot - $hot_yest ;
	    my $days = $key_day_num - $createday + 1;
	    $vist_avg = ($days == 0) ? $hot : sprintf("%.2f", $hot / $days);
	    #say "$id\t$name\t$time\t$hot\t$vist_avg" ;
	}
	
	$topic{id} = $id;
	$topic{system} = $sys ;
	$topic{visit_all} = $hot ;
	$topic{visit_day} = $visit_day ;
	$topic{visit_avg} = $vist_avg ;
	$topic{createTime} = $time ;
	$topic{content_num}= $content_num ;
	my $topic_info = encode_json \%topic;
	insert_redis_scalar('FT::A::content::topic::'.$id.'_'.$key_day  , $topic_info );
    }
    
}
#=cut

#=pod
# ---------------------------------------------------------------
# 新增设备 & 有效激活设备
# ---------------------------------------------------------------
say '-> Redis.FT::A::user::new::device*_DAY ' ;
for ( 1 .. $time_step)
{
    my $days_step = $_ ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    
    foreach( $redis->keys( 'FT::user::new::device*_'.$key_day) )
    {
        my $key = $_ ;
        my $num = $redis -> scard($key) ;
        if ($key =~ /^FT::(user::new::de.*?)_(\d+-\d+-\d+)$/) {
	    my ($temp,$time) = ($1,$2) ;
	    insert_redis_scalar( 'FT::A::'.$temp.'_'.$time , $num ) ;
        }
    }
    
}


say '-> Redis.FT::A::user::new::device*_MONTH ' ;
foreach($redis->keys( 'FT::user::new::device*_'.$month_new) )
{
    my $key = $_ ;
    my $num = $redis -> scard($key) ;
    if ($key =~ /^FT::(user::new::de.*?)_(\d+-\d+)$/) {
	my ($temp,$month) = ($1,$2) ;
	insert_redis_scalar( 'FT::A::'.$temp.'_'.$month , $num ) ;
    }
}
#=cut

#=pod
# ----------------------------------------------------------------
# 新增用户
# ----------------------------------------------------------------
say '-> Redis.FT::A::user::new_DAY ' ;
for ( 1 .. $time_step)
{
    my $days_step = $_ ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    my $time_start = strftime("%Y-%m-%d 00:00:00",localtime(time() - 86400 * $days_step));
    my $time_end   = strftime("%Y-%m-%d 23:59:59",localtime(time() - 86400 * $days_step));
    my %user_new ;
    my %user_ip ;
    my %user_market ;
    
    my $data_user = $collection_user -> find({'time' => { '$gte' => $time_start , '$lte' => $time_end}}); 
    while (my $ref = $data_user -> next)
    {
        my $accountId   = $ref -> {accountId} ;
	my $active_time = $ref -> {active_time} ;
        my $time      = $ref -> {time} ;
	my $gender    = $ref -> {gender} ;
        my $ip        = $ref -> {ip} ;
	my $version   = $ref -> {version} ;
        my $market    = $ref -> {market} ;
        
        my ($day,$hour) = $time =~ /^(\d+-\d+-\d+) (\d+):/ ;
        
        # user::ip
        my $city = $redis_ip->get($ip) ;
        my ($country,$province) = $city =~ /^(.*?)_(.*?)_.+$/ ;
        $user_ip{$country.'_'.$province} ++  if $province;
        
        my $rediskey_user_new                = "FT::A::user::new_".$day ;
	my $rediskey_user_new_version        = "FT::A::user::new::version::".$version.'_'.$day ;
	my $rediskey_user_new_market         = "FT::A::user::new::market::".$market.'_'.$day ;
	my $rediskey_user_new_market_version = "FT::A::user::new::market::".$market.'::version::'.$version.'_'.$day ;
	
	my $rediskey_user_new_gender         = 'FT::A::user::new::gender::'.$gender.'_'.$day ;
        my $rediskey_user_new_market_android = 'FT::A::user::new::android_'.$day ;
        my $rediskey_user_new_market_ios     = 'FT::A::user::new::ios_'.$day ;
        my $rediskey_user_new_hours          = 'FT::A::user::new::hour'.$hour.'_'.$day ;
	my $rediskey_user_new_hours_version  = 'FT::A::user::new::version::'.$version.'::hour'.$hour.'_'.$day ;
	my $rediskey_user_new_hours_market   = 'FT::A::user::new::market::'.$market.'::hour'.$hour.'_'.$day ;
	my $rediskey_user_new_hours_gender   = 'FT::A::user::new::hour'.$hour.'::gender::'.$gender.'_'.$day ;
        my $rediskey_user_new_ip             = 'FT::A::user::new::ip_'.$day ;
        
        $user_new{$rediskey_user_new} ++ ;
	$user_new{$rediskey_user_new_version} ++ ;
	$user_new{$rediskey_user_new_market} ++ if $market =~ /_\w+/;
	$user_new{$rediskey_user_new_market_version} ++ if $market =~ /_\w+/;
	
	$user_new{$rediskey_user_new_gender} ++ ;
        $user_new{$rediskey_user_new_market_android} ++ if $market =~ /Android/i ;
        $user_new{$rediskey_user_new_market_ios} ++ if $market =~ /iPhone/i ;
	
        $user_new{$rediskey_user_new_hours} ++;
	$user_new{$rediskey_user_new_hours_version} ++ ;
	$user_new{$rediskey_user_new_hours_market} ++ if $market =~ /_\w+/;
	$user_new{$rediskey_user_new_hours_gender} ++ ;
	
    }
    
    # -> Redis.FT::A::user::new::ip
    my $rediskey_user_ip = 'FT::A::user::new::ip_'.$key_day ;
    my $redisvalue_user_ip = join ';' , map { $_ . ',' . $user_ip{$_} } sort { $user_ip{$b} <=> $user_ip{$a} } keys %user_ip ;
    insert_redis_scalar($rediskey_user_ip , $redisvalue_user_ip) ;    

    # -> Redis.FT::A::user::new
    insert_redis_hash(\%user_new) ;

}

say '-> Redis.FT::A::user::new_MONTH ' ;
my %user_new_month ;
my %user_new_ip_temp ;
foreach($redis->keys( 'FT::A::user::new*' ) )
{
    my $key = $_ ;
    my $num = $redis -> get($key) ;
    my ($rediskey_user_new_month,$rediskey_user_new_all) ;
    if ($key =~ /^FT::A::user::new_(\d+-\d+)-\d+$/) {
	my $month = $1;
	$rediskey_user_new_month = "FT::A::user::new_" . $month ;
	$rediskey_user_new_all   = "FT::A::user::new" ;
	$user_new_month{$rediskey_user_new_month} += $num ;
	$user_new_month{$rediskey_user_new_all}   += $num ;
    }
    elsif($key =~ /^FT::A::user::new::(.*?)_(\d+-\d+)-\d+$/)
    {
	my ($key_type,$month) = ($1,$2) ;
	if ($key_type =~ /version|market|gender|android|ios|hour/){
	    $rediskey_user_new_month = "FT::A::user::new::".$key_type.'_'.$month ;
	    $rediskey_user_new_all   = 'FT::A::user::new::'.$key_type ;
	    $user_new_month{$rediskey_user_new_month} += $num ;
	    $user_new_month{$rediskey_user_new_all} += $num   ;
	}
	elsif($key_type eq 'ip'){
	    my @prov = split ';' , $num ;
	    foreach (@prov){
		my ($k,$v) = split ',' , $_ ;
		$user_new_ip_temp{$month}{$k} += $v ;
	    }
	}
    }
    next ;
}
insert_redis_hash(\%user_new_month) ;


foreach(keys %user_new_ip_temp){
    my $month = $_ ;
    my $rediskey   = 'FT::A::user::new::ip_' . $month ;
    my $redisvalue = join ';' , map { $_ . ',' . $user_new_ip_temp{$month}{$_} }
					sort { $user_new_ip_temp{$month}{$b} <=> $user_new_ip_temp{$month}{$a} }
					keys %{$user_new_ip_temp{$month}} ;
					
    insert_redis_scalar($rediskey => $redisvalue) ;
}
#=cut

#=pod
# ----------------------------------------------------------------
# 活跃用户
# ----------------------------------------------------------------
say '-> Redis.FT::A::user::active ' ;
for ( 1 .. $time_step)
{
    my $days_step = $_ ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    next unless $redis -> exists('FT::user::active_' .$key_day ) ;
    
    # -> Redis.FT::A::user::active_DAY
    my $nums_user_active = $redis -> scard( 'FT::user::active_'.$key_day );
    insert_redis_scalar('FT::A::user::active_'.$key_day , $nums_user_active) ;
    
    # -> Redis.FT::A::user::active::+_TIME
    my $nums_user_active_post = $redis -> scard( 'FT::user::active::+_'.$key_day );
    insert_redis_scalar('FT::A::user::active::+_'.$key_day , $nums_user_active_post) ;
    
    # -> Redis.FT::A::user::active::gender::*_DAY
    my @elements = $redis->smembers( 'FT::user::active_'.$key_day );
    my $nums_gender_0 ;
    foreach(@elements)
    {
	next unless /\d/ ;
	my $l99NO = $_ ;
	my $ref_user = get_user_from_l99NO($collection_user,$l99NO) ;
	my $gender = $ref_user -> {gender} ;
	$nums_gender_0 ++ if $gender eq '0' ;
    }
    my $rediskey_active_gender_0 = 'FT::A::user::active::gender::0_'.$key_day ;
    insert_redis_scalar($rediskey_active_gender_0 , $nums_gender_0) ;
    my $rediskey_active_gender_1 = 'FT::A::user::active::gender::1_'.$key_day ;
    insert_redis_scalar($rediskey_active_gender_1 , $nums_user_active - $nums_gender_0) ;
    

    
    foreach($redis->keys( 'FT::user::active::version::*'.'_'.$key_day ) )
    {
	my $key = $_ ;
	my $num = $redis -> scard($key) ;
	if ($key =~ /active::version::(\d+)_/) {
	    my $version = $1 ;
	    insert_redis_scalar( 'FT::A::user::active::version::'.$version.'_'.$key_day , $num ) ;
	}
    }
    
    foreach($redis->keys( 'FT::user::active*market::*'.'_'.$key_day) )
    {
	my $key = $_ ;
	my $num = $redis -> scard($key) ;
	if ($key =~ /user::(acti.*?market.*?)_/) {
	    my $temp = $1 ;
	    insert_redis_scalar( 'FT::A::user::'.$temp.'_'.$key_day , $num ) ;
	}
    }
}



my %user_active_month ;
my %user_active_ip_temp ;
foreach($redis->keys( 'FT::A::user::active::*' ) )
{
    my $key = $_ ;
    my $num = $redis -> get($key) ;
    if($key =~ /FT::A::user::active::(.*?)_(\d+-\d+)-\d+$/)
    {
	my ($key_type,$month) = ($1,$2) ;
	if ($key_type =~ /android|ios|hour|gender/){
	    $rediskey_user_active_month = "FT::A::user::active::".$key_type.'_'.$month ;
	    $user_active_month{$rediskey_user_active_month} += $num ;
	}
	elsif($key_type eq 'ip'){
	    my @prov = split ';' , $num ;
	    foreach (@prov){
		my ($k,$v) = split ',' , $_ ;
		$user_active_ip_temp{$month}{$k} += $v ;
	    }
	}
    }
    next ;
}
insert_redis_hash(\%user_active_month) ;

foreach(keys %user_active_ip_temp)
{
    my $month = $_ ;
    my $rediskey_active_ip   = 'FT::A::user::active::ip_' . $month ;
    my $redisvalue_active_ip = join ';' , map { $_ . ',' . $user_active_ip_temp{$month}{$_} }
					sort { $user_active_ip_temp{$month}{$b} <=> $user_active_ip_temp{$month}{$a} }
					keys %{$user_active_ip_temp{$month}} ;
					
    insert_redis_scalar($rediskey_active_ip => $redisvalue_active_ip) ;
}
#=cut

# 活跃设备相关的 日统计
say '-> Redis.FT::A::user::active::device*_DAY ' ;
for ( 1 .. $time_step)
{
    my $days_step = $_ ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    
    foreach( $redis->keys( 'FT::user::active::device*_'.$key_day) )
    {
        my $key = $_ ;
        my $num = $redis -> scard($key) ;
        if ($key =~ /^FT::(user::active::de.*?)_(\d+-\d+-\d+)$/) {
	    my ($temp,$time) = ($1,$2) ;
	    insert_redis_scalar( 'FT::A::'.$temp.'_'.$time , $num ) ;
        }
    }
    
}

# 活跃用户相关的 月统计 
say '-> Redis.FT::A::user::active*_MONTH ' ;
foreach($redis->keys( 'FT::user::active*_'.$month_new) )
{
    my $key = $_ ;
    my $num = $redis -> scard($key) ;
    if ($key =~ /^FT::(user::acti.*?)_(\d+-\d+)$/) {
	my ($temp,$month) = ($1,$2) ;
	insert_redis_scalar( 'FT::A::'.$temp.'_'.$month , $num ) ;
    }
}



#=pod
# ----------------------------------------------------------------
# 文章 排行 
# ----------------------------------------------------------------
say '-> Redis.FT::A::content::article::topN_TIME' ;
for ( 1 .. $time_step)
{
    my $days_step = $_ ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    my ($key_month) = $key_day =~ /^(\d+-\d+)-\d+/ ;
    $redis -> exists('FT::A::content::article::top1_' .$key_day ) && next ;
    
    my %content_ids ;

    my $v = $redis -> get('FT::content::article_'.$key_day) ;
    my @contentIds = split ';' , $v ;
    foreach(@contentIds){
	my ($cid,$nums) = split ',' , $_ ;
	$content_ids{$cid} = $nums ;
    }
    my $i = 1;
    foreach (sort {$content_ids{$b} <=> $content_ids{$a}} keys %content_ids)
    {
        last if $i > 40 ;	
        my $id = $_ ;
	my $nums = $content_ids{$id} ;
	my $dashboard_id = get_dashboard_id($id) ;
        my $rediskey_article_top = 'FT::A::content::article::top'.$i.'_'.$key_day ;
        insert_redis_scalar($rediskey_article_top , $dashboard_id.','.$nums) ;
        $i ++ ;
    }
    
    unless ($redis -> exists('FT::A::content::article::top1_' .$key_month ))
    {
	my %content_ids_month ;
	foreach($redis->keys( 'FT::content::article_'.$key_month.'-*' ) )
	{
	    my $k = $_ ;
	    my $v = $redis -> get($k) ;
	    foreach( split(';' , $v) ){
		my ($cid,$nums) = split ',' , $_ ;
		$content_ids_month{$cid} = $nums ;
	    }
	}
	my $j = 1;
	foreach (sort {$content_ids_month{$b} <=> $content_ids_month{$a}} keys %content_ids_month)
	{
	    last if $j > 40 ;	
	    my $id = $_ ;
	    my $nums = $content_ids_month{$id} ;
	    my $dashboard_id = get_dashboard_id($id) ;
	    my $rediskey_article_top_month = 'FT::A::content::article::top'.$j.'_'.$key_month ;
	    insert_redis_scalar($rediskey_article_top_month , $dashboard_id.','.$nums) ;
	    $j ++ ;
	}
    }
}
#=cut

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

sub insert_redis_scalar
{
    my ($rediskey,$redisvalue) = @_ ;
    $redis->set($rediskey,$redisvalue);
    say "$rediskey => $redisvalue" ;
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


sub get_dashboard_id
{
    my ($content_id) = @_ ;
    
    my $retry_times = 3 ;
    my ($i,$dashboard_id) ;
    while($i < $retry_times)
    {
	my $content = get("http://firsttime.l99.com/firsttime/rest/content/view?format=json&content_id=$content_id");
	($dashboard_id) = $content =~ /"dashboard_id":.*?(\d+),/ ;
	last if $dashboard_id ;
	$i ++ ;
    }
    return $dashboard_id ? $dashboard_id : 'R'.$content_id;
}



