#!/usr/bin/env perl 
# ==============================================================================
# Author: 	    kiwi
# createTime:	2014.6.30
# ==============================================================================
use 5.10.1 ;
use utf8 ;
use autodie ;
use Data::Dumper ;
use MaxMind::DB::Reader ;
use Redis;
use MongoDB ;
use JSON::XS ;
use URI::Split qw(uri_split uri_join);
use LWP::Simple;
use Config::Tiny ;
use Woothee;
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

my $L99_log_dir  = $Config -> {L99_LOG} -> {dir} ;
my $article_topN = $Config -> {L99_LOG} -> {article_topN} ;

my $maxmind_reader = MaxMind::DB::Reader->new( file => '/home/DA/DataAnalysis/GeoLite2-City.mmdb' );

my $redis_host = $Config -> {REDIS} -> {host};
my $redis_port = $Config -> {REDIS} -> {port};


my $time_step = $Config -> {time} -> {step} ;		# 设置为往前推 N天 统计，默认 N = 1 ;
my $timestamp_now = int scalar time ;
my $timestamp_start = $timestamp_now - 86400 * $time_step  ;	

# ---------------------------------------------------------------------
# connect to Redis & mongoDB
# ---------------------------------------------------------------------
my $redis    = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
my $redis_ip = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
$redis_ip -> select(1) ;
#my $mongo = MongoDB::MongoClient->new(host => '192.168.201.59', port => 27017);

# ---------------------------------------------------------------------
my %num2month = (
	'01' => "Jan" , '02' => "Feb" , '03' => "Mar" , '04' => "Apr" ,
	'05' => "May" , '06' => "Jun" , '07' => "Jul" , '08' => "Aug" ,
	'09' => "Sep" , '10' => "Oct" , '11' => "Nov" , '12' => "Dec"
);
my %month2num = reverse %num2month ;
# ---------------------------------------------------------------------
# L99::article::weixin & L99::download::weixin
# ---------------------------------------------------------------------

for ( 1 .. $time_step)
{
    my $days = $_ ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days)) ;		# redis_key的日期后缀格式
    my ($key_month,$key_day_numbers) = $key_day =~ /(\d+-\d+)-(\d+)/ ; 
    my $log_day = strftime("%Y%m%d", localtime(time()- 86400 * $days)) ;		# 指日志文件的后缀日期格式
    
    $redis -> exists('L99::A::content::article::weixin::top1_'.$key_day) && next ;	# 如果这一天的微信统计数据已经有了，跳过这一天的日志扫描
    
    my ($send,$like,$reply,$follow,$friend) ;
    
    my ($pv,$pv_spider,$source_direct,$source_direct_spider) ;
    my ($login,%ips) ;
    my (%weixin_text,%weixin_download) ;					# 微信 阅读单篇 & APP下载
    my (%l99_text,%l99_text_city) ;						# 存储 访问单篇的ip地域分布
    my (%active_hours,%active,%active_ip) ;
    my %sources ;								# 立方网平台流量来源站点 host
    my %timeline_page ;								# 时间轴滚动时每页的翻动次数
    my %l99_zone ;								# 个人空间访问次数
    my @dirs_ip = (14,69) ;     						# 199.14 & 199.69
    
    for(@dirs_ip)
    {
	my $ip = $_ ;
	my $file_l99_log = $L99_log_dir . $ip . '/access.www.l99.com_' . $log_day . '.log.gz' ;
        next unless -e $file_l99_log ;
	
	
        open my $fh_log , "gzip -dc $file_l99_log|" ;				# 日志文件目前以gz在本地存储了
	while(<$fh_log>)
	{
	    chomp;
	    my $log = $_ ;
	    # e.g.
            # 192.168.199.57 www.l99.com - [18/Jun/2014:18:18:53 +0800] "GET /timeline.action?dt=6 HTTP/1.1" 302 0 "http://www.l99.com/timeline.action" "Mozilla/4.0" "-" http 180.153.114.197 0.005 0.004 .
	    # 192.168.199.57 www.l99.com - [29/Jun/2014:00:00:16 +0800] "GET /EditText_view.action?textId=1627694&cf=true HTTP/1.1" 200 9947 "http://mp.weixin.qq.com/mp/redirect?url=http%3A%2F%2Fwww.l99.com%2FEditText_view.action%3FtextId%3D1627694%26cf%3Dtrue%23rd" "Mozilla/5.0 (Linux; Android 4.4.2; SM-G9008V Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Mobile Safari/537.36 MicroMessenger/5.3.1.50_r732663.462 NetType/WIFI" "3.11" http 27.16.170.144 0.202 0.202 .
	    #my ($hour,$url,$ref,$ip) = $_ =~ /^\S+ \S+ - \[.*?:(\d+):\d+:\d+ [0-9\+]+\] "\S+ (.*?) \S+" \S+ \S+ "(.*)" ".*" ".*" \S+ ([\.\d]+) [\s\S]+/ ;
	    
	    my ($day,$month,$year,$time_hms,$url,$ref,$useragent) = $log =~ /^\S+ \S+ \S+ \[(\d+)\/([a-zA-Z]+)\/(\d+):(\d+:\d+:\d+) \S+] "\S+ (.*) \S+" \S+ \S+ "(.*)" "(.*)" ".*"/ ;
            my $day_log = $year.'-'.$month2num{$month}.'-'.$day ;
	    my ($hour) = $time_hms =~ /^(\d+):/ ;
	    my $time = $day_log.' '.$time_hms ;
	    
	    my $tail_log = substr $log , -40 ;					# 这里单独截取ip
	    my ($ip) = $tail_log =~ /(\d+\.\d+\.\d+\.\d+)/ ;
	    next unless $ip ;
# ----------------------------------------------------------------------------------------------------

	    $ips{$ip} = 1 ;
	    
	    # 来源站点 host
	    my $source ;						
            if($ref =~ /[a-zA-Z]/)
	    {
                $source = uri_auth($ref) ;
                $sources{$source} ++ if $source =~ /\./ ;
            }
#=pod    
	    # ----------------------------------
	    # PV 统计 (按是否为 spider 区分统计)
	    # ----------------------------------
	    my $isSpider = isSpider($useragent) ;
	    if( isPage($url) ){
		$pv ++  ;
		$pv_spider ++ if $isSpider ;
		if ($ref eq '-')
		{
		    $source_direct ++ ;
		    $source_direct_spider ++ if $isSpider ;
		}
	    }

	    # ----------------------------------
	    # UV 统计
	    # ----------------------------------
	    $redis -> sadd( 'L99::user::uv_'.$key_day   , $ip ) if $ip;
	    #$redis -> sadd( 'L99::user::uv_'.$key_month , $ip ) if $ip; 	# 不生成月key了，节约redis内存
	    
	    # 单篇的访问ip地域分布
	    if ($url =~ /textId=(\d+)/) {
		my $textId = $1 ;
		$l99_text{$textId} ++ ;
		my $city_geo = $redis_ip->get($ip) ;
		$l99_text_city{$textId}->{$city_geo} ++ if $city_geo;
	    } 
	        
	    my $rediskey_active_hours = 'L99::A::user::active::hour' . $hour . '_' . $key_day ;
	    $active_hours{$rediskey_active_hours}{$ip} = 1;
	    
	    #if ($url =~ /EditAccount_login.action/){ $login ++ }			# 此项暂无意义，注释
	    
	    if ($url =~ /\/user.action\?longNO=(\d+)/){ $l99_zone{$1} ++ }
	    if ($url =~ /\/(\d+)/){ $l99_zone{$1} ++ }
	    
	    # 时间轴翻页加载次数
	    if ($url =~ /dashboardPage.action\?\S+nowPage=(\d+)/){
                my $page_num = $1 ;
                my $rediskey_page = 'L99::A::content::page::page' . $page_num .'_'. $key_day ;
                $timeline_page{$rediskey_page} ++ ;
            }
	    
	    if ($url =~ /timeline.action\?dt=(\d+)/){     
                my $n = $1 ;
                $send  ++ if $n == 1 ;					# 用户点击 ‘我发布的飞鸽’ 记录
                $like  ++ if $n == 2 ;					# 用户点击 ‘我品过的飞鸽’ 记录
                $reply ++ if $n == 6 ;					# 用户点击 ‘我回复的飞鸽’ 记录
            }
	     
            $follow ++ if $url =~ /EditFriend_follow.action/ ;		# 用户加关注 记录
            $friend ++ if $url =~ /EditFriend_friend.action/ ;		# 用户加好友 记录
	    

	    
	    # 统计来自于微信的访客阅读每篇飞鸽的次数
	    if($useragent =~ /MicroMessenger/)
	    {
		my ($textId) = $url =~ /textId=(\d+)/ ;
		my ($app) = $url =~ /lxconnect_download_app.action\?client=key:([a-zA-Z]+)/i ;
		
		$weixin_text{$textId} ++ if $textId;
		$weixin_download{$app} ++ if $app;
	    }
#=cut

=pod 记录 UA	目前先不记录这部分信息了，每天的UA量太大，消耗太大，意义不大.
	    my $time = $key_day.' '.$time_hms ;
	    my $database_mongo = $mongo -> get_database( 'l99' );
	    my $collection = $database_mongo -> get_collection( 'ua' );
            next if $collection->find_one({ ip => $ip , time => $time }) ;
            
	    # 解析后的 Useragent 开始插入 MongoDB 
            my $ua = Woothee -> parse($useragent) ;
            my $name     = $ua -> {name} ;
            my $version  = $ua -> {version};
            my $os       = $ua -> {os};
            my $category = $ua -> {category} ;
            my $vendor   = $ua -> {vendor} ;
            #say "$ip \t $time \t $name \t $version \t $os \t $category \t $vendor" ;
            $collection -> insert( {"ip" => $ip , "time" => $time , "name" => $name , "version" => $version ,
                                    "os" => $os , "category" => $category , "vendor" => $vendor } );
=cut 记录 UA
        }
    }
    
#=pod    
    # PV 浏览量
    insert_redis_scalar( 'L99::A::user::pv_'.$key_day , $pv);
    insert_redis_scalar( 'L99::A::user::pv::spider_'.$key_day , $pv_spider) ;
    
    # 直接访问网站量
    insert_redis_scalar( 'L99::A::user::source::direct_'.$key_day , $source_direct) ;
    insert_redis_scalar( 'L99::A::user::source::direct::spider_'.$key_day , $source_direct_spider) ;
             
    # 活跃用户的 IP 地域分布
    foreach(keys %ips)
    {
	my $ip = $_ ;
	my $city_geo = $redis_ip->get($ip) ;
	my ($country,$province) = $city_geo =~ /^(.*?)_(.*?)_.+$/ ;
	$active_ip{$country.'_'.$province} ++ if $province ;
    }
    my $rediskey_active_ip   = 'L99::A::user::active::ip_'.$key_day;
    my $redisvalue_active_ip = join ';' , map { $_ . ',' . $active_ip{$_} } sort { $active_ip{$b} <=> $active_ip{$a} } keys %active_ip ;
    insert_redis_scalar($rediskey_active_ip => $redisvalue_active_ip) ;
        
    # 排名 TOPN 的单篇 访问IP地域分布
    my $i = 1;
    foreach (sort { $l99_text{$b} <=> $l99_text{$a} } keys %l99_text)
    {
            last if $i > $article_topN ;	
            my $textId = $_ ;
	    my $text_info = decode_textId($textId) ;
	    my $rediskey_l99_article = 'L99::A::content::article::top'.$i.'_'.$key_day ;
	    my $location = join ";" , map {"$_,$l99_text_city{$textId}->{$_}"} keys %{$l99_text_city{$textId}} ;
	    my $redisvalue_l99_article =  $textId . '{' . $l99_text{$textId} .'}' . '{' .$text_info . '}' . $location ;
	    
	    insert_redis_scalar($rediskey_l99_article,$redisvalue_l99_article) ;
            $i ++ ;
    }

#=pod
    # 活跃用户数 (每个小时段 & 日总数)
    my $redis_key_active = 'L99::A::user::active_' . $key_day ;
    foreach(keys %active_hours){
	my $key = $_ ;
	my @ips_hour = keys %{$active_hours{$key}} ;
	$active{$_} = 1 for @ips_hour ;
	my $value = scalar @ips_hour ;
        insert_redis_scalar($key,$value);
    }
    my $active_users_numbers = scalar keys %active ;
    insert_redis_scalar($redis_key_active , $active_users_numbers) ;
    
    # SEND 我发布的飞鸽 点击量
    my $rediskey_send = 'L99::A::content::page::send_' . $key_day ;
    insert_redis_scalar($rediskey_send , $send) if $send;
    
    # LIKE 我品过的飞鸽 点击量
    my $rediskey_like = 'L99::A::content::page::like_' . $key_day ;
    insert_redis_scalar($rediskey_like,$like) if $like;
    
    # REPLY 我回复的飞鸽 点击量
    my $rediskey_reply = 'L99::A::content::page::reply_' . $key_day ;
    insert_redis_scalar($rediskey_reply , $reply) if $reply;
    
    # FOLLOW 用户加关注量
    my $rediskey_follow = 'L99::A::content::page::follow_' . $key_day ;
    insert_redis_scalar($rediskey_follow,$follow) if $follow;
    
    # FRIEND 用户加好友量
    my $rediskey_friend = 'L99::A::content::page::friend_' . $key_day ;
    insert_redis_scalar($rediskey_friend,$friend) if $friend;
    
    # -> Redis L99::timeline_page 时间轴滚动时每页的加载量
    insert_redis(\%timeline_page) ;
    
    # -> Redis L99::article::weixin
    my $rediskey_article_weixin = 'L99::article::weixin_'.$key_day;
    my $redisvalue_article_weixin = join ";" , map {"$_,$weixin_text{$_}"} keys %weixin_text ;
    insert_redis_scalar( $rediskey_article_weixin => $redisvalue_article_weixin );
    
    # -> Redis L99::download::weixin
    my $rediskey_download_weixin = 'L99::download::weixin_' .$key_day;
    my $redisvalue_download_weixin = join ";" , map {"$_,$weixin_download{$_}"} keys %weixin_download ;
    insert_redis_scalar( $rediskey_download_weixin => $redisvalue_download_weixin );

    # -> Redis L99::user::zone
    my $rediskey_l99_zone = 'L99::user::zone_'.$key_day ;
    # 这里，就不记录被访问 10 次以下的用户空间了，浪费内存
    my $redisvalue_l99_zone = join ";" , map {"$_,$l99_zone{$_}"} grep { $l99_zone{$_} > 10 } keys %l99_zone ;
    insert_redis_scalar($rediskey_l99_zone => $redisvalue_l99_zone) ;

    # -> Redis L99::source
    my $source_info = encode_json \%sources;
    insert_redis_scalar('L99::user::source_'.$key_day , $source_info) ;

#=cut
}

# ====================================  function  ======================================

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

sub decode_textId
{
    my ($textId) = @_ ;
    my $html = get("http://www.l99.com/EditText_view.action?textId=$textId") ;
    my ($author) = $html =~ /name="author" content="(.*?)_\S+"/ ;
    my ($title)  = $html =~ /<title>(.*?)\|/ ;
    return encode_utf8 "$title\t$author";
}

sub uri_auth
{
    my ($url) = @_ ;
    my ($scheme, $auth, $path, $query, $frag) = uri_split($url);
    return $auth ;
}

sub isSpider
{
    my ($ua) = @_ ;
    return 1 if $ua =~ /spider|bot/i ;
    my $w = Woothee -> parse($ua);
    return 1 if $w->{category} eq 'crawler' ; 
}

# --------------------------------------------------------------------
# 判断此次请求是否计算在 PV 里面
# --------------------------------------------------------------------
sub isPage
{
    my ($ref) = @_ ;
    my ($fileType) = $ref =~ /\.([^\/]+)$/ ;
    return ( $fileType =~ /css|js|jpg|tif|gif|avi|mov|mpeg|jpeg|iso|rm|mkv|rmvb|png|rar|zip|gz/i ) ? 0 : 1 ;
}

# hello ～