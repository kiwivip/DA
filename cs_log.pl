#!/usr/bin/env perl 
# ==============================================================================
# function:	*
# Author: 	kiwi
# createTime: 2014.6.5
# ==============================================================================
use 5.10.1 ;

BEGIN {
        my @PMs = (
		   #'Config::Tiny' ,
		   #'JSON::XS' ,
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
use MongoDB;
use Config::Tiny ;
use DBI ;
use JSON::XS ;
use LWP::Simple;
use POSIX qw(strftime);
use Time::Local ;
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

my $CS_log_dir   = $Config -> {CS_LOG} -> {dir} ;

my $redis_host = $Config -> {REDIS} -> {host};
my $redis_port = $Config -> {REDIS} -> {port};
my $mongo_host = $Config -> {MONGODB} -> {host};
my $mongo_port = $Config -> {MONGODB} -> {port};

my $time_step = 500 ;
#my $time_step = $Config -> {time} -> {step} ;			# 设置为往前推 N天 统计，默认 N = 1 ;
my $timestamp_now = int scalar time ;
my $timestamp_start = $timestamp_now - 86400 * $time_step  ;			
#my $time = strftime("%Y-%m-%d %H:%M:%S", localtime(time));
my $today = strftime("%Y-%m-%d", localtime(time));
my $day_start = strftime( "%Y-%m-%d 00:00:00" , localtime(time() - 86400 * $time_step) );

# -------------------------------
# connect to redis & mongoDB
# -------------------------------
my $redis     = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
my $redis_db2 = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
$redis_db2 -> select(2) ;
my $mongo = MongoDB::MongoClient->new(host => $mongo_host, port => $mongo_port , query_timeout => 1000000);
my $redis_active = Redis->new(server => "192.168.201.57:6379",reconnect => 10, every => 2000);

my $database_cs = $mongo -> get_database( 'cs' );
my $collection_user = $database_cs -> get_collection( 'user' );
#=pod
# ----------------------------------------------------------------------
# 活跃用户 & 用户文章阅读记录
# ----------------------------------------------------------------------

say "-> Redis.CS::user::active & CS::article " ;
for ( 2 .. $time_step)
{
    my $days = $_ ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days)) ;
    my $log_day = strftime("%Y%m%d", localtime(time()- 86400 * $days)) ;
    my ($key_month) = $key_day =~ /(\d+-\d+)-\d+/ ; 
    say $key_day ;
    #$redis -> exists('CS::A::user::active_' .$key_day ) && next ;

    # 扫描日志中所有的用户 (l99NO)
    my (%active_l99NOs,%dashboard_ids) ;
    my %active_hours ;						# 各时段用户分布
    my @dirs_ip = (14,69) ;					# 199.14 & 199.69
    for(@dirs_ip)
    {
        my $ip = $_ ;
        my $file_cs_log = $CS_log_dir . $ip . '/access.api.nyx.l99.com_' . $log_day . '.log.gz' ;
        open my $fh_cs_log , "gzip -dc $file_cs_log|" ;
        while(<$fh_cs_log>)
        {
        	chomp ;
        	# 192.168.199.57 api.nyx.l99.com 5390910 [27/Jun/2014:23:59:05 +0800] "GET /content/view?dashboard_id=19375158&machine_code=860070021342458&client=key%3ABedForAndroid&version=1.0&format=json HTTP/1.1" 200 1565 "-" "Dalvik/1.6.0 (Linux; U; Android 4.1.2; IdeaTabA1000-T Build/JZO54K)" "2.27" http 171.120.25.135 0.129 0.129 .
        	# 192.168.199.57 api.nyx.l99.com 15427460 [06/Aug/2015:23:59:02 +0800]
            #   "GET /notify/unread/v2?api_version=3&machine_code=867066027166746&client=key%3ABedForAndroid&market=chuangshang_huawei&version=1.0&format=json HTTP/1.0"
            #       200 98 "-" "com.l99.bed/4.5.2(Android OS 4.4.2,HUAWEI MT7-TL10)" "-" https 183.61.68.43 0.007 0.007 .
            my $line = $_ ;
            my $client ;
            my $mcode ;
            if ($line =~ /client=key.*?BedFor(.*?)&/)
            {
                my $s = $1 ;
                $client = 'iPhone'  if $s =~ /iPhone/i ;
                $client = 'Android' if $s =~ /Android/i ;
                if($line =~ /machine_code=(.*?)&/){
                        $mcode = $1 ;
                }
                if ($mcode) {
                        $redis_db2 -> sadd( 'CS::user::active::device::iphone_' .$key_day , $mcode ) if $client eq 'iPhone';
                        $redis_db2 -> sadd( 'CS::user::active::device::android_'.$key_day , $mcode ) if $client eq 'Android';
                }
                
                #say $client."\t".$mcode ;
            }
            
=pod            
        	if($line =~ /nyx.l99.com ([0-9]+) \[\d+\/[a-zA-Z]+\/\d+:(\d+):\d+:\d+ \S+]([\s\S]*?)"com\.l99\.(.*?)\/([0-9\.]+)\([\s\S]*?([\.0-9]+) \S+ \S+ \.$/)
        	{
        		my $l99NO = $1 ;
        		my $hour  = $2 ;
                my $get = $3 ;
                my $api_client = $4 ;
                my $version = $5 ;
        		my $ip = $6 ;
#=pod
                my $market ;
                if ($api_client =~ /bed/)
                {
                        $redis_active -> sadd('STORM::CS::user::version::'.$version.'_'.$key_day , $l99NO );
                }elsif($api_client =~ /chuangshang/)
                {
                        $market = "AppStore" ;
                }
                if ($get =~ /market=chuangshang_(.*?)&/) {
                        $market = $1 ;
                }
                $redis_active -> sadd('STORM::CS::user::active::market::'.$market.'_'.$key_day , $l99NO ) if $market;
#=cut
        		$active_hours{'hour'.$hour}{$l99NO} = 1;
        		$active_l99NOs{$l99NO} = $ip ;
        	}
        	if($line =~ /\/content\/view\S+dashboard_id=(\d+)&/){$dashboard_ids{$1} ++}
=cut
        }
    }
    
=pod 
    # 分时段活跃用户
    foreach (keys %active_hours)
    {
	my $hour = $_ ;
	my $rediskey_active_hours   = 'CS::A::user::active::' . $hour . '_' . $key_day ;
	my $redisvalue_active_hours = scalar keys %{$active_hours{$hour}} ;
	insert_redis_scalar($rediskey_active_hours => $redisvalue_active_hours) if $redisvalue_active_hours;
	
	my $nums_gender_0 = 0 ;
	foreach(keys %{$active_hours{$hour}})
	{
		my $l99NO = $_ ;
		my $data = $collection_user->find_one({'l99NO' => $l99NO});
		my $gender = $data -> {gender} ;
		$nums_gender_0 ++ if $gender eq '0' ;
	}
	my $rediskey_active_hours_gender_0 = 'CS::A::user::active::' . $hour . '::gender::0_' . $key_day ;
	insert_redis_scalar($rediskey_active_hours_gender_0 => $nums_gender_0) if $nums_gender_0;
	my $rediskey_active_hours_gender_1 = 'CS::A::user::active::' . $hour . '::gender::1_' . $key_day ;
	my $nums_gender_1 = $redisvalue_active_hours - $nums_gender_0 ;
	insert_redis_scalar($rediskey_active_hours_gender_1 => $nums_gender_1) if $nums_gender_1;
    }
    
    # 文章访问排行
    my $redis_key_article_day  = 'CS::article_' .$key_day ;
    my $redis_value_article_day = join ";" , map {"$_,$dashboard_ids{$_}"} keys %dashboard_ids ;
    insert_redis_scalar( $redis_key_article_day => $redis_value_article_day );
    
    my $redis_key_active_day   = 'CS::user::active_' .$key_day ;
    my $redis_key_active_month = 'CS::user::active_' .$key_month ;
    
    foreach (keys %active_l99NOs)
    {
        my $l99NO = $_ ;
        my $active ;
        my $data = $collection_user->find_one({'l99NO' => $l99NO});
        my $gender = $data -> {gender} ;
        if($gender eq '0'){
                $active = $l99NO.'_'.$active_l99NOs{$l99NO}.'_gender0' ;
        }else{
                $active = $l99NO.'_'.$active_l99NOs{$l99NO} ;
        }
	
        $redis -> sadd( $redis_key_active_day  , $active ) ;
        $redis -> sadd( $redis_key_active_month, $active ) ;
    }
=cut
}
#=cut

=pod
# ----------------------------------------------------------------------------
# UMENG::A 友盟的行业平均数据
# ----------------------------------------------------------------------------

say "-> Redis.UMENG ... " ;
my %apps_l99 = (
                'cs'     => 'd1ec308dcab042654a3ff525' ,
                'cbs'    => 'f810901cafb042659994d035' ,
                'ft'     => 'a603402f90b04265e2fc1535' ,
                'ft_iOS' => 'c27b70ba10b04265aa631935'
) ;
my %apps_demo = (
                'Android' => 'a20000aac57fc2112a949bd4' ,
                'iOS'     => '4100008dd65107258db11ef4' ,
                'Game'    => '2138b3ad0eb042656892c825'
) ;

# 友盟的示例 APP，其app_id可能变化，所以这里是模拟点击取最新的 demo app，来获取行业平均数据
# 2014.08.07 现在改为把 app_id 写死了，网站改版了...（下面注释掉的逻辑是模拟点击跳转

my %apps ;
%apps = (
		'iOS' => '4100008dd65107258db11ef4' ,
		'Android' => 'a20000aac57fc2112a949bd4' ,
		'Game' => '2138b3ad0eb042656892c825'
) ;

#my $analytics_html = $ua->max_redirects(3)->get('http://www.umeng.com/analytics' => {DNT => 1})->res->body  ;
#my ($href,$iOS_app_id) = $analytics_html =~ /<div class="link clearfix">[\s\S]*?<a href="(\/apps\/([a-zA-Z0-9]+)\/reports)"/ ;
#$apps{iOS} = $iOS_app_id ;
#my $url = 'www.umeng.com' . $href . '/benchmark' ;
#my $apps_html = $ua->max_redirects(3)->get($url => {DNT => 1})->res->body  ;
#
#while ($apps_html =~ /<li app_id="([a-zA-Z0-9]+)">([\s\S]*?)<\/li>/g){
#    my ($app_id,$temp) = ($1,$2) ;
#    $apps{Android} = $app_id if $temp =~ /Android/ ;
#    $apps{Game}    = $app_id if $temp =~ /Game/ ;
#}

foreach(keys %apps)
{
    my $app = $_ ;
    my $app_id = $apps{$app} ;
    my $url_umeng = 'http://www.umeng.com/apps/'.$app_id .'/reports/load_table_data?page=1&per_page=30'.
                    '&versions%5B%5D=&channels%5B%5D=&segments%5B%5D=&time_unit=&stats=benchmark&cat=allCat' ;
    my $res = get($url_umeng) ;
    next unless $res ;
    my $json = decode_json $res;
    foreach my $ref (@{$json->{stats}})
    {
        # name / item / app_stat / same_range_average / same_range_rank / all_range_rank / all_range_average
        my $item = $ref -> {item} ;
        my $average = $ref -> {all_range_average} ;
        my $redis_key_umeng = 'UMENG::A::'.$app.'_'.$today.'_'.$item ;
	
        $redis -> get($redis_key_umeng) == $average && next ;
        insert_redis_scalar( $redis_key_umeng => $average ) if $average;
    }

}
=cut


# ==================================== functions =========================================

sub insert_redis_scalar
{
    my ($rediskey,$redisvalue) = @_ ;
    $redis->set($rediskey,$redisvalue);
    say "$rediskey => $redisvalue" ;
}

=pod
$redis -> incr($key);
$redis -> rpush($key , $value);
