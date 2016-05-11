#!/usr/bin/env perl 
# ==============================================================================
# Author: 	    kiwi
# createTime:	2015.11.3
# ps:           aliyun:  47.88.17.171
#               aws: ssh -i /YourDir/keypair-uswest2-build.pem ubuntu@52.32.158.211
# ==============================================================================
use 5.10.1 ;

BEGIN {
        my @PMs = (
		   #'Config::Tiny' ,
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
use DBI ;
use JSON::XS ;
use POSIX qw(strftime);
use Time::Local ;
use Woothee ; 
use Date::Calc::XS qw (Date_to_Time Time_to_Date);
use Unicode::UTF8 qw (decode_utf8 encode_utf8);
binmode(STDIN, ':encoding(utf8)');
binmode(STDOUT, ':encoding(utf8)');
binmode(STDERR, ':encoding(utf8)');
# --------------------------------------------------------


my $time_step = 1 ;

my $num_month_ago = $time_step / 30 + 1;

my %num2month = (
	'01' => "Jan" , '02' => "Feb" , '03' => "Mar" , '04' => "Apr" ,
	'05' => "May" , '06' => "Jun" , '07' => "Jul" , '08' => "Aug" ,
	'09' => "Sep" , '10' => "Oct" , '11' => "Nov" , '12' => "Dec"
);
my %month2num = reverse %num2month ;

my $redis_ip = 'production-redis.aiuvdm.0001.usw2.cache.amazonaws.com' ;
my $redis = Redis->new(server => "$redis_ip:6379",reconnect => 10, every => 2000);
#$redis -> auth('TestDBSkst$@') ;
$redis -> select(9) ;

# ---------------------------------------------------
# connect to mysql
# ---------------------------------------------------
my $dj_db = 'pintimes' ;
my $dj_host = 'production-mysql.cone5c5tvg75.us-west-2.rds.amazonaws.com' ;
my ($usr,$psw) = ('pt','SkstWebServer') ;
my $dsn = "DBI:mysql:database=$dj_db;host=$dj_host" ;
my $dbh_dj = DBI -> connect($dsn, $usr, $psw, {'RaiseError' => 1} ) ;
$dbh_dj -> do ("SET NAMES UTF8");

=pod
# --------------------------------------
# 新增用户
# --------------------------------------
say "-> Redis.DJ::A::user::new* " ;
for ( 1 .. $time_step + 1 )
{
        my $days = $_ - 1;
        my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days)) ;
    
        my $num_day ;
        my %oses ;
        my $sth_user = $dbh_dj -> prepare("
                                        SELECT user_id,device_id,user_name,user_agent,create_time
                                        FROM
                                        wp_visitor
                                        WHERE
                                        create_time between '$key_day 00:00:00' and '$key_day 23:59:59'
                                        ");
        $sth_user -> execute();
        while (my $ref = $sth_user -> fetchrow_hashref())
        {
        
                my $userId    = $ref -> {user_id} ;
                my $deviceId  = $ref -> {device_id} ;
                my $name      = $ref -> {user_name} ;
                my $useragent = $ref -> {user_agent} ;
                my $time      = $ref -> {create_time} ;
                $num_day ++ ;
                $redis -> sadd( 'DJ::user::new::device_'.$key_day , $deviceId ) ;
                
                my $ua = Woothee -> parse($useragent) ;
                my $os = $ua -> {os};
                $oses{$os} ++ ;
        }
        
        insert_redis_scalar('DJ::A::user::new_'.$key_day , $num_day) if $num_day ;
        my $info = encode_json \%oses;
        insert_redis_scalar('DJ::A::user::new::os_'.$key_day , $info) ;
        
        # -------------------------------------------------------------------------------------------
        my $articles ;
        my $sth_article = $dbh_dj -> prepare("
                                            SELECT ID
                                            FROM
                                            wp_posts
                                            WHERE
                                            post_date between '$key_day 00:00:00' and '$key_day 23:59:59'
                                        ") ;
        $sth_article -> execute();
        while (my $ref = $sth_article -> fetchrow_hashref())
        {
            my $id = $ref -> {ID} ;
            $articles ++ ;
        }
        $sth_article -> finish ;
        insert_redis_scalar('DJ::A::content::article_'.$key_day , $articles) if $articles ;
        
}
=cut

=pod
for ( 1 .. $num_month_ago)	
{
        my $month_ago = $_ - 1 ;
        my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
        my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	
        my $news; 
        foreach( $redis -> keys( 'DJ::A::user::new_'.$month.'-*' ) )
        {
                my $k = $_ ;
                my $n = $redis->get($k) ;
                $news += $n ;
        }
        insert_redis_scalar( 'DJ::A::user::new_'.$month , $news ) ;
        
        # ----------------------------------------------------------
        my $articles; 
        foreach( $redis -> keys( 'DJ::A::content::article_'.$month.'-*' ) )
        {
                my $k = $_ ;
                my $n = $redis->get($k) ;
                $articles += $n ;
        }
        insert_redis_scalar( 'DJ::A::content::article_'.$month , $articles ) ;
}


#=cut

#=pod
# -------------------------------------------------------------------
# 读家活跃用户，文章访问
# -------------------------------------------------------------------

my %hosts = (
    #'198.11.176.245' => 'access.pintimes.pinup.news.log',
    #'198.11.176.253' => 'access.pintimes.pin.news.log',
    #'198.11.177.17' => 'access.pintimes.jiemian.news.log' ,
    #'47.88.24.225' => 'access.pintimes.pin.news.log' ,
    #'47.88.17.171' => 'access.pintimes.888.news.log'
    '52.11.83.66' => 'access.api.pintimes.com' 
) ;

for ( 1 .. $time_step)
{
	my $days = $_ ;
	my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days)) ;
	my $log_day = strftime("%Y%m%d", localtime(time()- 86400 * $days)) ;
    say $key_day ;
    
	for(keys %hosts)
    {
        my $host = $_ ;
        my $logName = $hosts{$host} ;   
        # access.pintimes.pin.news.log-20151030.gz
        my $file_name = $host.'/'.$logName.'.' .$log_day.'.log.gz' ;
        
        next if $redis->get($file_name) == 1 ;  # if the log is scanned 
        
        my $file_log = '/home/RS/LOG/'.$file_name ;
        next unless -e $file_log ;
        open my $fh_log , "gzip -dc $file_log|" ;
        while(<$fh_log>)
        {
                #47.88.24.225 - - [01/Nov/2015:03:42:00 +0800]
                #       "GET /jsp/content/view?content_id=270284 HTTP/1.1" 200 16714
                #       "http://pintimes.lifang.news/jsp/content/view?content_id=270284" "Mozilla/5.0 (X11; U; Linux i686; zh-CN; rv:1.9.1.2) Gecko/20090803 Fedora/3.5.2-2.fc11 Firefox/3.5.2" "-"
                #124.207.104.18 - daXgahIEeorl9TV6Esl/UESkGmp6YnV4lqVtzmN9MGCkqtTO7IOWOYPTbfRNZq7T [30/May/2015:09:15:32 +0800]
                #       "GET /api/content/view?version=1.0&machine_code=F56869A2-661B-4A75-A019-464C337A612E&content_id=145340 HTTP/1.1" 200 785
                #       "-" "com.l99.dujia/1.0 (638, iPhone OS 8.3, iPhone7,2, Scale/2.0)" "-"
                chomp ;
                my $log = $_ ;
                
                my $day_r ;
                if ($log =~ /\[(\d+)\/([a-zA-Z]+)\/(\d+):\d+:/)
                {
                        my ($day,$month_s,$year) = ($1,$2,$3) ;
                        my $month = $month2num{$month_s} ;
                        $day_r = $year.'-'.$month.'-'.$day ;
                }
                
                # 记录用户在首页进行搜索时的关键字及搜索次数等
                if ( $log =~ /\/api\/googlecustomer\/search\?searchStr=(.*?)&.*? HTTP/ ) {
                        my $str = $1 ;
                        $str =~ s/\%([A-Fa-f0-9]{2})/pack('C', hex($1))/seg ;  
                        #say decode_utf8 $str ;
                        $redis -> zincrby( 'DJ::content::google::str_'.$day_r , 1 , $str ) ;
                }
                
               
                # 记录用户的ip和操作系统
                my $os ;
                if ($log =~ /^([0-9\.]+) /)
                {
                        my $ip = $1 ;
                        next if exists $hosts{$ip} ;
                        
                        if ($log =~ /" (\d+) \d+ "/) {
                                my $status = $1 ;
                                next unless $status == 200 ;
                        }
                        
                        
                        if ($log =~ /"([^"]*?)" "[^"]*?"$/)
                        {
                                my $useragent = $1 ;
                                my $ua = Woothee -> parse($useragent) ;
                                $os = $ua -> {os};
                        }
                        $redis -> sadd( 'DJ::user::active::ip_'.$day_r , $ip.'_'.$os ) ;
                }
                
                # 记录活跃设备
                if ($log =~ /machine_code=(.*?)&/) {
                        my $mId = $1 ;
                        $redis -> sadd( 'DJ::user::active::device_'.$day_r , $mId ) ;
                }
                
                # 记录文章阅读数，自增，注意回滚时这个部分要注释掉
                if ($log =~ /content\/view.*?content_id=(\d+) HTTP/)
                {
                        my $contentId = $1 ;
                        $redis -> zincrby( 'DJ::content::view_'.$day_r , 1 , $contentId ) ;
                }
             
        } # end of while(<$fh_log>)
        
        $redis -> set($file_name , 1) ;  # the log is scanned
    } # end of for(keys %hosts)
    
        insert_redis_scalar( 'DJ::A::user::active::ip_'.$key_day     , $redis -> scard('DJ::user::active::ip_'.$key_day) ) ;
        insert_redis_scalar( 'DJ::A::user::active::device_'.$key_day , $redis -> scard('DJ::user::active::device_'.$key_day) ) ;
        insert_redis_scalar( 'DJ::A::content::view_'.$key_day ,        $redis -> zcard('DJ::content::view_'.$key_day) ) ;

}
#=cut


#=pod
for ( 1 .. $num_month_ago )	
{
        my $month_ago = $_ - 1 ;
        my $month = strftime("%Y-%m", localtime(time() - 86400 * 28 * ($month_ago) )) ;
        say $month ;
        my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 28 * ($month_ago - 1) )) ;
	
        # -----------------------------------------------------------
        # content::google
        # -----------------------------------------------------------
        my %google ;
        foreach( $redis -> keys( 'DJ::content::google::str_'.$month.'-*' ) )
        {
                my $k = $_ ;
                next unless $redis->exists($k) ;
                my ($day) = $k =~ /google::str_(\d+-\d+-\d+)$/ ;
                my $times ;
                foreach($redis -> zrange($k, 0, -1))
                {
                        my $str = $_ ;
                        my $n = $redis->zscore($k , $str) ;
                        $times += $n ;
                        $google{$str} += $n ;
                }
                insert_redis_scalar( 'DJ::A::content::google::str_'.$day , $times ) if $times;
        }
        my $times_month ;
        foreach (keys %google)
        {
                my $s = $_ ;
                my $n = $google{$s} ;
                next unless $n ;
                $times_month += $n ;
                $redis -> zadd( 'DJ::content::google::str_'.$month , $n , $s ) ;
        }
        insert_redis_scalar( 'DJ::A::content::google::str_'.$month , $times_month ) if $times_month;
   
        # -----------------------------------------------------------
        # content::view
        # -----------------------------------------------------------
        my %views ;
        foreach( $redis -> keys( 'DJ::content::view_'.$month.'-*' ) )
        {
                my $k = $_ ;
                foreach($redis -> zrange($k, 0, -1))
                {
                        my $contentId = $_ ;
                        my $n = $redis->zscore($k , $contentId) ;
                        $views{$contentId} += $n ;
                }
        }
        foreach (keys %views)
        {
                my $contentId = $_ ;
                my $n = $views{$contentId} ;
                $redis -> zadd( 'DJ::content::view_'.$month , $n , $contentId ) ;
        }
        
        # ----------------------------------------------------------------
        # active::device
        # ----------------------------------------------------------------
        my %devices ;
        foreach( $redis -> keys( 'DJ::user::active::device_'.$month.'-*' ) )
        {
                my $k = $_ ;
                my @ds = $redis->smembers($k);
                $devices{$_} = 1 for @ds ;
        }
        insert_redis_scalar( 'DJ::A::user::active::device_'.$month , scalar keys %devices ) ;
        
        # -----------------------------------------------------------------
        # active::[ip/os]
        # -----------------------------------------------------------------
        my %ips ;
        my %oses ;
        foreach( $redis -> keys( 'DJ::user::active::ip_'.$month.'-*' ) )
        {
                my $k = $_ ;
                my @ips = $redis->smembers($k);
                for(@ips){
                        my ($ip,$os) = split '_' , $_ ;
                        $ips{$ip} = 1 ;
                        $oses{$os} ++ ;
                }
        }
        insert_redis_scalar( 'DJ::A::user::active::ip_'.$month , scalar keys %ips ) ;
        my $info = encode_json \%oses;
        insert_redis_scalar('DJ::A::user::active::os_'.$month , $info) ;
}
=cut


# --------------------------
# 设备留存率
# --------------------------
say "-> Redis.DJ::A::user::liucun* " ;
for ( 1 .. $time_step+30 )
{
        my $days = $_ - 1;
        my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days)) ;
        
        my $day_t1 = strftime("%Y-%m-%d", localtime(time() - 86400 * ($days-1))) ;
}

# =========================================  functions  ==========================================

sub insert_redis_scalar
{
    my ($rediskey,$redisvalue) = @_ ;
    $redis->set($rediskey,$redisvalue);
    say "$rediskey \t=>\t $redisvalue" ;
}

sub insert_redis_hash
{
    my ($ref) = @_ ;
    foreach (keys %$ref)
    {
        my $key = $_ ;
        my $value = $ref->{$key} ;
        $redis->set($key,$value);
        say "$key \t=>\t $value" ;
    }
}

