#!/usr/bin/env perl 
# ==============================================================================
# Author: 	    kiwi
# createTime:	2015.11.5
# ps: run at 47.88.17.171
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
use DBI ;
use Redis ;
use Statistics::R;
use MaxMind::DB::Reader ;
use JSON::XS ;
use POSIX qw(strftime);
use Unicode::UTF8 qw (decode_utf8 encode_utf8);
binmode(STDIN, ':encoding(utf8)');
binmode(STDOUT, ':encoding(utf8)');
binmode(STDERR, ':encoding(utf8)');


my $maxmind_reader = MaxMind::DB::Reader->new( file => '/home/RS/GeoLite2-City.mmdb' );

my $redis_ip = 'production-redis.aiuvdm.0001.usw2.cache.amazonaws.com' ;
my $redis = Redis->new(server => "$redis_ip:6379",reconnect => 10, every => 2000);
#$redis -> auth('TestDBSkst$@') ;
$redis -> select(9) ;         # 统计指标存储于db9

my $time_step = 1 ;
my $num_month_ago = $time_step / 30 + 1;


# ---------------------------------------------------
# connect to mysql
# ---------------------------------------------------
# 'pintimes','dujiamysql.mysql.rds.aliyuncs.com'     
my ($dj_db,$dj_host) = ('pintimes','production-mysql.cone5c5tvg75.us-west-2.rds.amazonaws.com') ;
my ($usr,$psw) = ('pt','SkstWebServer') ;
my $dsn = "DBI:mysql:database=$dj_db;host=$dj_host" ;
my $dbh_dj = DBI -> connect($dsn, $usr, $psw, {'RaiseError' => 1} ) ;
$dbh_dj -> do ("SET NAMES UTF8");

#=pod
# --------------------------------------
# KPI月份的新增用户
# --------------------------------------
for ( 1 .. $num_month_ago)	
{
        my $month_ago = $_ - 1 ;
        my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
        my $month_last = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago + 1) )) ;
        
        my $kpi_time_start = $month_last.'-25' ;
        my $kpi_time_end   = $month.'-24' ;
        
        my $num_new ;
        my $sth_user = $dbh_dj -> prepare("
                                SELECT user_id,device_id,user_name,user_agent,create_time
                                FROM
                                wp_visitor
                                WHERE
                                create_time between '$kpi_time_start 00:00:00' and '$kpi_time_end 23:59:59'
                                ");
        $sth_user -> execute();
        while (my $ref = $sth_user -> fetchrow_hashref())
        {  
                my $userId    = $ref -> {user_id} ;
                $num_new ++ ;
        }
        
        say "$kpi_time_start => $kpi_time_end user_new : $num_new" ;

        # new user's os 
        # --------------------------------------------------------------------------
        my %oses ;
        foreach( $redis -> keys( 'DJ::A::user::new::os_'.$month_last.'-*' ) )
        {
                my $key = $_ ;
                my ($day) = $key =~ /_\d+-\d+-(\d+)$/ ;
                next if $day < 25 ;
                my $os_info = $redis->get($key) ;
                
                say "$key \t=>\t $os_info" ;
                
                my $temp = decode_json $os_info ;
                foreach (keys %$temp){
                    my $os = $_ ;
                    my $n = $$temp{$os} ;
                    $oses{$os} += $n ;
                }
        }
        foreach( $redis -> keys( 'DJ::A::user::new::os_'.$month.'-*' ) )
        {
                my $key = $_ ;
                my ($day) = $key =~ /_\d+-\d+-(\d+)$/ ;
                next if $day > 24 ;
                my $os_info = $redis->get($key) ;
                
                say "$key \t=>\t $os_info" ;
                
                my $temp = decode_json $os_info ;
                foreach (keys %$temp){
                    my $os = $_ ;
                    my $n = $$temp{$os} ;
                    $oses{$os} += $n ;
                }
        }
        my $temp = encode_json \%oses;
        say "$kpi_time_start => $kpi_time_end user_new_os : $temp" ;
}
#=cut


#=pod
# --------------------------------------
# KPI月份的活跃设备／ip／系统
# --------------------------------------
# user::active::[device/ip/os]
for ( 1 .. $num_month_ago)	
{
        my $month_ago = $_ - 1 ;
        my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
        my $month_last = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago + 1) )) ;
        
        my %ips ;
        my %ips_weixin ;
        my %devices ;
        my %oses ;
        
        # 25-24 devices 活跃设备
        foreach( $redis -> keys( 'DJ::user::active::device_'.$month_last.'-*' ) )
        {
            my $key = $_ ;
            my ($day) = $key =~ /_\d+-\d+-(\d+)$/ ;
            next if $day < 25 ;
            my @temp = $redis->smembers($key);
            my $n = scalar @temp ;
            say "$key \t=>\t $n" ;
            
            $devices{$_} = 1 for @temp ;
        }
        foreach( $redis -> keys( 'DJ::user::active::device_'.$month.'-*' ) )
        {
            my $key = $_ ;
            my ($day) = $key =~ /_\d+-\d+-(\d+)$/ ;
            next if $day > 24 ;
            my @temp = $redis->smembers($key);
            my $n = scalar @temp ;
            say "$key \t=>\t $n" ;
            $devices{$_} = 1 for @temp ;
        }
        
        # 25-24 活跃的微信ip
        foreach( $redis -> keys( 'DJ::user::active::ip::weixin_'.$month_last.'-*' ) )
        {
            my $key = $_ ;
            my ($day) = $key =~ /_\d+-\d+-(\d+)$/ ;
            next if $day < 25 ;
            my @temp = $redis->smembers($key);
            my $n = scalar @temp ;
            say "$key \t=>\t $n" ;
            $ips_weixin{$_} = 1 for @temp ;
        }
        foreach( $redis -> keys( 'DJ::user::active::ip::weixin_'.$month.'-*' ) )
        {
            my $key = $_ ;
            my ($day) = $key =~ /_\d+-\d+-(\d+)$/ ;
            next if $day > 24 ;
            my @temp = $redis->smembers($key);
            my $n = scalar @temp ;
            say "$key \t=>\t $n" ;
            $ips_weixin{$_} = 1 for @temp ;
        }
        
        # 25-24 ip 活跃ip
        foreach( $redis -> keys( 'DJ::user::active::ip_'.$month_last.'-*' ) )
        {
            my $key = $_ ;
            my ($day) = $key =~ /_\d+-\d+-(\d+)$/ ;
            next if $day < 25 ;
            my @ips = $redis->smembers($key);
            my $n = scalar @ips ;
            say "$key \t=>\t $n" ;
            for(@ips){
                    my ($ip,$os) = split '_' , $_ ;
                    $ips{$ip} = 1 ;
                    $oses{$os} ++ ;
            }
        }
        foreach( $redis -> keys( 'DJ::user::active::ip_'.$month.'-*' ) )
        {
            my $key = $_ ;
            my ($day) = $key =~ /_\d+-\d+-(\d+)$/ ;
            next if $day > 24 ;
            my @ips = $redis->smembers($key);
            my $n = scalar @ips ;
            say "$key \t=>\t $n" ;
            for(@ips){
                    my ($ip,$os) = split '_' , $_ ;
                    $ips{$ip} = 1 ;
                    $oses{$os} ++ ;
            }
        }
        
        
        #my %countrys ;
        #for(keys %ips)
        #{
        #        my $ip = $_ ;
        #        my $record = $maxmind_reader -> record_for_address($ip);
        #        my $ref_geo = geoIP($record) ;
        #        my $country = $ref_geo -> {country} ;
        #        my $subdivisions = $ref_geo -> {subdivisions}  ;
        #        my $city = $ref_geo -> {city}  ;
        #        $countrys{$country} ++ ;
        #}
        #my $temp_country = encode_json \%countrys;
        #say $month_last.'-25 => ' .$month.'-24 ip_country : ' . $temp_country ;
        
        say $month_last.'-25 => ' .$month.'-24 active devices : ' . scalar keys %devices ;
        say $month_last.'-25 => ' .$month.'-24 active ips : ' . scalar keys %ips ;
        say $month_last.'-25 => ' .$month.'-24 from_weixin ips : ' . scalar keys %ips_weixin ;
        my $temp = encode_json \%oses;
        say $month_last.'-25 => ' .$month.'-24 active oses : ' . $temp ;
}
#=cut

# ----------------------------------
# 用户搜索关键词的月度统计
# ----------------------------------
for ( 1 .. $num_month_ago)	
{
        my $month_ago = $_ - 1 ;
        my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
        my $month_last = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago + 1) )) ;
        
        my $kpi_time_start = $month_last.'-25' ;
        my $kpi_time_end   = $month.'-24' ;
        my %google ;
        my $times ;
        foreach( $redis -> keys( 'DJ::content::google::str_'.$month_last.'-*' ) )
        {
                my $key = $_ ;
                my ($day) = $key =~ /_\d+-\d+-(\d+)$/ ;
                next if $day < 25 ;
                my $times_day ;
                foreach($redis -> zrange($key, 0, -1))
                {
                        my $str = $_ ;
                        my $n = $redis->zscore($key , $str) ;
                        $times_day += $n ;
                        $times += $n ;
                        $google{decode_utf8 $str} += $n ;
                }
                say $month_last.'-'.$day.' google times : '.$times_day ;
        }
        foreach( $redis -> keys( 'DJ::content::google::str_'.$month.'-*' ) )
        {
                my $key = $_ ;
                my ($day) = $key =~ /_\d+-\d+-(\d+)$/ ;
                next if $day > 24 ;
                my $times_day ;
                foreach($redis -> zrange($key, 0, -1))
                {
                        my $str = $_ ;
                        my $n = $redis->zscore($key , $str) ;
                        $times_day += $n ;
                        $times += $n ;
                        $google{decode_utf8 $str} += $n ;
                }
                say $month.'-'.$day.' google times : '.$times_day ;
        }
        
        # KPI月份的总google搜索次数及各关键词搜索次数
        my $info = decode_utf8 encode_json \%google ;
        say $month_last.'-25 => ' .$month.'-24 google times : '. $times ;
        say $month_last.'-25 => ' .$month.'-24 google strings : ' .$info ;
        
        # 搜索关键词按次数排序固化，方便后续画词云图
        open my $fh_g , ">:utf8" , '/home/RS/googlestr_'.$month.'.txt' ;
        foreach(sort {$google{$b} <=> $google{$a}} keys %google)
        {
                my $word = $_ ;
                my $v = $google{$word} ;
                say $fh_g "$word\t$v" ;
        }
        
}

#=pod
# ------------------------------------------------
# content::[article/view/keywords]
# 文章更新数量，浏览量及对所有文章内容取关键词
# ------------------------------------------------
for ( 1 .. $num_month_ago)	
{
        my $month_ago = $_ - 1 ;
        my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
        my $month_last = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago + 1) )) ;
        
        my $kpi_time_start = $month_last.'-25' ;
        my $kpi_time_end   = $month.'-24' ;
        
        foreach( $redis -> keys( 'DJ::A::content::article_'.$month_last.'-*' ) )
        {
            my $key = $_ ;
            my ($day) = $key =~ /_\d+-\d+-(\d+)$/ ;
            next if $day < 25 ;
            my $n = $redis->get($key) ;
            say "$key \t=>\t $n" ;
        }
        foreach( $redis -> keys( 'DJ::A::content::article_'.$month.'-*' ) )
        {
            my $key = $_ ;
            my ($day) = $key =~ /_\d+-\d+-(\d+)$/ ;
            next if $day > 24 ;
            my $n = $redis->get($key) ;
            say "$key \t=>\t $n" ;
        }
        
        # how many articles 
        # ----------------------------------------------------------------------
        my $articles ;
        my %authors ;
        my %articleIds ;
        my $sth_user = $dbh_dj -> prepare("
                                        SELECT p.ID,p.post_author,u.display_name
                                        FROM
                                        wp_posts p left join wp_users u on p.post_author = u.ID 
                                        WHERE
                                        p.post_date between '$kpi_time_start 00:00:00' and '$kpi_time_end 23:59:59'
                                        ") ;
        $sth_user -> execute();
        while (my $ref = $sth_user -> fetchrow_hashref())
        {
            my $id     = $ref -> {ID} ;
            my $author = decode_utf8 $ref -> {display_name} ;
            $articleIds{$id} = 1 ;
            $authors{$author} ++ ;
            $articles ++ ;
        }
        $sth_user -> finish ;
        
        say "$kpi_time_start => $kpi_time_end articles : $articles" ;
        my $temp_author = decode_utf8 encode_json \%authors;
        say "$kpi_time_start => $kpi_time_end authors : $temp_author" ;
        
      
        # article-view top10
        # ------------------------------------------------------------------------
        my %views ;
        my $times ;
        foreach( $redis -> keys( 'DJ::content::view_'.$month_last.'-*' ) )
        {
            my $key = $_ ;
            my ($day) = $key =~ /_\d+-\d+-(\d+)$/ ;
            next if $day < 25 ;
            foreach($redis -> zrange($key, 0, -1))
            {
                        my $contentId = $_ ;
                        my $n = $redis->zscore($key , $contentId) ;
                        $times += $n ;
                        $views{$contentId} += $n ;
            }
        }
        foreach( $redis -> keys( 'DJ::content::view_'.$month.'-*' ) )
        {
            my $key = $_ ;
            my ($day) = $key =~ /_\d+-\d+-(\d+)$/ ;
            next if $day > 24 ;
            foreach($redis -> zrange($key, 0, -1))
            {
                        my $contentId = $_ ;
                        my $n = $redis->zscore($key , $contentId) ;
                        $times += $n ;
                        $views{$contentId} += $n ;
            }
        }
        my $i = 1;
        my $top ;
        foreach (sort { $views{$b} <=> $views{$a} } keys %views)
        {
            last if $i > 10 ;	
            my $textId = $_ ;
            my $times = $views{$textId} ;
            $top .= $textId.'-'.$times.',' ;
            $i ++ ;
        }
        say "$kpi_time_start => $kpi_time_end \n \t\t times : $times \n \t\t articles_view_top10 : $top" ;
        
}
#=cut

# ==================================== functions =====================================

sub geoIP
{
    my ($record) = @_ ;
    my $country = $record->{country}->{names}->{en};
    my ($subdivisions,$city) ;
    # 这个地方处理了一下，ip解析的时候中国地区的用中文，外国的用英文
    if ($country eq 'China')
    {
        $country = '中国' ;
        $subdivisions = $record->{subdivisions}->[0]->{names}->{'zh-CN'};
        $city         = $record->{city}->{names}->{'zh-CN'};
    }else{
        $subdivisions = $record->{subdivisions}->[0]->{names}->{en};
        $city = $record->{city}->{names}->{en};
    }
    my $ref_geo ;
    $ref_geo -> {country} = $country ;
    $ref_geo -> {subdivisions} = $subdivisions ;
    $ref_geo -> {city} = $city ;
    return $ref_geo  ;
    
}



