#!/usr/bin/env perl 
# ==============================================================================
# function:	    数据分析 for 立方网网站平台
# Author: 	    kiwi
# createTime:	2014.6.23
# ==============================================================================

use 5.10.1 ; 
BEGIN {
    # 如果程序迁移到新环境，需要Linux预配置好 cpanm ，然后解掉注释，自动安装依赖包；
    # 若系统perl<5.10，请手动进行perl升级与依赖包安装
    my @PMs = (
            #'JSON::XS', 
            #'Config::Tiny',
            #'Unicode::UTF8',
            #'HTML::ExtractMain',
            #'Statistics::R'
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
use MongoDB ;
use Time::Local ;
use JSON::XS ;
use LWP::Simple;
use Config::Tiny ;
use Statistics::R;
use HTML::ExtractMain qw( extract_main_html );
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
$Config = Config::Tiny->read( '/home/DA/DataAnalysis/config.ini', 'utf8' );
my $version = $Config -> {_} -> {version};

my $L99_log_dir         = $Config -> {L99_LOG} -> {dir} ;			# 日志所在文件夹路径
my $L99_os_json         = $Config -> {L99_LOG} -> {file_os} ;
my $L99_browser_json    = $Config -> {L99_LOG} -> {file_browser} ;
my $source_topN         = $Config -> {L99_LOG} -> {source_topN} ;		# 立方网流量来源网站排行
my $weixin_article_topN = $Config -> {L99_LOG} -> {weixin_topN} ;		# 微信阅读文章 排行
my $l99_zone_topN       = $Config -> {L99_LOG} -> {l99_zone_topN} ;		# 立方网用户个人空间访问 排行
my $l99_keywords_topN   = $Config -> {L99_LOG} -> {l99_keywords_topN} ;		# 立方网最火的文章内容的关键字标签个数

my $L06_host     = $Config -> {L06_DB} -> {host};
my $L06_db       = $Config -> {L06_DB} -> {database} ;
my $L06_usr      = $Config -> {L06_DB} -> {username};
my $L06_password = $Config -> {L06_DB} -> {password};

my $redis_host = $Config -> {REDIS} -> {host};
my $redis_port = $Config -> {REDIS} -> {port};

#my $time_step = 3 ;
my $time_step = $Config -> {time} -> {step} ;					# 设置为往前推 N天 统计；一般地，N = 1 

my $time_now = strftime("%Y-%m-%d %H:%M:%S", localtime(time));
my $day_yest = strftime("%Y-%m-%d", localtime(time() - 86400));			# 昨天
my ($month_yest) = $day_yest =~ /^(\d+-\d+)-\d+/ ;				# 昨天所在的月份
my $months_ago = $time_step / 30 + 1;

# -------------------------------------------------------------------
# connect to mysql
# -------------------------------------------------------------------
my $dsn_v506 = "DBI:mysql:database=$L06_db;host=$L06_host" ;
my $dbh_v506 = DBI -> connect($dsn_v506, $L06_usr, $L06_password, {'RaiseError' => 1} ) ;
$dbh_v506 -> do ("SET NAMES UTF8");

# -------------------------------------------------------------------
# connect to Redis & mongoDB
# -------------------------------------------------------------------
my $redis = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);

#my $mongo  = MongoDB::MongoClient->new(host => $mongo_host, port => $mongo_port ,query_timeout => 100000);	# 100s timeout
#my $mongo = MongoDB::MongoClient->new(host => '192.168.201.59', port => 27017);
#my $db_l99 = $mongo -> get_database( 'l99' ) ;
#my $collection_ua = $db_l99 -> get_collection( 'ua' );

# -------------------------------------------------------------------------------

#=pod
# -------------------------------------------------
# 月 PV/UV
# -------------------------------------------------
say '-> Redis.L99::A::user::pv_MONTH ' ;
my %user_puv ;
foreach($redis->keys( 'L99::A::user::pv*' ) )
{
    my $key = $_ ;
    my $value = $redis->get($key);
    
    if($key =~ /user::(pv.*?)_(\d+-\d+)-\d+$/ )
    {
	my ($type,$month) = ($1,$2) ;
	my $redis_key_user_month = 'L99::A::user::'.$type . '_' .$month ;
	$user_puv{$redis_key_user_month} += $value ;
    }
}
insert_redis(\%user_puv) ;


say '-> Redis.L99::A::user::uv ' ;
for ( 1 .. $time_step)
{
    my $days_step = $_ ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    
    next unless $redis -> exists('L99::user::uv_' .$key_day ) ;
    
    my $num_day = $redis -> scard( 'L99::user::uv_'.$key_day );
    insert_redis_scalar('L99::A::user::uv_'.$key_day , $num_day) ;
    
}

my %ips_month ;
foreach($redis->keys( 'L99::user::uv_' . $month_yest . '*') )
{
    my $key = $_ ;
    my @ips = $redis->smembers( $key ) ;
    foreach( @ips ){
	$ips_month{$_} = 1 ;
    }
}
my $num_month = scalar keys %ips_month ;
insert_redis_scalar('L99::A::user::uv_' . $month_yest , $num_month) ;



# -----------------------------------------------------------------
# 活跃用户的IP分布
# -----------------------------------------------------------------
say "-> Redis.L99::A::user::active::ip_MONTH" ;
my %user_active_ip_temp ;
foreach($redis->keys( 'L99::A::user::active::ip_*' ) )
{
    my $key = $_ ;
    my $num = $redis -> get($key) ;
    if($key =~ /L99::A::user::active::ip_(\d+-\d+)-\d+$/)
    {
	my $month = $1;
	my @prov = split ';' , $num ;
	foreach (@prov){
	    my ($k,$v) = split ',' , $_ ;
	    $user_active_ip_temp{$month}{$k} += $v ;
	}
	
    }
}
foreach(keys %user_active_ip_temp){
    my $month = $_ ;
    my $rediskey_active_ip   = 'L99::A::user::active::ip_' . $month ;
    my $redisvalue_active_ip = join ';' , map { $_ . ',' . $user_active_ip_temp{$month}{$_} }
					sort { $user_active_ip_temp{$month}{$b} <=> $user_active_ip_temp{$month}{$a} }
					keys %{$user_active_ip_temp{$month}} ;
    insert_redis_scalar($rediskey_active_ip => $redisvalue_active_ip) ;
}
#=cut

#=pod
# -------------------------------------------------------------------------------
#  月PV/UV基准线，以上个月的日 MAX 定义
# -------------------------------------------------------------------------------
say '-> Redis.L99::A::user::[pu]v::baseline_MONTH ' ;

for ( 1 .. $months_ago)				# 往前取几个月
{
    my $month_ago = $_ - 1 ;
    my $month_last = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago + 1) )) ;
    my $month_now  = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
    my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
    
    my (@pvs_last_month,@uvs_last_month) ;
    
    foreach($redis->keys( 'L99::A::user::pv_' . $month_last . '-*' ) ){					
        push @pvs_last_month , $redis->get($_) ;
    }
    my $pv_month_baseline = max(@pvs_last_month) ;
    insert_redis_scalar('L99::A::user::pv::baseline_' . $month_now , $pv_month_baseline) ;


    foreach($redis->keys( 'L99::A::user::uv_' . $month_last . '-*' ) ){					
        push @uvs_last_month , $redis->get($_) ;
    }
    my $uv_month_baseline = max(@uvs_last_month) ;
    insert_redis_scalar('L99::A::user::uv::baseline_' . $month_now , $uv_month_baseline) ;

}

#=pod
# ---------------------------------------------------------------------
#  用户个人空间的访问量排行
# ---------------------------------------------------------------------

say "-> Redis.L99::A::user::zone::topN_TIME " ;
for ( 1 .. $time_step)
{
    my $days_step = $_ ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    $redis -> exists('L99::A::user::zone::top1_' . $key_day) && next ; 
    
    my (%zone) ;
    my $zones = $redis->get('L99::user::zone_' . $key_day) ;
    foreach(split(';',$zones)){
	my ($l99NO,$num) = split ',' , $_ ;
	$zone{$l99NO} = $num if $l99NO;
    }
    
    my $i = 1;
    foreach (sort { $zone{$b} <=> $zone{$a} } keys %zone)
    {
	last if $i > $l99_zone_topN ;				
	my $l99NO = $_ ;
	my $num = $zone{$l99NO} ;
	my $user_info = decode_l99NO($l99NO) ;
	
        my $rediskey_l99_zone_top = 'L99::A::user::zone::top'.$i.'_'.$key_day ;
	my $value = "$l99NO,$num,'$user_info'" ;
	insert_redis_scalar($rediskey_l99_zone_top,$value) ;
        $i ++ ;
    }
}
my %zone_month ;
my $zone_month_time = strftime("%Y-%m",localtime(time()-86400));
foreach($redis->keys( 'L99::user::zone_'.$zone_month_time.'*' ) ){
	my $key = $_ ;						
	my $zones = $redis->get($key) ;
	my ($month) = $key =~ /zone_(\d+-\d+)-\d+$/ ;
	
	foreach(split(';',$zones)){
	    my ($user,$num) = split ',' , $_ ;
	    $zone_month{$month} -> {$user} += $num if $user;
	}
}
foreach(keys %zone_month){
    my $month = $_ ;
    my $ref = $zone_month{$month} ;
    
    my $i = 1;
    foreach (sort { $ref -> {$b} <=> $ref -> {$a} } keys %$ref)
    {
	last if $i > $l99_zone_topN ;
        my $user = $_ ;
	my $num = $ref -> {$user} ;
	my $user_info = decode_l99NO($user) ;
	
        my $rediskey_zone_month_top = 'L99::A::user::zone::top'.$i.'_'.$month ;
	my $value = "$user,$num,'$user_info'" ;
	insert_redis_scalar($rediskey_zone_month_top,$value) ;
        $i ++ ;
    }
}
#=cut
#=pod
# ---------------------------------------------------------------------------------
#  使用微信阅读的飞鸽排行
# ---------------------------------------------------------------------------------
say "-> Redis.L99::A::content::article::weixin::top " ;
for ( 1 .. $time_step)
{
    my $days_step = $_ ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;

    $redis -> exists('L99::A::content::article::weixin::top1'.$key_day ) && next ; 
    my (%weixin) ;
    my $articles = $redis->get('L99::article::weixin_'.$key_day) ;
    foreach(split(';',$articles)){
	my ($article,$num) = split ',' , $_ ;
	$weixin{$article} = $num ;
    }
    
    my $i = 1;
    foreach (sort { $weixin{$b} <=> $weixin{$a} } keys %weixin)
    {
	last if $i > $weixin_article_topN ;
	my $article = $_ ;
	my $num = $weixin{$article} ;
	my $article_info = decode_textId($article) ;
	
        my $rediskey_article_top = 'L99::A::content::article::weixin::top'.$i . '_' . $key_day ;
	my $value = "$article,$num,'$article_info'" ;
	insert_redis_scalar($rediskey_article_top,$value) ;
        $i ++ ;
    }
}
my %weixin_month ;
my $weixin_month_time = strftime("%Y-%m",localtime(time()-86400));
foreach($redis->keys( 'L99::article::weixin_'.$weixin_month_time.'*' ) )
{
	my $key = $_ ;									# L99::article::weixin_2014-06-30
	my ($weixin_month_time) = $key =~ /weixin_(\d+-\d+)-\d+$/ ;
	my $articles = $redis->get($key) ;
	foreach(split(';',$articles)){
	    my ($article,$num) = split ',' , $_ ;
	    $weixin_month{$weixin_month_time} -> {$article} += $num ;
	}
}
foreach(keys %weixin_month){
    my $month = $_ ;
    my $ref = $weixin_month{$month} ;
    
    my $i = 1;
    foreach (sort { $ref -> {$b} <=> $ref -> {$a} } keys %$ref)
    {
	last if $i > $weixin_article_topN ;	
        my $article = $_ ;
	my $num = $ref -> {$article} ;
	my $article_info = decode_textId($article) ;
	
        my $rediskey_article_month_top = 'L99::A::content::article::weixin::top'.$i . '_' . $month ;
	my $value = "$article,$num,'$article_info'" ; ;
	insert_redis_scalar($rediskey_article_month_top,$value) ;            
        $i ++ ;
    }
}

# --------------------------------------------------------------------
# L99::A::content::download::weixin::{}_TIME 
# --------------------------------------------------------------------
for ( 1 .. $time_step)
{
    my $days_step = $_ ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;

    my $downloads = $redis->get('L99::download::weixin_'.$key_day) ;
    foreach(split(';',$downloads)){
	my ($app,$num) = split ',' , $_ ;
	my $rediskey_download = 'L99::A::content::download::weixin::{'.$app.'}_'.$key_day ;
	insert_redis_scalar($rediskey_download,$num) ;
    }
}

#=cut


# ---------------------------------------------------------------------
# 用户来源 L99::A::user::source
# ---------------------------------------------------------------------
#=pod

say '-> Redis.L99::A::user::source::direct(::spider)?_MONTH' ;
my %user_source_direct ;
foreach($redis->keys( 'L99::A::user::source::direct*' ) )
{
    my $key = $_ ;
    my $value = $redis->get($key);
    if($key =~ /source::(dir.*?)_(\d+-\d+)-\d+$/ )
    {
	my ($type,$month) = ($1,$2) ;
	$user_source_direct{ 'L99::A::user::source::'.$type.'_'.$month } += $value ;
    }
}
insert_redis(\%user_source_direct) ;


#=cut

#=pod
# ------------------------------------------------------------------------------
# L99::A::content::page::send(like reply follow friend pageN) 月点击量
# ------------------------------------------------------------------------------
say "-> Redis.L99::A::content::page::*_MONTH" ;
my %content_page ;
foreach($redis->keys( 'L99::A::content::page::*' ) )
{
    my $key = $_ ;
    my $value = $redis->get($key);
    
    if($key =~ /L99::A::content::page::(.*?)_(\d+-\d+)-\d+/ ){
	my ($type,$month) = ($1,$2) ;
	my $redis_key_user_month = 'L99::A::content::page::' .$type . '_' . $month;
	$content_page{$redis_key_user_month} += $value ;
    }
}
insert_redis(\%content_page) ;

#=cut

#=pod
# ---------------------------------------------------------------
# 立方网当天最火Top文章的关键词标签云
# ---------------------------------------------------------------

say "-> Redis.L99::A::content::article::keywords_TIME " ;

for ( 1 .. $time_step)
{
    my $days_step = $_ ;
    my $key_day = strftime("%Y-%m-%d", localtime(time() - 86400 * $days_step)) ;
    say $key_day ;
    my $DIR = '/home/DA/DataAnalysis/' ;
    my $num_top = 80;
    
    my $txt = $DIR . 'word.txt' ;
    my $txt_word = $DIR . 'TXT/words_' . $key_day . '.txt' ;
    
    #my $photo = $DIR.'JPG/keywords_'.$key_day.'.jpg' ;
    my $photo = '/home/Nodejs/Nodejs/GuoJ/guojia_online/tongji/static/images/keywords_'.$key_day.'.jpg' ;
    next if -e $photo ;
    
    open my $fh_keywords , ">>:utf8" , $txt ;
    for(1 .. 20)
    {
        my $article = $redis->get('L99::A::content::article::top' . $_.'_'. $key_day ) ;
        my ($textId) = $article =~ /^(\d+)\{/ ;
        my $body = text_body($textId) ;
        print $fh_keywords $body;
    }
    
    my (@nums,@names) ;
    my $R = Statistics::R->new(bin => '/usr/local/bin/R');		# 这里需要显式地配置R路径，否则crontab无效
    my $output = $R -> run
    (
        q`library(jiebaR)` ,
        "keys = worker('keywords', topn = $num_top ,encoding = 'UTF-8')" ,
        "keys <= '$txt'"
    );
    #这里因为是获取R编译器的STDOUT，所以字符串处理一下
    while($output =~ /([0-9\.]+)[\s\n]/g) {
        push @nums , int($1) ;
    }
    my $num_color = max(@nums) ;
    while($output =~ /"(.*?)"[\s\n]/g) {
        push @names , decode_utf8 $1 ;
    }
    $R -> stop();
    unlink($txt) ;

    my %keywords ;
    for( 1 .. $num_top)
    {
        my $sub = $_ - 1 ;
        $keywords{$names[$sub]} = $nums[$sub] if $names[$sub];
    }
    
    open my $fh_c , ">>:utf8" , $txt_word ;
    foreach( sort {$keywords{$b} <=> $keywords{$a}} keys %keywords)
    {
        my $keyword = $_ ;
        my $num = $keywords{$keyword} ;
        $keyword =~ s/(?<=\\)(\d{3})/sprintf("%x",oct($1))/eg;
        $keyword =~ s/\\/%/g ;
        $keyword =~ s/\%([A-Fa-f0-9]{2})/pack('C', hex($1))/seg; 
        print $fh_c "$keyword\t$num\n" ;
    }
    $R -> run
    (
        "library(wordcloud)" ,
        "library(Cairo)" ,
        "library(showtext)" ,
        # Loading Google fonts (http://www.google.com/fonts)
        q`font.add.google("Gochi Hand", "gochi")` ,
        q`font.add.google("Schoolbell", "bell")` ,
        q`font.add.google("Covered By Your Grace", "grace")` ,
        q`font.add.google("Rock Salt", "rock")` ,

        # Automatically use showtext to render text for future devices
        "showtext.auto()" ,
        "data_word = read.table('$txt_word',encoding='UTF-8',sep = \"\t\")" ,
        "mycolor <- colorRampPalette(c('blue', 'red'))(800)" ,
        "CairoJPEG(filename='$photo', width=800,height=800,units='px')" ,
        q`wordcloud(data_word$V1,data_word$V2,c(6,1),random.order=FALSE,color=mycolor)`,
        "dev.off()"
    );
    
=pod
    # -----------------------------------------------------------------
    # jieba（R） 实现的分词
    # 环境需要：R > 3.0 & GCC > 4.6 
    my $R = Statistics::R->new(bin => '/usr/local/bin/R');		# 这里需要显式地配置R路径，否则crontab无效
    my $output = $R->run(
        q`library(jiebaR)` ,
        q`keys = worker("keywords", topn = 30,encoding = 'UTF-8')` ,
        "keys <= '$txt'"
    );
    my $keywords = join "\t" , map {s/^"|"$//g;$_} grep {/"/} split " " , $output ;
    $R->stop();
    # ------------------------------------------------------------------
    unlink($txt) ;
    
    # 这里测试发现 $keywords 生成的程序在 crontab 计划执行时居然表现为八进制显示编码 所以需要转一下
    $keywords =~ s/(?<=\\)(\d{3})/sprintf("%x",oct($1))/eg;
    $keywords =~ s/\\/%/g ;
    $keywords =~ s/\%([A-Fa-f0-9]{2})/pack('C', hex($1))/seg; 
    insert_redis_scalar('L99::A::content::article::keywords_'.$key_day , $keywords) if $keywords;
=cut
}
#=cut


=pod	# useragent的保存逻辑注释掉了，储存的代价太高
# ---------------------------------------------------------------
# L99::A::user_OS 用户使用的操作系统
# ---------------------------------------------------------------
say "-> Redis.L99::A::user_OS" ;
my $result_os = $db_l99 -> run_command(
    [
       "distinct" => "ua",
       "key"      => "os",
       "query"    => {}
    ]
);
foreach(@{$result_os->{values}}){
    my $os = $_ ;
    my $num = $collection_ua -> count({os => $os}) ;
    
    my $rediskey_user_os = 'L99::A::user_OS_{'.$os.'}' ;
    insert_redis_scalar($rediskey_user_os,$num) ;
}

# ---------------------------------------------------------------
# L99::A::user_BROWSER 用户使用的浏览器
# ---------------------------------------------------------------
say "-> Redis.L99::A::user_BROWSER" ;
my $result_browser = $db_l99 -> run_command(
    [
       "distinct" => "ua",
       "key"      => "name",
       "query"    => {}
    ]
);
foreach(@{$result_browser->{values}}){
    my $bowser_name = $_ ;
    my $result_browser_version = $db_l99 -> run_command(
	[
	    "distinct" => "ua",
	    "key"      => "version",
	    "query"    => {name => $bowser_name}
	]
    );
    foreach(@{$result_browser_version->{values}}){
	my $version = $_ ;
	my $num = $collection_ua -> count({name => $bowser_name , version => $version}) ;
	
	my $rediskey_user_browser = "L99::A::user_BROWSER_{'$bowser_name','$version'}" ;
	insert_redis_scalar($rediskey_user_browser,$num) ;
    }
}

say "-> Nodejs.os.json" ;
my %os = ('Android' => [] ,'BlackBerry' => [],'ChromeOS' => [],'Linux' => [],'Mac OSX' => [] ,'Nintendo 3DS' => [],'SymbianOS' => [] ,
          'PlayStation' => ["PlayStation Portable","PlayStation Vita"] ,'iOS' => ["iOS","iPad","iPhone","iPod"] ,
          'Windows' => ["Windows 2000","Windows 7","Windows 8","Windows 8.1","Windows 95","Windows 98","Windows CE",
              "Windows NT 4.0","Windows Phone OS","Windows UNKNOWN Ver","Windows Vista","Windows XP"] ,
          'Other' => ["au","docomo","emobile","jig","UNKNOWN","Mobile Transcoder"] ) ;
open my $fh_os, '>:utf8', $L99_os_json;
print $fh_os Encode_OS_JSON(\%os,'os') ;

say "-> Nodejs.browser.json" ;
my %browser = ('Other' => ["UNKNOWN","Mobile Transcoder"] , 'PlayStation' => ["PlayStation Portable","PlayStation Vita"] , 
               'Chrome' => [] ,'Firefox' => [],'Internet Explorer' => [] ,'Nintendo 3DS' => [],'Opera'=>[],'Safari' => [],'SymbianOS' => [] ,
               'Bot' => ["Baiduspider","Genieo Web Filter","Google AppEngine","Google Desktop","Google Feedfetcher",
			 "Google Web Preview","Googlebot","Googlebot Mobile","HTTP Library","Indy Library","RSSReader",
			 "Windows RSSReader","Yahoo! Slurp","ahref AhrefsBot","au by KDDI","bingbot","docomo","emobile",
			 "facebook","jig browser","misc crawler","msnbot"]) ;
open my $fh_browser, '>:utf8', $L99_browser_json;
print $fh_browser Encode_BROWSER_JSON(\%browser,'browser') ;
=cut


# ====================================== functions ======================================

# -----------------------------------------------------------------------------------------
# get user_info.[userl99No,username,status] from accountId -- TABLE:account
# -----------------------------------------------------------------------------------------
sub get_user_from_accountId
{
	my ($dbh,$accountId) = @_ ;
	my $ref_accountId ;
	my $sth_account = $dbh -> prepare(" SELECT accountId,l99NO,name,status FROM account WHERE accountId = $accountId ") ;
	$sth_account -> execute();
	while (my $ref_account = $sth_account -> fetchrow_hashref())
	{
		my $accountId = $ref_account -> {accountId};
		my $l99NO     = $ref_account -> {l99NO};
		my $name      = $ref_account -> {name};
		my $status    = $ref_account -> {status} ;
		$ref_accountId -> {l99NO}  = $l99NO ;
		$ref_accountId -> {name}   = $name ;
		$ref_accountId -> {status} = $status ;
	}
	$sth_account -> finish ;
	return $ref_accountId ;
}


# -----------------------------------------------
# "textId" => "title	author"
# -----------------------------------------------
sub decode_textId
{
    my ($textId) = @_ ;
    my $html = get("http://www.l99.com/EditText_view.action?textId=$textId") ;
    my ($author) = $html =~ /name="author" content="(.*?)_\S+"/ ;
    my ($title)  = $html =~ /<title>(.*?)\|/ ;
    return encode_utf8 "$title\t$author";
}

# -----------------------------------------------
# "textId" => "html_main_body"
# -----------------------------------------------
sub text_body
{
    my ($textId) = @_ ;
    my $html = get("http://www.l99.com/EditText_view.action?textId=$textId") ;
    my $content = $html ;
    my $main_body;
    eval{
	$main_body = extract_main_html($content);
    } ;
    $main_body = $html if $@ ;
    $main_body =~ s/<a[\s\S]*?<\/a>//g ;
    $main_body =~ s/<[\s\S]*?>//g ;
    $main_body =~ s/[\r\n\s<>]+|&nbsp;//g ;
    $main_body =~ s/201\dwindow[\s\S]*$// ;
    $main_body."\n" ;
}

sub decode_l99NO
{
    my ($l99NO) = @_ ;
    my ($author) ;
    my $html = get("http://www.l99.com/$l99NO");
    if($html =~ /name="author" content="(.*?)_\S+"/){
	$author = $1 ;
    }else{
	($author) = $html =~ /<li class="">(\S+)[(（]$l99NO/ ;
    }
    return encode_utf8 $author ;
}

sub get_alexa
{
    my $ref ;
    my $html = get("http://www.alexa.com/siteinfo/www.l99.com");
    my ($rank_global) = $html =~ /alt='Global rank icon'[\s\S]*?([,0-9]+)/ ;
    my ($rank_china)  = $html =~ /alt='China Flag'[\s\S]*?>([,0-9]+)</ ;
    s/,//g for ($rank_global,$rank_china) ;
    $ref->{rank_global} = $rank_global ;
    $ref->{rank_china}  = $rank_china ;
    $ref ;
}

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

=pod
sub Encode_OS_JSON
{
    my ($ref_hash,$json_obj_name) = @_ ;
    my %json ;
    $json{name} = $json_obj_name ;
    for(keys %$ref_hash){
        my %temp ;
        my $os_type = $_ ;
        $temp{name} = $os_type ;
        my $types = ~~ @{$$ref_hash{$os_type}} ;
        if( $types == 0){
            $temp{children}->[0]->{name} = $os_type ;
            $temp{children}->[0]->{size} = $redis->get("L99::A::user_OS_{$os_type}")  ;
        }
        else{
            for(0 .. $types - 1){
                my $num = $_ ;
                my $name = @{$os{$os_type}}[$num] ;
                $temp{children}->[$num]->{name} = $name ;
                $temp{children}->[$num]->{size} = $redis->get("L99::A::user_OS_{$name}")  ;
            }
        }
        push @{$json{children}} , \%temp ;
    }
    encode_json \%json;
}

sub Encode_BROWSER_JSON
{
    my ($ref_hash,$json_obj_name) = @_ ;
    my %json ;
    $json{name} = $json_obj_name ;
    for(keys %$ref_hash){
        my %temp ;
        my $os_type = $_ ;
        $temp{name} = $os_type ;
        my $types = ~~ @{$$ref_hash{$os_type}} ;
        if( $types == 0){
            my $i ;
            foreach($redis->keys( "L99::A::user_BROWSER_{'$os_type'*" ) ){
                my $num = $redis -> get($_) ;
                my ($v) = $_ =~ /^L99::A::user_BROWSER_{'.*','(.*)'}$/ ;
                my $browser_name = "$os_type $v" ;
                $temp{children}->[$i]->{name} = $browser_name ;
                $temp{children}->[$i]->{size} = $num ;
                $i ++ ;
            }
        }
        else{
            for(0 .. $types - 1){
                my $position = $_ ;
                my $name = @{$browser{$os_type}}[$position] ;
                foreach($redis->keys( "L99::A::user_BROWSER_{'$name'*" ) ){
                    my $num = $redis -> get($_) ;
                    my ($name,$v) = $_ =~ /^L99::A::user_BROWSER_{'(.*)','(.*)'}$/ ;
                    my $browser_name = "$name $v" ;
                    $temp{children}->[$position]->{name} = $browser_name ;
                    $temp{children}->[$position]->{size} = $num ;
                }
            }
        }
        push @{$json{children}} , \%temp ;
    }
    encode_json \%json;
}
=cut

=pod
sub keywords
{
    my ($str,$topN) = @_ ;
    return cut_jieba($str,$topN) ;
}

use Inline Python => <<'PYTHON_CODE';
import jieba
import jieba.analyse

def cut_jieba(str,topN):
    return "\t".join(jieba.analyse.extract_tags(str,topN))

PYTHON_CODE

=pod
# hello
from snownlp import SnowNLP
def nlp(str):
    s = SnowNLP(str)
    return s.words
