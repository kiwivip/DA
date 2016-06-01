#!/usr/bin/env perl 
# ==============================================================================
# function:		床上项目数据统计
# Author:		kiwi
# createTime:	2015.4.7
# ==============================================================================
use 5.10.1 ;            # with CentOS 6.4
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
binmode(STDIN,  ':encoding(utf8)');
binmode(STDOUT, ':encoding(utf8)');
binmode(STDERR, ':encoding(utf8)');

$| = 1;				
# -----------------------------------------------------------------------
# read config.ini
# -----------------------------------------------------------------------
my $Config = Config::Tiny->new;
$Config = Config::Tiny->read( '/home/DA/DataAnalysis/config.ini', 'utf8' );

my $redis_host = $Config -> {REDIS} -> {host};
my $redis_port = $Config -> {REDIS} -> {port};

my $L06_host     = $Config -> {L06_DB} -> {host};
my $L06_db       = $Config -> {L06_DB} -> {database} ;
my $L06_usr      = $Config -> {L06_DB} -> {username};
my $L06_password = $Config -> {L06_DB} -> {password};

my $CS_host      = $Config -> {CS_DB}  -> {host};
my $CS_db        = $Config -> {CS_DB}  -> {database};
my $CS_usr       = $Config -> {CS_DB}  -> {username};
my $CS_password  = $Config -> {CS_DB}  -> {password};
my $CS_user_dbs  = $Config -> {CS_DB}  -> {database_account} ;
my $CS_db_comment= $Config -> {CS_DB}  -> {database_comment} ;
my $CS_log_dir   = $Config -> {CS_LOG} -> {dir} ;
my $article_topN = $Config -> {CS_LOG} -> {topN} ;

#my $time_step = 5 ;
my $time_step = $Config -> {time} -> {step} ;
my $num_month_ago = $time_step / 30 + 1;

my $redis = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
my $redis_db2 = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
$redis_db2 -> select(2) ;

# ------------------------------------------------------------------
# connect to mysql
# ------------------------------------------------------------------
my $dsn   = "DBI:mysql:database=$CS_db;host=$CS_host" ;
my $dbh   = DBI -> connect($dsn, $CS_usr, $CS_password, {'RaiseError' => 1} ) ;
$dbh -> do ("SET NAMES UTF8");

my $dsn_v506 = "DBI:mysql:database=$L06_db;host=$L06_host" ;
my $dbh_v506 = DBI -> connect($dsn_v506, $L06_usr, $L06_password, {'RaiseError' => 1} ) ;
$dbh_v506 -> do ("SET NAMES UTF8");

#=pod
say '-> Redis.CS::A::user::new::auth_MONTH' ;

for ( 1 .. $num_month_ago )	
{
	my $month_ago = $_ - 1 ;
	my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	
	my %temp_month ;
	foreach( $redis->keys( 'CS::A::user::new::auth_'.$month.'-*' ) )
	{
		my $ref = decode_json $redis -> get( $_ ) ;
		foreach(keys %$ref)
		{
			my $type = $_ ;
			my $num = $$ref{$type} ;
			$temp_month{$type} += $num ;
		}
	}
	my $info = encode_json \%temp_month;
	insert_redis_scalar('CS::A::user::new::auth_'.$month , $info) ;
}
#=cut

#=pod
# 新增用户按月的统计，往前取几个月
say "-> Redis.CS::A::user::new*_MONTH" ;

for ( 1 .. $num_month_ago )	
{
	my $month_ago = $_ - 1 ;
	my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	say $month ;
	
	my %user_new_month ;
	foreach( $redis->keys( 'CS::A::user::new*'.'_'.$month.'-*' ) )
	{
		my $key = $_ ;		
		my $type ;
		if ($key =~ /^CS::A::user::new(.*?)_[\d\-]+$/ )
		{
			$type = $1 ;
			next if $type =~ /auth/ ;
			my $num = $redis->get($key);
			$user_new_month{'CS::A::user::new'.$type.'_'.$month  } += $num ;
		}
	}

	insert_redis_hash(\%user_new_month) ;
}

say "-> Redis.CS::A::user::active*_MONTH" ;
for ( 1 .. $num_month_ago)
{
	my $month_ago = $_ - 1 ;
	my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	say $month ;
	my %num;
	my %gender_0 ;
	foreach( $redis->keys( 'CS::user::active_'.$month.'-*' ) )
	{
		my $key = $_ ;
		my @elements = $redis->smembers( $key );
		foreach(@elements){
			$num{$_} = 1  ;
			$gender_0{$_} = 1 if /_gender0/ ;
		}
	}
	my $num_active = keys %num ;
	my $gender_0 = keys %gender_0 ;
	my $gender_1 = $num_active - $gender_0 ;
	
	insert_redis_scalar('CS::A::user::active_'.$month , $num_active) ;
	insert_redis_scalar('CS::A::user::active::gender::1_'.$month , $gender_1) if $gender_1;
	insert_redis_scalar('CS::A::user::active::gender::0_'.$month , $gender_0) if $gender_0;
}
#=cut

#=pod
say "-> Redis.CS::A::content::article::topN_MONTH " ;

for ( 1 .. $num_month_ago )
{
	my $month_ago = $_ - 1 ;
	my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	
	my %temp_month ;
	foreach($redis->keys( 'CS::article_'.$month.'-*' ) )
	{
		my $key = $_ ;							
		my $articles = $redis->get($key) ;
		foreach(split(';',$articles)){
			my ($article,$num) = split ',' , $_ ;
			$temp_month{$article} += $num ;
		}
	}
	my $i = 1;
	foreach ( sort { $temp_month{$b} <=> $temp_month{$a} } keys %temp_month )
	{
		last if $i > $article_topN ;
        my $article = $_ ;
		my $num = $temp_month{$article} ;
		insert_redis_scalar( 'CS::A::content::article::top'.$i.'_'.$month , $article.','.$num ) ;
        $i ++ ;
	}
	
} 

#=pod
# ---------------------------------------------------------------------------------------
# 床上用户内容被选入各版块的数量
# ---------------------------------------------------------------------------------------
say "-> Redis.CS::A::content::guide_MONTH" ;

for ( 1 .. $num_month_ago)	# 往前取几个月
{
	my $month_ago = $_ - 1 ;
	my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
   
	my %guide_month ;
	foreach($redis->keys( 'CS::A::content::guide_'.$month.'-*' ) )
	{
		my $ref_words = decode_json $redis -> get( $_ ) ;
		foreach(keys %$ref_words)
		{
			my $typeId = $_ ;
			my $num = $$ref_words{$typeId} ;
			$guide_month{$typeId} += $num ;
		}
	}
	my $guide_info = encode_json \%guide_month;
	insert_redis_scalar('CS::A::content::guide_'.$month , $guide_info) ;
}

say "-> Redis.CS::A::content::*_MONTH" ;
for ( 1 .. $num_month_ago)
{
	my $month_ago = $_ - 1 ;
	my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	say $month ;
	my %temp ;
	
	foreach($redis->keys( 'CS::A::content::*_'.$month.'-*' ) )
	{
		my $key = $_ ;
		if ($key =~ /^CS::A::content::(.*?)_\d+/)
		{
            my $type = $1 ;
			next if $type =~ /top/ ;
			next unless $type =~ /^article|^photo|^comment|^avatar/ ;
			my $n = $redis->get($key) ;
			$temp{'CS::A::content::'.$type.'_'.$month} += $n ;
        }
        
	}
	
	insert_redis_hash(\%temp) ;
}
#=cut

for ( 1 .. $num_month_ago)
{
	my $month_ago = $_ - 1 ;
	my $month = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
    
	my %present_info ;
	my %month_temp ;
	
	foreach($redis->keys( 'CS::A::payin::present::*_'.$month.'-*' ) )
	{
		my $key = $_ ;
		
		if ($key =~ /^CS::A::payin::(present::\d+.*?)_(\d+-\d+)-\d+$/ )
		{
			my $v = $redis -> get($key) ;
			my ($type,$month) = ($1,$2) ;
			my ($presentId) = $type =~ /present::(\d+)/ ;
			
			my ($count) = $v =~ /"count":(\d+)/  ;
			my ($name)  = $v =~ /"name":"(.*?)"/ ;
			my ($price) = $v =~ /"price":"(.*?)"/ ;
			
			$present_info{$presentId}{name}  = $name ;
			$present_info{$presentId}{price} = $price ;
			
			$month_temp{$type} += $count ;
		}
	}
		
	foreach (%month_temp)
	{
		my $type = $_ ;
		my ($presentId) = $type =~ /present::(\d+)/ ;
		
		my $count = $month_temp{$type} ;
		next unless $count > 0 ;
		my $name  = $present_info{$presentId}{name} ;
		my $price = $present_info{$presentId}{price} ;
		
		my %temp = ('name' => $name , 'price' => $price , 'count' => $count) ;
		my $v_temp = decode_utf8 encode_json \%temp;
		
		insert_redis_scalar('CS::A::payin::'.$type.'_'.$month , $v_temp) ;
	}
	
}


say "-> Redis.CS::A::content::chat*_MONTH" ;

for ( 1 .. $num_month_ago)
{
	my $month_ago = $_ - 1 ;
	my $month      = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	
	my $chat_times;
	foreach( $redis -> keys( 'CS::A::content::chat::times_'.$month.'-*' ) ){
		my $key = $_ ;
		my $n = $redis->get($key) ;
		$chat_times += $n ;
	}
	insert_redis_scalar( 'CS::A::content::chat::times_'.$month , $chat_times) ;
	
	my (%from,%to) ;
	foreach( $redis_db2 -> keys( 'CS::content::chat::from_'.$month.'-*' ) ){
		my $k = $_ ;
		my @ids = $redis_db2->smembers($k);
		$from{$_} = 1 for @ids ;
	}
	foreach( $redis_db2 -> keys( 'CS::content::chat::to_'.$month.'-*' ) ){
		my $k = $_ ;
		my @ids = $redis_db2->smembers($k);
		$to{$_} = 1 for @ids ;
	}
	insert_redis_scalar( 'CS::A::content::chat::from_'.$month , scalar keys %from ) ;
	insert_redis_scalar( 'CS::A::content::chat::to_'  .$month , scalar keys %to   ) ;
}
#=cut

say "-> Redis.CS::A::content::broadcast::N_MONTH" ;

for ( 1 .. $num_month_ago)
{
	my $month_ago = $_ - 1 ;
	my $month      = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	
	my %temp ;
	foreach( $redis -> keys( 'CS::A::content::broadcast::*_'.$month.'-*' ) ){
		my $key = $_ ;
		if ($key =~ /broadcast::(\d+)_/)
		{
            my $type = $1 ;
			my $n = $redis->get($key) ;
			$temp{'CS::A::content::broadcast::'.$type.'_'.$month} += $n ;
        }
	}
	insert_redis_hash(\%temp) ;
}



#=pod
# -----------------------------------
# 用户消费统计  
# -----------------------------------

say "-> Redis.CS::A::payin*_MONTH" ;

for ( 1 .. $num_month_ago)
{
	my $month_ago = $_ - 1 ;
	my $month      = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
	
	foreach( $redis_db2 -> keys( 'CS::payin*:uv*_'.$month ) )
	{
		my $k = $_ ;
		if ($k =~ /^CS::(payin.*?uv.*?)_/ )
		{
			my $type = $1 ;
			my $count = $redis_db2 -> scard($k) ;
			insert_redis_scalar( 'CS::A::'.$type.'_'.$month , $count ) ;
		}
	}
}
#=cut

for ( 1 .. $num_month_ago)
{
	my $month_ago = $_ - 1 ;
	my $month      = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago) )) ;
	my $month_next = strftime("%Y-%m", localtime(time() - 86400 * 30 * ($month_ago - 1) )) ;
    say $month ;
	
	my %pay ;
	my %pay_user ;
	my $pay_type ;
	
	foreach($redis->keys( 'CS::A::payin::*::user_'.$month.'-*' ) )
	{
		my $key_user = $_ ;
		say $key_user ;
		my ($pay_type) = $key_user =~ /^CS::A::payin::(.+)::user/ ;
		foreach($redis -> zrange($key_user, 0, -1))
		{
			my $id = $_ ;
			my $pay = $redis->zscore($key_user , $id) ;
			#say "\t $id => $pay" ;
			$pay_user{$pay_type.'_'.$id} += $pay ;
		}
	}
	foreach( keys %pay_user )
	{
		my ($pay_type,$accountId) = split '_' , $_ ;
		my $pay = $pay_user{$pay_type.'_'.$accountId} ;
		
		$redis->zadd('CS::A::payin::'.$pay_type.'::user_'.$month  , $pay , $accountId) ;
	}
	
	foreach($redis->keys( 'CS::A::payin::[^u]*_'.$month.'-*' ) )
	{
		my $key = $_ ;
		next if $key =~ /::user/ ;
		if ($key =~ /^CS::A::payin::(.*?)_[-\d]+/ )
		{
			my $pay = $redis->get($key) ;
			$pay_type = $1 ;
			next if $pay_type =~ /uv/ ;
			$pay{'CS::A::payin::'.$pay_type.'_'.$month } += $pay ;
		}
	}
	
	insert_redis_hash(\%pay) ;

}

#=cut


# =========================================  functions  ==========================================

sub insert_redis_scalar
{
    my ($rediskey,$redisvalue) = @_ ;
    $redis->set($rediskey,$redisvalue);
    say "$rediskey => $redisvalue" ;
}

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


