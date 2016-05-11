#!/usr/bin/env perl 
# ==============================================================================
# function:		床上项目数据统计
# Author:		kiwi
# createTime:	2015.4.7
# ==============================================================================
use 5.10.1 ;

BEGIN
{
    my @PMs = (
		   #'Redis' ,
           #'MongoDB' ,
		   #'Config::Tiny' ,
		   #'DBI' ,
		   #'JSON::XS' ,
		   #'Date::Calc::XS' ,
		   #'Time::Local'
	) ;
    foreach(@PMs)
	{
            my $pm = $_ ;
            eval {require $pm;};
            if ($@ =~ /^Can't locate/)
			{
                        print "install module $pm";
                        # 需要Linux事先配置好cpanm环境
						# curl -L http://cpanmin.us | perl - --sudo App::cpanminus
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
use File::Lockfile ; 
use POSIX qw(strftime);
use Time::Local ;
use Date::Calc::XS qw (Date_to_Time Time_to_Date);
use Unicode::UTF8 qw (decode_utf8 encode_utf8);
binmode(STDIN,  ':encoding(utf8)');
binmode(STDOUT, ':encoding(utf8)');
binmode(STDERR, ':encoding(utf8)');
# ----------------------------------------------------------------------------------

#my $lockfile = File::Lockfile->new('cs.lock' , '/home/DA/DataAnalysis');
#if ( my $pid = $lockfile->check ) {
#        say "Seems that program is already running with PID: $pid";
#        exit;
#}
#$lockfile->write;

# -----------------------------------------------------------------------
# read config.ini
# -----------------------------------------------------------------------
my $Config = Config::Tiny->new;
$Config = Config::Tiny->read( '/home/DA/DataAnalysis/config.ini', 'utf8' );
my $version = $Config -> {_} -> {version};

my $redis_host_market = $Config -> {REDIS} -> {host_market};
my $redis_host = $Config -> {REDIS} -> {host};
my $redis_port = $Config -> {REDIS} -> {port};

my $mongo_host = $Config -> {MONGODB} -> {host};
my $mongo_port = $Config -> {MONGODB} -> {port};

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

my $OF_host     = $Config -> {CS_DB}  -> {host_openfire} ;
my $OF_db       = $Config -> {CS_DB}  -> {database_openfire} ;
my $OF_usr      = $Config -> {CS_DB}  -> {username_openfire} ;
my $OF_password = $Config -> {CS_DB}  -> {password_openfire} ;

my $Wwere_host     = $Config -> {Wwere_DB} -> {host};
my $Wwere_db       = $Config -> {Wwere_DB} -> {database} ;
my $Wwere_usr      = $Config -> {Wwere_DB} -> {username};
my $Wwere_password = $Config -> {Wwere_DB} -> {password};

my $time_step = 2000 ;
#my $time_step = $Config -> {time} -> {step} ;													# 设置为往前推 N天 统计，默认 N = 1 	
my $day_start = strftime( "%Y-%m-%d 00:00:00" , localtime(time() - 86400 * $time_step) );		# %Y-%m-%d %H:%M:%S
my ($sec,$min,$hour,$dday,$mmon,$yyear,$wday,$yday,$isdst) = localtime(time - 86400 * $time_step);    
$yyear += 1900;    
my $timestamp_start = timelocal(0,  0,  0 , $dday , $mmon, $yyear);
my $num_month_ago = $time_step / 30 + 1;

# ------------------------------------------------------------------------------------------------
# connect to Redis & mongoDB
# ------------------------------------------------------------------------------------------------
#my $redis    = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
#my $redis_ip = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
#$redis_ip -> select(1) ;
#my $redis_db2 = Redis->new(server => "$redis_host:$redis_port",reconnect => 10, every => 2000);
#$redis_db2 -> select(2) ;
#my $redis_market = Redis->new(server => "$redis_host_market:$redis_port",reconnect => 10, every => 2000);
#$redis_market -> select(6) ;
#my $redis_active = Redis->new(server => "192.168.201.57:6379",reconnect => 10, every => 2000);
#my $mongo = MongoDB::MongoClient->new(host => $mongo_host, port => $mongo_port , query_timeout => 1000000);
#my $collection_present = $mongo -> get_database( 'cs' ) -> get_collection( 'presents' );

# ------------------------------------------------------------------
# connect to mysql
# ------------------------------------------------------------------
#my $dsn   = "DBI:mysql:database=$CS_db;host=$CS_host" ;
#my $dbh   = DBI -> connect($dsn, $CS_usr, $CS_password, {'RaiseError' => 1} ) ;
#$dbh -> do ("SET NAMES UTF8");
#
#my $dsn_openfire = "DBI:mysql:database=$OF_db;host=$OF_host" ;
#my $dbh_openfire   = DBI -> connect($dsn_openfire, $OF_usr, $OF_password, {'RaiseError' => 1} ) ;
#$dbh_openfire -> do ("SET NAMES UTF8");
#
#my $dsn_v506 = "DBI:mysql:database=$L06_db;host=$L06_host" ;
#my $dbh_v506 = DBI -> connect($dsn_v506, $L06_usr, $L06_password, {'RaiseError' => 1} ) ;
#$dbh_v506 -> do ("SET NAMES UTF8");
#
#my $dsn_wwere  = "DBI:mysql:database=$Wwere_db;host=$Wwere_host" ;
#my $dbh_wwere = DBI -> connect($dsn_wwere, $Wwere_usr, $Wwere_password, {'RaiseError' => 1} ) ;
#$dbh_wwere -> do ("SET NAMES UTF8");

my $redis = Redis->new(server => "192.168.199.55:6379",reconnect => 10, every => 2000);
$redis-> select(2) ;
#my $mongo178 = MongoDB::MongoClient->new(host => '192.168.1.178', port => 27017 , query_timeout => 1000000);
my $mongo178 = MongoDB::MongoClient->new();
my $kk = $mongo178 -> get_database( 'kiwi' ) -> get_collection( 'payin' );

foreach($redis->keys( 'CS::payin::*user::*_2014-03-01*' ) )
{
	my $key = $_ ;
    my $v = $redis->get($key) ;
    #say "$key \t=>\t $v" ;
    next if $kk->find_one({ "rediskey" => $key }) ;
    say "$key \t=>\t $v" ;
    $kk -> insert( {"rediskey" => $key , "redisvalue" => $v} );
    
}
#for(100000 .. 666666){
#    my $m = $_ ;
#    say $m;
#    my $n = $m + int rand 1000 ;
#    $kk->update_one( { name => "lll" }, { '$addToSet' => { test2 => $n } } );
#}


=pod
# -----------------------------------------------------------------------------
#  扫描新增用户   insert into MongoDB
# -----------------------------------------------------------------------------

say "-> MongoDB.cs.user " ;
foreach(split(',',$CS_user_dbs))
{
	my $table_user = $_ ;
	#say $table_user ;
	my $database   = $mongo -> get_database( 'cs' );
	my $collection = $database -> get_collection( 'user' );
	
	my $sth_user = $dbh -> prepare("
                                        SELECT accountId,gender,lat,lng,localName,createTime
										FROM
										$table_user
										WHERE
										createTime > '$day_start'
									");
	$sth_user -> execute();
	while (my $ref = $sth_user -> fetchrow_hashref())
	{

		my $accountId  = $ref -> {accountId} ;
		next if $collection->find_one({ accountId => $accountId }) ;    	# 如果Mongo表中有了，就跳过
		
		my $gender     = $ref -> {gender} ;
		my $lat        = $ref -> {lat} ;
		my $lng        = $ref -> {lng} ;
		my $cityName   = decode_utf8 $ref -> {localName} ;
		my $time       = $ref -> {createTime} ;
		
		my $ref_account = get_user_from_accountId($dbh_v506,$accountId) ;
        my ($l99NO,$name) = ($ref_account->{l99NO} , $ref_account->{name}) ;

        
		my $province ;
		if ($cityName =~ /北京|Beijing/i){ $province = 'CN_北京市' ;}
                else{
		    $province = decode_utf8 getProvince($lat,$lng) ;
		}

		$collection -> insert( {"accountId" => $accountId , "l99NO" => $l99NO , "name" => $name ,
					"gender" => $gender , "lat" => $lat , "lng" => $lng ,
					"province" => $province ,"time" => $time} );
	}
	$sth_user -> finish();
}




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
		#say "$key => $value" ;
    }
}

# -----------------------------------------------------------------------------------------
# 根据用户 accountId 获取用户[userl99No,username,status]  -- TABLE:account
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

sub get_gender_from_accountId
{
	my ($dbh,$table,$accountId) = @_ ;
	my $ref_accountId ;
	my $sth_user = $dbh -> prepare(" SELECT accountId,gender,longNO FROM $table WHERE accountId = $accountId ");
    $sth_user -> execute();
    while (my $ref = $sth_user -> fetchrow_hashref())
    {
        	my $accountId  = $ref -> {accountId} ;
        	my $gender     = $ref -> {gender} ;
			my $l99NO      = $ref -> {longNO} ;
			$ref_accountId -> {gender}  = $gender ;
			$ref_accountId -> {l99NO}   = $l99NO ;
	}
	$sth_user -> finish ;
	return $ref_accountId ;
}

sub get_accountId_from_l99NO
{
	my ($dbh,$l99NO) = @_ ;
	my $accountId ;
	my $sth = $dbh -> prepare(" SELECT accountId FROM account WHERE l99NO = $l99NO ") ;
	$sth -> execute();
	while (my $ref = $sth -> fetchrow_hashref())
	{
		$accountId = $ref -> {accountId};
	}
	$sth -> finish ;
	return $accountId ;
}

# --------------------------------------------------------------------------------
# 根据用户 accountId 获取用户系统
# --------------------------------------------------------------------------------
sub get_user_market_from_accountId
{
	my ($dbh,$accountId) = @_ ;
	my $ref_accountId ;
	my $os ;
	my $sth_account = $dbh -> prepare(" SELECT accountId,market FROM account_log WHERE accountId = $accountId ") ;
	$sth_account -> execute();
	while (my $ref_account = $sth_account -> fetchrow_hashref())
	{
		my $accountId = $ref_account -> {accountId};
		my $market    = $ref_account -> {market} ;
		
		if ($market =~ /iPhone/i){
			$os = 'ios'  ;
		}else{
			$os = 'android' ;
		}
		$ref_accountId -> {market}  = $os ;
	}
	$sth_account -> finish ;
	return $ref_accountId ;
}

sub get_user_market2_from_accountId
{
	my ($dbh,$accountId) = @_ ;
	my $ref_accountId ;
	my $os ;
	my $sth_account = $dbh -> prepare(" SELECT accountId,market FROM account_log WHERE accountId = $accountId ") ;
	$sth_account -> execute();
	while (my $ref_account = $sth_account -> fetchrow_hashref())
	{
		my $accountId = $ref_account -> {accountId};
		my $market    = $ref_account -> {market} ;
		
		if ($market =~ /iPhone/i){
			$os = 'AppStore'  ;
		}else{
			($os) = $market =~ /chuangshang_(.+)/ ;
		}
		$ref_accountId -> {market}  = $os ;
	}
	$sth_account -> finish ;
	return $ref_accountId ;
}


sub get_user_version
{
	my ($redis_p,$time) = @_ ;
	my $ref_version ;
	foreach( $redis_p->keys( 'STORM::CS::user::version*_'.$time ) )
	{
		my $key = $_ ;
		my ($version) = $key =~ /version::(.*?)_/ ;
		my @temp = $redis_p->smembers($key) ;
		foreach(@temp){
			my $l99NO = $_ ;
			$ref_version -> {$l99NO} = $version ;
		}
	}
	return $ref_version ;
}

sub unix2local
{
    my ($time) = @_ ;
    my ($sec,$min,$hour,$day,$mon,$year,$wday,$yday,$isdst) = localtime $time ;
    $year += 1900;    
    $mon ++;
    foreach($mon,$day,$hour,$min,$sec){ $_ = sprintf("%02d", $_); }
    my $local = "$year-$mon-$day $hour:$min:$sec" ;
    return $local ;
}

sub local2unix
{
	my ($time) = @_ ;
	my ($yyear,$m,$dday,$h,$mi,$s) = $time =~ /^(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)$/ ;
	my $mmon = $m - 1 ;
	my $timestamp = timelocal($s,  $mi,  $h , $dday , $mmon, $yyear);
	#my $timestamp = timegm($s,  $mi,  $h , $dday , $mmon, $yyear);		# timelocal + 3600 * 8
	return $timestamp ;
}

# --------------------------------------------------------------------------------------------
# 根据 lat&lng 取 countryId_province
# 这里用的是扫本地库POI的方式来判定，虽然正确率不会达到严格意义的100%，但是不用走webAPI，可持续性更好
# 本算法的正确率依赖于本地数据库中POI的分布密度与POI省份字段的正确率，目前大致可用
# --------------------------------------------------------------------------------------------
sub getProvince
{
    my ($lat_cs,$lng_cs) = @_ ;
    return 0 if $lat_cs == 0 && $lng_cs == 0 ;
    my $lat = substr($lat_cs,0,10) ;
    my $lng = substr($lng_cs,0,10) ;
    my $step = 0.003 ;
    my %city ;
    while (! ~~ keys %city)
    {
        my $sql = "SELECT cityId FROM townfile_local WHERE lat > $lat - $step and lat < $lat + $step and lng > $lng - $step and lng < $lng + $step ;" ;
        my $sth = $dbh_wwere -> prepare($sql);
        $sth -> execute();
        while (my $ref = $sth -> fetchrow_hashref()){
            my $cityId = $ref -> {'cityId'} ;
            $city{$cityId} ++ ;
        }
        $step += 0.01;
    }
    my ($countryId,$province) ;
    my ($cityId) = map {$_} sort {$city{$b} <=> $city{$a}} keys %city ;
    
    my $sth_city = $dbh_wwere -> prepare("SELECT countryId,province FROM city where cityId = $cityId") ;
    $sth_city -> execute();
    while (my $ref = $sth_city -> fetchrow_hashref()){
	$countryId = $ref -> {countryId} ;
	$province  = $ref -> {province} ;
    }
    $sth_city -> finish() ;
    return $countryId.'_'.$province ;
}
