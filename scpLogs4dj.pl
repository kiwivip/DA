#!/bin/env perl
# ==============================================================================
# Author: 	    kiwi
# createTime:	2015.11.3
# ==============================================================================
use 5.10.1;
use POSIX qw(strftime);
use Net::SSH::Perl ;        # libmath-gmp-perl
use Net::SCP::Expect ; 
# -------------------------------------------------------------------------------

my %hosts = (
    '198.11.176.245' => 'access.pintimes.pinup.news.log',
    '198.11.176.253' => 'access.pintimes.pin.news.log',
    '198.11.177.17' => 'access.pintimes.jiemian.news.log' ,
    '47.88.24.225' => 'access.pintimes.pin.news.log' ,
) ;

my ($dj_userName,$dj_passwd) = ('root', 'L99pintimes') ;
my $scp_dj = Net::SCP::Expect->new(user => $dj_userName , password => $dj_passwd , timeout => '10000' );

for (1 .. 1)
{ 
    my $log_day = strftime("%Y%m%d",localtime(time() - 86400 * $_));
    say $log_day ;
    
    for(keys %hosts)
    {
        my $host = $_ ;
        my $logName = $hosts{$host} ;   
        # access.pintimes.pin.news.log-20151030.gz
        my $file_log_local = $logName.'-' .$log_day.'.gz' ;
        say $file_log_local ;
        
        say "file : $file_log_local exists !" and next if -e '/home/LOG/'.$host.'/'.$file_log_local ;
        
        say "start to scp log from $host ... " ;
        $scp_dj->scp( $dj_userName.'@'.$host.':/var/log/nginx/'.$file_log_local  , '/home/LOG/'.$host.'/');
        say "scp $host:$file_log_local success ." ;
    }

}


