#!/usr/bin/env perl
# 请确保Linux已经配置好cpanm工具：
# curl -L http://cpanmin.us | perl - --sudo App::cpanminus
# test
use 5.10.1 ;

BEGIN {
        my @PMs = (
            'Config::Tiny' ,
            'Date::Calc::XS' ,
            'File::Lockfile' ,
            'Unicode::UTF8',
            'JSON::XS' ,
            'LWP::Simple',
            'DBI' ,
            'Redis',
            'MongoDB'
        ) ;
        foreach(@PMs)
        {
                my $pm = $_ ;
                eval {require $pm;};
                if ($@ =~ /^Can't locate/) {
                        print "install module $pm";
                        `cpanm $pm`;
                }
        }
}
