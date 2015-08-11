#!/usr/bin/perl -w
use warnings;
use strict;

use HashDB;
use BerkeleyDB;
my $dir = '/Users/vk2/Sites/uploads/';
my $filename = <$dir/data.db>;
my $dbh = new BerkeleyDB::Hash(
        -Filename =>$filename,
        -Flags    =>DB_CREATE)
        or die "Error opening $filename: $! $BerkeleyDB::Error\n";
my $key = "filename";
my $value;
my $status = $dbh->db_get($key,$value);
print $status;
if ($status) {
    print "Error: $status\n";
}

else {
    print "$value";
}

#HashDB::delete_berkeley($dir);
