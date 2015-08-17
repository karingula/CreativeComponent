################################################################################

## Purpose: This file contains subroutines to create and delete Berkeley DB file
## Berkeley DB is used to store the temporary data structures as key-value pairs
## In this Project Berkeley DB is used to fetch the file name of Excel sheet

## Author: Vijay Shanker Karingula
## Language: Perl

##################################################################################

package HashDB;

use BerkeleyDB;
use Exporter;
our @ISA = 'Exporter';
our @EXPORT = qw($dbh);

our $dbh;
sub dbh_berkeleyDB {
my ($input_dir) = @_;
my $filename = <$input_dir/data.db>;

$dbh = new BerkeleyDB::Hash(
        -Filename =>$filename,
        -Flags    =>DB_CREATE)
        or die "Error opening $filename: $! $BerkeleyDB::Error\n";
return $dbh;

}

sub delete_berkeley{
 my ($input_dir) = @_;
 my $filename = <$input_dir/data.db>;
 my $remove_status = BerkeleyDB::db_remove(
                    -Filename=>$filename);
 #if($remove_status) {
   #print "Old berkeleydb file is deleted\n";
 #}
}

1;
