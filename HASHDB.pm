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
