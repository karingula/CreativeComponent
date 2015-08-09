######################################################################################################

#Purpose:  This perl code loads the data into database tables
#Author:   Vijay Shanker Karingula
#Language: Perl
#Database: PostgreSQL

######################################################################################################


use strict;
use warnings;

use DBI;

# 'Orderutils' is a self-defined utility file coded to support with necessary sub routines
use Orderutils;


my $driver = "Pg";
my $database = "super_store";
my $dsn = "DBI:$driver:dbname=$database";

my $userid='';
my $password='';
my $dbh=DBI->connect($dsn, $userid,$password, {RaiseError=>1, AutoCommit=>0})
        or die "Error connecting to the database: $DBI::errstr\n";
print "Opened database successfully\n";

  my $warn = <<EOS
    Usage:
      
    $0 data-dir
EOS
;

my $input_dir = $ARGV[0];
my @filepaths = <$input_dir/*.txt>;
my %files = map { s/$input_dir\/(.*)$/$1/; $_ => 1} @filepaths;

# Get spreadsheet constants
my %ord  = getColumnsInfo('Orders');
my %ret  = getColumnsInfo('Returns'); 
our ($sql, $sth, $row, @records);
my %return;

  # Use a transaction so that it can be rolled back if there are any errors
  eval {
    loadOrdersData($dbh);  
    loadReturnsData($dbh);
    
    $dbh->commit;   # commit the changes if we get this far
  };
  if ($@) {
    print "\n\nTransaction aborted because $@\n\n";
    # now rollback to undo the incomplete changes
    # but do it in an eval{} as it may also fail
    eval { $dbh->rollback };
  }
  
$dbh->disconnect();
print "\n\nScript completed\n\n";


###### Sub routines for loading  ######

sub loadOrdersData {
    my $dbh = $_[0];
    #my $fields;
  
    my $table_file = "$input_dir/$ord{'worksheet'}.txt";
    print "Loading $table_file...\n";
    
    @records = readFile($table_file);
    my $line_count = 0;
    foreach my $fields (@records) {
      $line_count++;
      my $row_id = $fields->{$ord{'row_id_fld'}};
      my $region_id = setRegionRec($dbh, $fields);
      my $province_id = setProvinceRec($dbh, $fields, $region_id);
      my $customer_id = setCustomerRec($dbh, $fields, $province_id);
      my $uniq_id = setOrderRec($dbh, $fields, $customer_id);
      my $pmain_catid = setProduct_Main_Category($dbh, $fields);
      my $pcat_id = setProduct_Category($dbh, $fields, $pmain_catid);
      my $pid = setProductRec($dbh, $fields, $pcat_id);
      setSalesRec($dbh, $fields, $uniq_id, $pid);
    }
}

sub loadReturnsData {
  my $dbh = $_[0];
  #my $fields;
  
  my $table_file = "$input_dir/$ret{'worksheet'}.txt";
  print "Scanning $table_file...\n";
  
  @records = readFile($table_file);
  my $line_count = 0;
  foreach my $fields(@records) {
    $line_count++;
    my $order_id = $fields->{$ret{'o_id_fld'}};
    print "This is order_id: $order_id\n";
    my $status = $fields->{$ret{'status_fld'}};
    #print "This is status: $status\n";
    if (lc($status) eq 'returned') {
      #$return{$order_id} = 1;
    }
    
  }
}

###################################################################################
###################################################################################


sub setRegionRec {
  my ($dbh,$fields) = @_;
  
  my $region_name = $fields->{$ord{'region_fld'}};
  $sql = "
  INSERT INTO region (region_name)
  VALUES ('$region_name')
  RETURNING region_id";
  
  logSQL('', $sql);
  $sth = doQuery($dbh, $sql);
  $row = $sth->fetchrow_hashref;
  my $region_id = $row->{'region_id'};
  $sth->finish;
  
  return $region_id;
  
}#setRegionRec

sub setProvinceRec {
  my ($dbh, $fields, $region_id) = @_;
  
  my $province_name = $fields->{$ord{'province_fld'}};
  $sql = "
  INSERT INTO province (province_name, region_id)
  VALUES ('$province_name', $region_id)
  RETURNING province_id";
  
  logSQL('', $sql);
  $sth = doQuery($dbh, $sql);
  $row = $sth->fetchrow_hashref;
  my $province_id = $row->{'province_id'};
  $sth->finish;
  
  return $province_id;
  
}#setProvinceRec

sub setCustomerRec {
  my ($dbh, $fields, $province_id) = @_;
  
  my $cust_name = $fields->{$ord{'customer_name_fld'}};
  $cust_name =~ s/\'/\'\'/g;
  my $cust_segment = $fields->{$ord{'customer_segment_fld'}};
  $sql = "
  INSERT INTO customer (customer_name, customer_segment, province_id)
  VALUES ('$cust_name', '$cust_segment', $province_id)
  RETURNING customer_id";
  
  logSQL('', $sql);
  $sth = doQuery($dbh, $sql);
  $row = $sth->fetchrow_hashref;
  my $customer_id = $row->{'customer_id'};
  $sth->finish;
  
  return $customer_id;
}#setCustomerRec

sub setOrderRec {
  my ($dbh, $fields, $customer_id) = @_;
  
  my $oid = $fields->{$ord{'order_id_fld'}};
  my $odate = $fields->{$ord{'order_date_fld'}};
  my $opriority = $fields->{$ord{'order_priority_fld'}};
  my $oquantity = $fields->{$ord{'order_quantity_fld'}};
  my $ship_date = $fields->{$ord{'ship_date_fld'}};
  my $ship_mode = $fields->{$ord{'ship_mode_fld'}};
  my $return_oid = $fields->{$ret{'order_id_fld'}};
  my $returned = 'false';
  
  if ($return{$oid}) {
    $returned = 'true';
  }
  $sql = "
  INSERT INTO orders
  (order_id, order_date, order_priority, order_quantity, customer_id, shipping_date, shipping_mode, is_return)
  VALUES
  ($oid, '$odate', '$opriority', $oquantity, $customer_id, '$ship_date', '$ship_mode', $returned)
  RETURNING unique_id";
  
  logSQL('', $sql);
  $sth = doQuery($dbh,$sql);
  $row = $sth->fetchrow_hashref;
  my $uniq_id = $row->{'unique_id'};
  $sth->finish;
  
  return $uniq_id;
}#setOrderRec

sub setProduct_Main_Category {
  my ($dbh, $fields) = @_;
  
  my $pmain_catname = $fields->{$ord{'product_main_cat_fld'}};
  
  $sql = "
  INSERT INTO product_main_category (pmain_catname)
  VALUES
  ('$pmain_catname')
  RETURNING pmain_catid";
  
  logSQL('', $sql);
  $sth = doQuery($dbh, $sql);
  $row = $sth->fetchrow_hashref;
  my $pmain_catid = $row->{'pmain_catid'};
  $sth->finish;
  return $pmain_catid;
}#setProduct_main_Category

sub setProduct_Category {
  my ($dbh, $fields, $pmain_catid) = @_;
  
  my $pcat_name = $fields->{$ord{'product_cat_fld'}};
  
  $sql = "
  INSERT INTO product_category(pmain_catid,pcat_name)
  VALUES
  ($pmain_catid,'$pcat_name')
  RETURNING pcat_id";
  
  logSQL('', $sql);
  $sth = doQuery($dbh,$sql);
  $row = $sth->fetchrow_hashref;
  my $pcat_id = $row->{'pcat_id'};
  $sth->finish;
  return $pcat_id;
}# setProduct_Category

sub setProductRec {
  my ($dbh, $fields, $pcat_id) = @_;
  
  my $product_name = $fields->{$ord{'product_name_fld'}};
  $product_name =~ s/\'/\'\'/g;
  my $price = $fields->{$ord{'unit_price_fld'}};
  my $pbase_margin = $fields->{$ord{'product_base_margin_fld'}};
  
  $sql = "
  INSERT INTO product (pname,pcat_id,price,pbase_margin)
  VALUES
  ('$product_name',$pcat_id,$price,$pbase_margin)
  RETURNING pid";
  
  logSQL('', $sql);
  $sth = doQuery($dbh, $sql);
  $row = $sth->fetchrow_hashref;
  my $pid = $row->{'pid'};
  $sth->finish;
  return $pid;
}#setProductRec

sub setSalesRec {
  my ($dbh, $fields, $uniq_id, $pid) = @_;
  
  my $sales    = $fields->{$ord{'sales_fld'}};
  my $oid = $fields->{$ord{'order_id_fld'}};
  my $discount = $fields->{$ord{'discount_fld'}};
  my $profit   = $fields->{$ord{'profit_fld'}};
  $sql = "
  INSERT INTO sales (unique_id,order_id, pid, sales, discount, profit)
  VALUES
  ($uniq_id,$oid,$pid,$sales,$discount,$profit)";
  
  logSQL('', $sql);
  $sth = doQuery($dbh, $sql);
  $sth->finish;
}
