#################################################################################################

# Purpose:  This file is self defined module(Package) code which is used in other
#           files of the project.
#
# Author:   Vijay Shanker Karingula
# Language: Perl
# Database: PostgreSQL

#################################################################################################

use strict;
use base 'Exporter';
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);

package Orderutils;

our @ISA         = qw(Exporter);
our @EXPORT      = (
                    qw(getColumnsInfo),
                    qw(readFile),
                    qw(logSQL),
                    qw(doQuery)
                    );

sub getColumnsInfo {
    my $ss = $_[0];
    if ($ss eq 'Orders') {
        return (
            'worksheet' => 'Orders',
            'row_id_fld' => 'Row_ID',
            'order_id_fld' => 'Order_ID',
            'order_date_fld'=> 'Order_Date',
            'order_priority_fld' => 'Order_Priority',
            'order_quantity_fld' => 'Order_Quantity',
            'sales_fld' => 'Sales',
            'discount_fld' => 'Discount',
            'ship_mode_fld' => 'Ship_Mode',
            'profit_fld' => 'Profit',
            'unit_price_fld' => 'Unit_Price',
            'shipping_cost_fld' => 'Shipping_Cost',
            'customer_name_fld' => 'Customer_Name',
            'province_fld' => 'Province',
            'region_fld' => 'Region',
            'customer_segment_fld' => 'Customer_Segment',
            'product_main_cat_fld' => 'Product_Category',
            'product_cat_fld' => 'Product_Sub_Category',
            'product_name_fld' => 'Product_Name',
            'product_container_fld' => 'Product_Container',
            'product_base_margin_fld' => 'Product_Base_Margin',
            'ship_date_fld' => 'Ship_Date',                 
        );
    }
    elsif($ss eq 'Returns') {
        return(
               'worksheet' => 'Returns',
               'o_id_fld' => 'Order_ID',
               'status_fld' => 'Status',
               );
    }
    
}#getColumnsInfo

sub readFile {
  use Encode;

  my $table_file = $_[0];
  # execute perl one-liner to fix line endings
  my $cmd = "perl -pi -e 's/(?:\\015\\012?|\\012)/\\n/g' $table_file";
  `$cmd`;

  open IN, "<:utf8", $table_file
      or die "\n\nUnable to open $table_file: $!\n\n";
  my @records = <IN>;
  close IN;

  my @hash_records;
  my (@cols, @field_names, $field_count);
  
  my $header_rows = 0;
  do {
#print $header_rows . ':' . $records[$header_rows] . "\n\n";
    next if ($records[$header_rows] =~ /^##/);  # A double-# marks comments at the head of a worksheet
    chomp $records[$header_rows];chomp $records[$header_rows];
    if ($records[$header_rows] =~ /^#/ && (scalar @field_names) == 0) {
      # First column that starts with #, so must be the header row
      $records[$header_rows] =~ s/#//;
      @cols = split "\t", $records[$header_rows];
      foreach my $col (@cols) {
        next if $col eq 'NULL';
        next if $col =~/TEMP/;
        $col =~ s/^\s+//;
        $col =~ s/\s+$//;
        push @field_names, $col;
      }

      $field_count = (scalar @field_names);
    }#heading row
    $header_rows++;
  } while ((scalar @field_names) == 0);
#print "Got header rows:\n" . Dumper(@field_names);
  
  for (my $i=$header_rows; $i<(scalar @records); $i++) {
    chomp $records[$i];
    next if ($records[$i] =~ /^#/);
    
    my @fields = split "\t", $records[$i];
    next if _allNULL(@fields);
    next if ((scalar @fields) == 0);
    if ((scalar @fields) < $field_count) {
      my $msg = "Insufficient columns. Expected $field_count, found " . (scalar @fields);
      reportError($i, $msg);
    }
    my %hash_record;
    for (my $j=0; $j<(scalar @field_names); $j++) {
      $fields[$j] =~ s/^"//;
      $fields[$j] =~ s/"$//;
      $hash_record{$field_names[$j]} = encode("utf8", $fields[$j]);
      # convert to Perl string format
#      $hash_record{$field_names[$j]} = decode("iso-8859-1", $fields[$j]);
#      $hash_record{$field_names[$j]} = $fields[$j];
    }
    if (!$hash_record{'unique_id'}) {
      $hash_record{'unique_id'} = uniqueID(5);
    }
    
    push @hash_records, {%hash_record};
  }

  print "File had " . (scalar @records) . " lines,";
  print " " . (scalar @hash_records) . " records were read successfully.\n";
  
  return @hash_records;
}#readFile

sub uniqueID {
  my $len = $_[0];
  my @a = map { chr } (48..57, 65..90, 97..122); 
  my $uniq;
  $uniq .=  $a[rand(@a)] for 1..$len;
  return $uniq;
}#uniqueID

sub logSQL {
  my ($datatype, $sql) = @_;  # $datatype now ignored
  my ($sec, $min, $hours, $mday, $month, $year) = localtime;
  $min = $min%10*20;   # new log every 20 minutes
  my $date = "" . (1900+$year) . "-" . ($month+1) . "-$mday:$hours:$min";
  open SQL, ">>log.$date.sql";
  print SQL "$0:\n$sql\n";
  close SQL;
}#logSQL

sub doQuery {
  use Data::Dumper;
  
  my ($dbh, $sql, @vals) = @_;
  if (!@vals) { @vals = (); }
  
  # Translate any UniCode characters in sql statment
  $sql = decode("iso-8859-1", $sql);
  
  # Translate any UniCode chararcters in values array
  if (@vals && (scalar @vals) > 0) {
    my @tr_vals = map{ decode("iso-8859-1", $_) } @vals;
    @vals = @tr_vals;
  }
  my $sth = $dbh->prepare($sql);
  $sth->execute(@vals);
  
  return $sth;
}#doQuery

sub _allNULL {
  my $all_null = 1;
  foreach my $f (@_) {
    my $t = $f;
    $t =~ s/\s//;
    if ( $t ne '' && lc($t) ne 'null' ) { 
      $all_null = 0; 
    }
  }
  return $all_null;
}#_allNULL

1;