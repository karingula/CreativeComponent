############################################################################
#Purpose:  This perl code creates all the necessary tables in the database
#          along with imposing all necessary constraints on tables.

#Author:   Vijay Shanker Karingula
#Language: Perl
#Database: PostgreSQL
############################################################################

#!usr/bin/perl

use DBI;
use strict;
my $driver = "Pg";
my $database = "super_store";
my $dsn="DBI:$driver:dbname=$database";

my $userid = '';
my $password = '';
my $dbh = DBI->connect($dsn, $userid,$password, {RaiseError=>1, AutoCommit =>0})
        or die "Error connecting to the database: $DBI::errstr\n";
print "Opened database successfully.....\n";

eval {
    createTables($dbh);
    setConstraints($dbh);
  
    $dbh->commit;   # commit the changes if we get this far
  };

if ($@) {
    print "\n\nTransaction aborted because $@\n\n";
    # now rollback to undo incomplete changes,
    #if any of the above subroutines fails
    eval { $dbh->rollback };
  }

####################   Create Tables    ####################
sub createTables {
  my $dbh = $_[0];
  my $stmt = qq(CREATE TABLE IF NOT EXISTS orders(
                unique_id INTEGER,
                order_id INTEGER,
                order_date DATE,
                order_priority VARCHAR(100),
                order_quantity INTEGER,
                customer_id INTEGER,
                shipping_date DATE,
                shipping_mode VARCHAR(100),
                is_return BOOLEAN);
              );

  my $rv = $dbh->do($stmt);
  if($rv<0) {
    print DBI::errstr;
  }

  $stmt = qq(CREATE TABLE IF NOT EXISTS customer(
             customer_id INTEGER,
             customer_name VARCHAR(100),
             customer_segment VARCHAR(100),
             province_id INTEGER);
            );

  $rv = $dbh->do($stmt);
  if($rv<0) {
    print DBI::errstr;
  }
 
  $stmt = qq(CREATE TABLE IF NOT EXISTS province(
             province_id INTEGER,
             province_name VARCHAR(100),
             region_id INTEGER);
            );
 
  $rv = $dbh->do($stmt);
  if($rv<0) {
    print DBI::errstr;
  }
 
  $stmt = qq(CREATE TABLE IF NOT EXISTS region(
             region_id INTEGER,
             region_name VARCHAR(100));
            );

  $rv = $dbh->do($stmt);
  if($rv<0) {
    print DBI::errstr;
  }
 
  $stmt = qq(CREATE TABLE IF NOT EXISTS sales(
             unique_id INTEGER,
             sales_id INTEGER,
             order_id INTEGER,
             pid INTEGER,
             sales NUMERIC,
             discount REAL,
             profit NUMERIC);
            );

  $rv = $dbh->do($stmt);
  if ($rv<0) {
    print DBI::errstr;
  }
 
  $stmt = qq(CREATE TABLE IF NOT EXISTS product(
             pid INTEGER,
             pname VARCHAR(100),
             pcat_id INTEGER,
             price NUMERIC,
             pbase_margin REAL);
            );
            
  $rv = $dbh->do($stmt);
  if ($rv<0) {
    print DBI::errstr;
  }

  $stmt = qq(CREATE TABLE IF NOT EXISTS product_category(
             pcat_id INTEGER,
             pmain_catid INTEGER,
             pcat_name VARCHAR(100));
            );

  $rv = $dbh->do($stmt);
  if ($rv<0) {
    print DBI::errstr;
  }

  $stmt = qq(CREATE TABLE IF NOT EXISTS product_main_category(
             pmain_catid INTEGER,
             pmain_catname VARCHAR(100));
            );

  $rv = $dbh->do($stmt);
  if ($rv<0) {
    print DBI::errstr;
  }

} #createTables

##################    Imposing constraints in Tables    ######################

sub setConstraints {
  my $dbh = $_[0];
  eval{
    my $stmt = qq(CREATE SEQUENCE product_main_catid;
                  ALTER TABLE product_main_category ALTER COLUMN pmain_catid SET DEFAULT NEXTVAL('product_main_catid');
                  ALTER SEQUENCE product_main_catid OWNED BY product_main_category.pmain_catid;
                  ALTER TABLE product_main_category ADD PRIMARY KEY(pmain_catid);
                );
    my $rv = $dbh->do($stmt);
    if ($rv<0) {
      print DBI::errstr;
    }
    else {
      print"'product_main_category' table created successfully...\n";
    }

    $stmt = qq(CREATE SEQUENCE rid;
               ALTER TABLE region ALTER COLUMN region_id SET DEFAULT NEXTVAL('rid');
               ALTER SEQUENCE rid OWNED BY region.region_id;
               ALTER TABLE region ADD PRIMARY KEY(region_id);
            );
    $rv = $dbh->do($stmt);
    if ($rv<0) {
      print DBI::errstr;
    }
    else {
      print"'region' table created successfully...\n";
    }

    $stmt = qq(CREATE SEQUENCE product_cat_id;
               ALTER TABLE product_category ALTER COLUMN pcat_id SET DEFAULT NEXTVAL('product_cat_id');
               ALTER TABLE product_category ADD CONSTRAINT pmain_catid_fk FOREIGN KEY(pmain_catid) REFERENCES product_main_category(pmain_catid) MATCH FULL;
               ALTER SEQUENCE product_cat_id OWNED BY product_category.pcat_id;
               ALTER TABLE product_category ADD PRIMARY KEY(pcat_id);
            );
    $rv = $dbh->do($stmt);
    if ($rv<0) {
      print DBI::errstr;
    }
    else {
      print"'product_category' table created successfully...\n";
    }

    $stmt = qq(CREATE SEQUENCE provnc_id;
               ALTER TABLE province ALTER COLUMN province_id SET DEFAULT NEXTVAL('provnc_id');
               ALTER TABLE province ADD CONSTRAINT region_id_fk FOREIGN KEY(region_id) REFERENCES region(region_id);
               ALTER SEQUENCE provnc_id OWNED BY province.province_id;
               ALTER TABLE province ADD PRIMARY KEY(province_id)
            );
    $rv = $dbh->do($stmt);
    if ($rv<0) {
      print DBI::errstr;
    }
    else {
      print"'province' table created successfully...\n";
    }

    $stmt = qq(CREATE SEQUENCE product_id;
               ALTER TABLE product ALTER COLUMN pid SET DEFAULT NEXTVAL('product_id');
               ALTER TABLE product ADD CONSTRAINT pcat_id_fk FOREIGN KEY(pcat_id) REFERENCES product_category(pcat_id);
               ALTER SEQUENCE product_id OWNED BY product.pid;
               ALTER TABLE product ADD PRIMARY KEY(pid);
            );
    $rv = $dbh->do($stmt);
    if ($rv<0) {
      print DBI::errstr;
    }
    else {
      print"'product' table created successfully...\n";
    }

    $stmt = qq(CREATE SEQUENCE cust_id;
               ALTER TABLE customer ALTER COLUMN customer_id SET DEFAULT NEXTVAL('cust_id');
               ALTER TABLE customer ADD CONSTRAINT province_id_fk FOREIGN KEY(province_id) REFERENCES province(province_id);
               ALTER SEQUENCE cust_id OWNED BY customer.customer_id;
               ALTER TABLE customer ADD PRIMARY KEY(customer_id);
            );
    $rv = $dbh->do($stmt);
    if ($rv<0) {
      print DBI::errstr;
    }
    else {
      print"'customer' table created successfully...\n";
    }

    $stmt = qq(CREATE SEQUENCE uniq_id;
               ALTER TABLE orders ALTER COLUMN unique_id SET DEFAULT NEXTVAL('uniq_id');
               ALTER TABLE orders ADD CONSTRAINT customer_id_fk FOREIGN KEY(customer_id) REFERENCES customer(customer_id) MATCH FULL;
               ALTER SEQUENCE uniq_id OWNED BY orders.unique_id;
               ALTER TABLE orders ADD PRIMARY KEY(unique_id);
            );
    $rv = $dbh->do($stmt);
    if ($rv<0) {
      print DBI::errstr;
    }
    else {
      print"'orders' table created successfully...\n";
    }

    $stmt = qq(CREATE SEQUENCE sales_id;
               ALTER TABLE sales ALTER COLUMN sales_id SET DEFAULT NEXTVAL('sales_id');
               ALTER TABLE sales ADD constraint uniq_id_fk FOREIGN KEY(unique_id) REFERENCES orders(unique_id);
               ALTER TABLE sales ADD CONSTRAINT pid_fk FOREIGN KEY(pid) REFERENCES product(pid);
               ALTER SEQUENCE sales_id OWNED BY sales.sales_id;
               ALTER TABLE sales ADD PRIMARY KEY(sales_id);
            );
    $rv = $dbh->do($stmt);
    if ($rv<0) {
      print DBI::errstr;
    }
    else {
      print"'sales' table created successfully\n";
    }

  }; # Transaction Ends
} # setConstraints

 
