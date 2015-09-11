#!/usr/bin/perl -w

# Note: The use of the -w is to make Perl warn us of any potential dangers in our code.
# It's nearly always a good idea to put the -w in!


use strict;
use warnings;
use CGI;
use CGI::Carp "fatalsToBrowser";
use HashDB;

my $q = new CGI (\&hook);


 #In this file upload hook we can update our session
 #file with the details toshow the upload progress
sub hook {
        my ($filename,$buffer,$bytes_read,$file) = @_;

         #Get our sessid from the form submission.
        my ($sessid) = $ENV{QUERY_STRING};
        $sessid =~ s/[^A-F0-9]//g;

         #Calculate the (rough estimation) of the file size. This isn't
         #accurate because the CONTENT_LENGTH includes not only the file's
         #contents, but also the length of all the other form fields as well,
         #so it's bound to be at least a few bytes larger than the file size.
         #This obviously doesn't work out well if you want progress bars on
         #a per-file basis, if uploading many files. This proof-of-concept only
         #supports a single file anyway.
        my $length = $ENV{'CONTENT_LENGTH'};
        my $percent = 0;
        if ($length > 0) { # Don't divide by zero.
                $percent = sprintf("%.1f",
                        (( $bytes_read / $length ) * 100)
                );
        }

         #Write this data to the session file.
        open (SES, ">$sessid.session");
        print SES "$bytes_read:$length:$percent";
        close (SES);
}

# Header defined in HTTP and will make use of MIME.
print "Content-Type: text/html\n\n";

my $dir = "/Users/vk2/Sites/uploads";
my $berkeley_dbh = HashDB::dbh_berkeleyDB($dir);
my $key = "filename";

my $action = $q->param("do") || "unknown";
if ($action eq "upload") {
        # They are first submitting the file. This code doesn't really run much
        # until AFTER the file is completely uploaded.
        my $filename = $q->param("incoming");
        my $handle   = $q->upload("incoming");
        #my $sessid   = $q->param("sessid");
        #$sessid     =~ s/[^A-F0-9]//g;
        $filename =~ s/(?:\\|\/)([^\\\/]+)$/$1/g;
        $berkeley_dbh->db_put($key,$filename);

        #Copy the file to its final location.
        open (FILE, ">$dir/$filename") or die "Can't create file: $!";
        my $buffer;
        while (read($handle,$buffer,2048)) {
                print FILE $buffer;
        }
        close (FILE);
        
        print "Thank you! Your file is now uploaded into the Server"; # <a href=\"$dir/$filename\">Here it is again.</a>";
}

print '<html>';
print '<head>';
print '<title> Gracias! </title>';
print '</head>';
print '<body>';
print '<h2> Now get your file into Database.... </h2>';
print '<form method="post" action = "pipe.cgi" enctype="multipart/form-data">';
print '<input type="submit" name="Upload" value="Load"> </br>';
print '</form>';
print '<small> <span style="color: #FF0000">*</span> Clicking the \'Load\' button above will load the file into Database </small>';
print '</body>';
print '</html>';

1;
