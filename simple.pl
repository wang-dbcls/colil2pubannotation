#!/usr/bin/perl
#perl simple.pl output_folder sql_CitationPMC.id_begin sql_CitationPMC.id_and &
#mysql_enable_utf8mb4=1
#use utf8;
#sql join

use strict;
use warnings;
 
use DBI;

use utf8;
use Encode qw(decode encode);
use open ':std', ':encoding(utf8)';

use JSON;

use FreezeThaw qw(cmpStr cmpStrHard); #comparing hashes and arrayes
use Scalar::Util qw(looks_like_number);

if (@ARGV == 0 ) {die "usage: perl *.pl output_folder\nNov. 2021\n"}

my $db = "colil";
my $hostname = "vs13.dbcls.jp";
my $port = "23306";
my $dsn = "DBI:mysql:database=$db;host=$hostname;port=$port;mysql_enable_utf8mb4=1";

my $user = "";
my $password = $user;

my $dbh = DBI->connect($dsn, $user, $password);

my $start = $ARGV[1];
my $stop = $ARGV[2];
my $sql = "select * from CitationPMC join PMC2DOIPMID on CitationPMC.PMID_from = PMC2DOIPMID.PMID join Comments on CitationPMC.CommentID = Comments.id where CitationPMC.id between '$start' and '$stop'"; 
#between 9000001 and 10000000";
my $sth = $dbh->prepare($sql);
$sth->execute();

my $home = "/home/wang/work20/colil";
`mkdir -p $home/result/$ARGV[0]`; 
`rm -rf $home/result/$ARGV[0]/*`;


my (@id, @pmid, @pmcid, @target, @comment_id) = ();
my ($id, $pmid, $pmcid, $target, $comment_id) = ();
my %hash = ();
my (@array, $array) = ();

while (my $ref = $sth->fetchrow_hashref()) {
	push (@id, $ref->{'id'}); #If multiple rows are returned with the same values for the key fields then later rows overwrite earlier ones.
	push (@pmid, $ref->{'PMID_from'});
#	my $obj = $ref->{'PMID_to'};
	my $comment_id = $ref->{'CommentID'};
	push (@comment_id, $comment_id);
	push (@target, decode('utf8', $ref->{'Comment'}));
	push (@pmcid, $ref->{'PMCID'});
	push (@{$hash{$comment_id}}, $ref->{'PMID_to'}); #hash of hashes
#	$array->{$comment_id} = $ref->{'PMID_to'}; #array of hashes
	push (@array, $array);
}

my %tag = ();
foreach my $i (0 .. $#id) {
	my @span = ();
	my @denotations = ();
	my $begin = my $end = 0;
	if ($target[$i] =~ /\>\>(\d+)\<\</) {
		$begin = $-[0];
		$end = $+[0];
		$target[$i] =~ s/\>\>//;
		$target[$i] =~ s/\<\<//;
		push (@span, {begin => 0+ $begin, end =>0+ $end-4});
		push (my @obj, @{$hash{$comment_id[$i]}});
		push (@denotations, {id => $pmid[$i]."-".$begin. "-" .$end."-".$id[$i], span => @span , obj => \@obj});
	}	
	else { @denotations = () }

	my %output = ( 
		text => $target[$i],
		sourcedb => "PMC",
		sourceid => $pmcid[$i],
		denotations => \@denotations,
	);

	if (cmpStr(\%tag, \%output) == 0) {	
	}
	else {
		my $json= to_json ( force_numbers(\%output));
		open OUT, ">>$home/result/$ARGV[0]/$pmcid[$i].json" or die;
		print OUT "$json,";
		%tag = %output;
	}

}


close OUT;

$sth->finish();

#NOT to put quotes around numbers
sub force_numbers {
	if (ref $_[0] eq ""){
		if ( looks_like_number($_[0]) ){
			$_[0] += 0;
		}
	}
	elsif ( ref $_[0] eq 'ARRAY' ){
		force_numbers($_) for @{$_[0]};
	}
	elsif ( ref $_[0] eq 'HASH' ) {
		force_numbers($_) for values %{$_[0]};
	}
        return $_[0];
}   

$dbh->disconnect();

