#!/usr/bin/perl
# This source code is subject to the terms of the MIT License. If a
# copy of the MIT License was not distributed with this file, you can
# obtain one at http://opensource.org/licenses/MIT.
# MMDB format docs: https://maxmind.github.io/MaxMind-DB/
# MaxMind::DB:Writer::Tree API docs: https://metacpan.org/pod/MaxMind::DB::Writer::Tree

use strict;
use warnings;

use Data::Dumper;
use Getopt::Long qw(GetOptions);
use List::Util;
use MaxMind::DB::Writer::Tree;
use Net::Works::Network;
use Getopt::Long;
use DBI;
use utf8;
use POSIX qw(strftime);
binmode(STDIN,":encoding(gbk)");

###################数据库连接信息###################
my $host = "192.168.44.53";     # 主机地址
my $database = "cn_api";        # 数据库
my $username = "root";          # 数据库用户名
my $password = "111111";        # 数据库密码

my @args = @ARGV;
my $where= @args ? " WHERE knowledge_id=".$args[0]:"";
my $sql = "SELECT ip1,ip2,asn,organization FROM `cn_ip_asn_built_in` $where";
# 驱动程序对象的句柄
my $dsn = "DBI:mysql:database=$database:$host";
my $outfile = 'asn_builtin.mmdb';
###################数据库连接信息###################

my $dbh = DBI->connect($dsn, $username, $password ) or die $DBI::errstr;
print "HOST: $host, DB: $database connect: ". ($dbh->ping() ? "success" : "fail")."
";
$dbh->do("SET NAMES utf8");
$dbh->{'mysql_enable_utf8mb4'} = 1;
my $sth = $dbh->prepare("$sql");
$sth->execute();

my $record_size = 24;
my $db_type = "ASNLOCATOR";
my $ip_version = 6;

GetOptions(
	'db-type=s' => \$db_type,
	'ipv=i' => \$ip_version,
	'record-size=i' => \$record_size,
) or die "Usage: $0 --db-type TYPE --ipv VERSION --record-size SIZE\n";

my $db_name = "ASNLOCATOR";
my $db_description = "IP Geolocation Library";
my %types = (
	ASN				      => 'utf8_string',
	COUNTRY_CODE 		  => 'utf8_string',
	ORGANIZATION      	  => 'utf8_string',
);

my $tree = MaxMind::DB::Writer::Tree->new(
	remove_reserved_networks => 0,
	ip_version            => $ip_version,
	record_size           => $record_size,
	database_type         => $db_name,
	languages             => ['en', 'zh'],
	description           => { en => $db_description, zh => 'ASN定位库'},
	map_key_type_callback => sub { $types{ $_[0] } },
);

while (my $ref = $sth->fetchrow_hashref()) {

    my $start_ip = $ref->{'ip1'};
    my $mask = $ref->{'ip2'};
    my $netblock = $start_ip.'/'.$mask;

    if ($start_ip) {
        my $network = Net::Works::Network->new_from_string( string => $netblock );
        my %data = ('ASN'=>isNotBlank($ref->{'asn'}),'ORGANIZATION'=>isNotBlank($ref->{'organization'}));
        $tree->insert_network($network, \%data);
    }
};

open my $fhfile, '>:raw', $outfile;
$tree->write_tree($fhfile);
close($outfile);
print "The perl script is finished!\n";

sub isNotBlank {
   my $tmp = $_[0];
   if(defined $tmp){
     return $tmp;
   } else{
     return "";
   }
}

