#!/usr/bin/perl
# This source code is subject to the terms of the MIT License. If a
# copy of the MIT License was not distributed with this file, you can
# obtain one at http://opensource.org/licenses/MIT.

use strict;
use warnings;

use Data::Dumper;
use Getopt::Long qw(GetOptions);
use MaxMind::DB::Writer::Tree;
use Net::Works::Network;
use Getopt::Long;
use JSON;
use DBI;
use utf8;
use POSIX qw(strftime);
binmode(STDIN,":encoding(gbk)");


#接收参数，第一个参数为addr_type,缺省为4
my @args = @ARGV;
my $addrType = 4;
$addrType= @args && $args[0] ?$args[0]:4;
my $whereKnowledgeId= @args && $args[1] ? " AND knowledge_id=".$args[1]:"";
my $where="WHERE addr_type=$addrType".$whereKnowledgeId;
###################数据库连接信息###################
my $host = "192.168.44.53";         # 主机地址
my $driver = "mysql";           # 接口类型 默认为 localhost
my $database = "cn-dev-2306";        # 数据库
my $sql = "SELECT addr_type,ip1,ip2,asn,organization FROM `cn_ip_asn_built_in` $where";
# 驱动程序对象的句柄
my $dsn = "DBI:$driver:database=$database:$host";
my $userid = "root";            # 数据库用户名
my $password = "111111";        # 数据库密码
###################数据库连接信息###################

###################日志文件信息###################
printf "sql : $sql \n\n";

my $help = 0;
my $outfile = 'asn'.$addrType.'.mmdb';;

my $mmdbLog = 'built-mmdb.log';
my $mmdbStatus = 'built-status.json';
print "The built-in perl script is running!\n";

GetOptions("db=s" => \$outfile, 'help|?' => \$help, ) or &help();
&help() if $help;

my $db_name = "IPLOCATOR";
my $db_description = "IP Geolocation Library";
my %types = (
	ASN				=> 'utf8_string',
	COUNTRY_CODE 			=> 'utf8_string',
	ORGANIZATION      		=> 'utf8_string',
);
my $tree = MaxMind::DB::Writer::Tree->new(
	remove_reserved_networks => 0,
	ip_version            => $addrType,
	record_size           => 24,
	database_type         => $db_name,
	languages             => ['en', 'zh'],
	description           => { en => $db_description, zh => 'IP定位库'},
	map_key_type_callback => sub { $types{ $_[0] } },
);

my $dbh = DBI->connect($dsn, $userid, $password ) or die $DBI::errstr;
print "HOST: $host, DB: $database connect: ". ($dbh->ping() ? "success" : "fail")."
";
$dbh->do("SET NAMES utf8");
$dbh->{'mysql_enable_utf8mb4'} = 1;
my $sth = $dbh->prepare("$sql LIMIT ?,?");   # 预处理 SQL  语句

my $pageNo = 0;
my $pageSize = 200000;
my $totalv4 = 0;
my $totalv6 = 0;
my $total= 0;
my $requestMsg= 'success';
my $requestCode= 0;
my @ipv4Sample= ();
my @ipv6Sample= ();

while(1==1){

my $offset = $pageNo * $pageSize;
$sth->execute($offset,$pageSize);
print "$sql LIMIT $offset,$pageSize; \n";

$pageNo+=1;
#记录日志
my $gmtDatestring = strftime "%Y-%m-%d %H:%M:%S", gmtime;
open(MMDBLOG, "+>>$mmdbLog");

my $resCount = 0;
while (my $ref = $sth->fetchrow_hashref()) {
      $resCount++;
      if ($addrType == 4) {
              if($resCount == 1){
                 push(@ipv4Sample, $ref->{'ip1'});
              }
              $totalv4++;
      } elsif ($addrType == 6) {
            $totalv6++;
            if($resCount == 1){
                 push(@ipv6Sample, $ref->{'ip1'});
            }
      };
      my $start_ip = $ref->{'ip1'};
      my $mask = $ref->{'ip2'};
      my $netblock = $start_ip.'/'.$mask;
      if ($start_ip) {
          my $network = Net::Works::Network->new_from_string( string => $netblock );
          my %data = ('ASN'=>isNotBlank($ref->{'asn'}), 'ORGANIZATION'=>isNotBlank($ref->{'organization'}));
        $tree->insert_network(
            $network,
              \%data
      );
     }
};

$sth->finish();
printf MMDBLOG "$gmtDatestring MMDB write success , pageNo: $pageNo, resultCount: $resCount\n";

if($resCount < $pageSize){
  $requestCode= 200;
  print MMDBLOG "IPv4 total: $totalv4. IPv6 total: $totalv6\n";
  close( MMDBLOG );
        last;
  }
  $resCount = 0;
}
$dbh->disconnect();

open(MMDBLOG, "+>>$mmdbLog");
my $gmtDateLog = strftime "%Y-%m-%d %H:%M:%S", gmtime;

open my $fhfile, '>:raw', $outfile;
$tree->write_tree($fhfile);
close($outfile);
printf MMDBLOG "$gmtDateLog The built-in perl script is finished !\n\n";
close( MMDBLOG );

#状态
my $totalv46 = $totalv4+$totalv6;
$requestMsg = "$requestMsg . Getting result total $totalv46, ipv4 total  $totalv4, ipv6 total $totalv6.";

print "The built-in perl script is finished!\n";

my %statusHash = ('status'=>$requestCode, 'updateTime'=>time(), 'ipv4Total'=>$totalv4, 'ipv6Total'=>$totalv6, 'message'=>$requestMsg,'ipv4Sample'=> \@ipv4Sample, 'ipv6Sample'=> \@ipv6Sample);
my $statusJson = encode_json \%statusHash;
open my $statusFile, '>', $mmdbStatus;
print $statusFile $statusJson;

sub changeSplit {
  my $n = scalar(@_);
  my $i = $_[0];
  $i =~ s/\\b/ /g;

  return "$i"
}

sub isNotBlank {
   my $tmp = $_[0];
  return $tmp ? "$tmp" : " ";
}


sub help {
    print "Usage: $0 -f file.json -d my-ip-data.mmdb\n";
    print 'Example json:
{
  "records": [
    {
      "address": "6.6.6.6/28",
      "service_area": "LAX"
    }
  ]
}' . "\n";
    exit;
};
