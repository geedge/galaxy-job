#!/usr/bin/perl
# This source code is subject to the terms of the MIT License. If a
# copy of the MIT License was not distributed with this file, you can
# obtain one at http://opensource.org/licenses/MIT.
use strict;      #use this all times
use warnings;    #this too - helps a lot!
use JSON;
use MaxMind::DB::Writer::Tree;
use Getopt::Long;
use DBI;
use utf8;
use POSIX qw(strftime);
binmode(STDIN,":encoding(gbk)");

###################数据库连接信息###################

#接收参数，第一个参数为addr_type,缺省为4
my @args = @ARGV;
my $addrType = 4;
$addrType= @args && $args[0] ?$args[0]:4;
my $whereKnowledgeId= @args && $args[1] ? " AND knowledge_id=".$args[1]:"";

my $where="WHERE addr_type=$addrType".$whereKnowledgeId;

my $host = "192.168.44.53";         # 主机地址
my $driver = "mysql";           # 接口类型 默认为 localhost
my $database = "cn-dev-2306";        # 数据库
my $sql = "SELECT addr_type,start_ip,end_ip,latwgs,lngwgs,isp,continent,country,province,city FROM cn_ip_location_built_in $where";
# 驱动程序对象的句柄
my $dsn = "DBI:$driver:database=$database:$host";
my $userid = "root";            # 数据库用户名
my $password = "111111";        # 数据库密码
###################数据库连接信息###################

###################日志文件信息###################

printf "sql : $sql \n\n";

my $help = 0;
my $outfileV6 = 'ip6.mmdb';;
my $outfileV4 = 'ip4.mmdb';

my $mmdbLog = 'built-mmdb.log';
my $mmdbStatus = 'built-status.json';
print "The built-in perl script is running!\n";

GetOptions("db=s" => \$outfileV6, 'help|?' => \$help, ) or &help();

GetOptions("db=s" => \$outfileV4, 'help|?' => \$help, ) or &help();

&help() if $help;

my %types = (
        AREA_CODE => 'utf8_string',
        ASN => 'utf8_string',
        ISP => 'utf8_string',
        LATITUDE => 'utf8_string',
        LONGITUDE => 'utf8_string',
        COUNTRY => 'utf8_string',
        CONTINENT => 'utf8_string',
        SUPER_ADMINISTRATIVE_AREA => 'utf8_string',
        ADMINISTRATIVE_AREA => 'utf8_string',
        SUB_ADMINISTRATIVE_AREA => 'utf8_string',
        LOCALITY => 'utf8_string',
        DEPENDENT_LOCALITY => 'utf8_string',
        DOUBLE_DEPENDENT_LOCALITY => 'utf8_string',
);

# V6databse info
my $treeV6 = MaxMind::DB::Writer::Tree->new(
       ip_version            => 6,
       record_size           => 32,
       database_type         => 'IPLOCATOR',
       languages             => ['en'],
       description           => { en => 'My database of IP data' },
       map_key_type_callback => sub { $types{ $_[0] } },
);
# V4databse info
my $treeV4 = MaxMind::DB::Writer::Tree->new(
       ip_version            => 4,
       record_size           => 32,
       database_type         => 'IPLOCATOR',
       languages             => ['en'],
       description           => { en => 'My database of IP data' },
       map_key_type_callback => sub { $types{ $_[0] } },
       remove_reserved_networks => 0,
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
printf MMDBLOG "$gmtDatestring MMDB request success , pageNo: $pageNo \n";

    my $resCount = 0;
    while (my $ref = $sth->fetchrow_hashref()) {
      $resCount++;
      if(($ref->{'addr_type'}) == 4) {
              if($resCount == 1){
                 push(@ipv4Sample, $ref->{'start_ip'});
              }
              $totalv4++;
              $treeV4->insert_range(
                   $ref->{'start_ip'},
                   $ref->{'end_ip'},
              {
                  AREA_CODE => isNotBlank($ref->{'areacode'}),
                  ASN       => isNotBlank($ref->{'asnumber'}),
                  ISP       => isNotBlank($ref->{'isp'}),
                  LATITUDE  => isNotBlank($ref->{'latwgs'}),
                  LONGITUDE => isNotBlank($ref->{'lngwgs'}),
                  CONTINENT => isNotBlank($ref->{'continent'}),
                  COUNTRY   => changeSplit(isNotBlank($ref->{'country'})),
                  SUPER_ADMINISTRATIVE_AREA => changeSplit(isNotBlank($ref->{'province'})),
                  ADMINISTRATIVE_AREA => changeSplit(isNotBlank($ref->{'city'})),
               },
            );
      }elsif (($ref->{'addr_type'}) == 6) {
            if($resCount == 1){
                 push(@ipv6Sample, $ref->{'start_ip'});
            }
            $totalv6++;
            $treeV6->insert_range(
                   $ref->{'start_ip'},
                   $ref->{'end_ip'},
                {
                  AREA_CODE => isNotBlank($ref->{'areacode'}),
                  ASN       => isNotBlank($ref->{'asnumber'}),
                  ISP       => isNotBlank($ref->{'isp'}),
                  LATITUDE  => isNotBlank($ref->{'latwgs'}),
                  LONGITUDE => isNotBlank($ref->{'lngwgs'}),
                  CONTINENT => isNotBlank($ref->{'continent'}),
                  COUNTRY   => changeSplit(isNotBlank($ref->{'country'})),
                  SUPER_ADMINISTRATIVE_AREA => changeSplit(isNotBlank($ref->{'province'})),
                  ADMINISTRATIVE_AREA => changeSplit(isNotBlank($ref->{'city'})),
                },
            );
        };
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

if($addrType == 6) {
    open my $fhV6, '>:raw', $outfileV6;
    $treeV6->write_tree($fhV6);
    close($outfileV6);
    printf MMDBLOG "$gmtDateLog MMDB IPv6 Write Over !\n";

} elsif($addrType == 4) {
    open my $fhV4, '>:raw', $outfileV4;
    $treeV4->write_tree($fhV4);
    close($outfileV4);
    printf MMDBLOG "$gmtDateLog MMDB IPv4 Write Over !\n";
}

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