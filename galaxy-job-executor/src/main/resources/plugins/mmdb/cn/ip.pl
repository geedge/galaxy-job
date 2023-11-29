#!/usr/bin/perl
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
my $host = "192.168.44.53";    # 主机地址
my $database = "cn_api";       # 数据库
my $userid = "root";
my $password = "111111";

my @args = @ARGV;
my $where= @args && $args[0] ? " WHERE knowledge_id=".$args[0]:"";
my $sql = "SELECT start_ip,end_ip,latwgs,lngwgs,isp,continent,country,province,city FROM cn_ip_location_built_in $where";
my $dsn = "DBI:mysql:database=$database:$host";
###################数据库连接信息###################

###################日志文件信息###################

printf "sql : $sql \n\n";

my $fileName='ip_builtin';
my $outfile = "$fileName.mmdb";
my $mmdbLog = "$fileName.log";
my $mmdbStatus = $fileName.'_status.json';
print "The perl script is running, output to $outfile \n";

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

my $dbh = DBI->connect($dsn, $userid, $password ) or die $DBI::errstr;
print "HOST: $host, DB: $database connect: ". ($dbh->ping() ? "success" : "fail")."
";
$dbh->do("SET NAMES utf8");
$dbh->{'mysql_enable_utf8mb4'} = 1;
my $sth = $dbh->prepare("$sql LIMIT ?,?");

my $pageNo = 0;
my $pageSize = 500000;
my $total= 0;
my $requestMsg= 'success';
my $requestCode= 0;
my @ipSample= ();

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
        if($resCount == 1 && $ref->{'lngwgs'} ){
           push(@ipSample, $ref->{'start_ip'});
        }
        $total++;
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
  $sth->finish();
  printf MMDBLOG "$gmtDatestring MMDB write success , pageNo: $pageNo, resultCount: $resCount\n";

  if($resCount < $pageSize){
      $requestCode= 200;
      print MMDBLOG "IP total: $total.\n";
      close( MMDBLOG );
      last;
  }
  $resCount = 0;
}
$dbh->disconnect();

open(MMDBLOG, "+>>$mmdbLog");
my $gmtDateLog = strftime "%Y-%m-%d %H:%M:%S", gmtime;

if($total==0){
 @ipSample=();
}

open my $fhV6, '>:raw', $outfile;
$treeV6->write_tree($fhV6);
close($outfile);
printf MMDBLOG "$gmtDateLog MMDB IP Write Over !\n";

printf MMDBLOG "$gmtDateLog The built-in perl script is finished !\n\n";
close( MMDBLOG );

#状态
$requestMsg = "$requestMsg . Getting result total $total";

print "The built-in perl script is finished!\n";

my %statusHash = ('status'=>$requestCode, 'updateTime'=>time(), 'total'=>$total,'message'=>$requestMsg,'ipSample'=> \@ipSample);
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
   if(defined $tmp){
     return $tmp;
   } else{
     return "";
   }
}

