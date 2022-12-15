use strict;      #use this all times
use warnings;    #this too - helps a lot!
use JSON;
use MaxMind::DB::Writer::Tree;
use Getopt::Long;
use DBI;
use utf8;
use LWP::UserAgent;
use POSIX qw(strftime);
binmode(STDIN,":encoding(gbk)");

###################数据库连接信息###################
my $url="http://192.168.44.3:8080/v1/global/geolocation/dict/detail?isBuiltin=0&VSYS=1";
my $token="aa2bdec5518ad131f71944b13ce5c298&1&";
my $help = 0;

my $outfileV6 = 'ip_v6_user_defined.mmdb';
my $outfileV4 = 'ip_v4_user_defined.mmdb';

my $mmdbLog = 'user-mmdb.log';
my $mmdbStatus = 'user-status.json';

print "The user-defined perl script is running!\n";

GetOptions("db=s" => \$outfileV6, 'help|?' => \$help, )
or &help();

GetOptions("db=s" => \$outfileV4, 'help|?' => \$help, )
or &help();

&help() if $help;
my %types = (
        AREA_CODE => 'utf8_string',
        ASN => 'utf8_string',
        ISP => 'utf8_string',
        LATITUDE => 'utf8_string',
        LONGITUDE => 'utf8_string',
        COUNTRY => 'utf8_string',
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
       record_size           => 24,
       database_type         => 'IPLOCATOR',
       languages             => ['en'],
       description           => { en => 'My database of IP data' },
       map_key_type_callback => sub { $types{ $_[0] } },
       remove_reserved_networks => 0,
);
# V4databse info
my $treeV4 = MaxMind::DB::Writer::Tree->new(
       ip_version            => 4,
       record_size           => 24,
       database_type         => 'IPLOCATOR',
       languages             => ['en'],
       description           => { en => 'My database of IP data' },
       map_key_type_callback => sub { $types{ $_[0] } },
       remove_reserved_networks => 0,
);

$treeV4->insert_range(
      "0.0.0.0",
      "0.0.0.0",
  {
      AREA_CODE => ' ',
      ASN       => ' ',
      ISP       => ' ',
      LATITUDE  => '00',
      LONGITUDE => '00',
      COUNTRY   => changeSplit('aa'),
      SUPER_ADMINISTRATIVE_AREA => changeSplit('aa'),
      ADMINISTRATIVE_AREA => changeSplit('aa'),
      SUB_ADMINISTRATIVE_AREA => changeSplit('aa'),
      LOCALITY => changeSplit('aa'),
      DEPENDENT_LOCALITY => changeSplit('aa'),
      DOUBLE_DEPENDENT_LOCALITY => changeSplit('aa'),
  },
);


$treeV6->insert_range(
      "0000:0000:0000:0000:0000:0000:0000:0000",
      "0000:0000:0000:0000:0000:0000:0000:0000",
  {
      AREA_CODE => ' ',
      ASN       => ' ',
      ISP       => ' ',
      LATITUDE  => '00',
      LONGITUDE => '00',
      COUNTRY   => changeSplit('aa'),
      SUPER_ADMINISTRATIVE_AREA => changeSplit('aa'),
      ADMINISTRATIVE_AREA => changeSplit('aa'),
      SUB_ADMINISTRATIVE_AREA => changeSplit('aa'),
      LOCALITY => changeSplit('aa'),
      DEPENDENT_LOCALITY => changeSplit('aa'),
      DOUBLE_DEPENDENT_LOCALITY => changeSplit('aa'),
  },
);
my $pageNo = 0;
my $pageSize = 20000;
my $totalv4 = 0;
my $totalv6 = 0;
my $total= 0;
my $requestMsg= '';
my $requestCode= 0;
my @ipv4Sample= ('0.0.0.0');
my @ipv6Sample= ('0000:0000:0000:0000:0000:0000:0000:0000');

my $ua = new LWP::UserAgent;
#提取API接口中的信息，放到一个数组中

while(1==1){
my $request = new HTTP::Request("GET", "$url&pageSize=$pageSize&pageNo=$pageNo");
$request->header('Authorization' => $token);
my $response = $ua->request($request);
my $json = new JSON;
my $obj = $json->decode($response->content);

$pageNo+=1;
#记录日志
my $gmtDatestring = strftime "%Y-%m-%d %H:%M:%S", gmtime;

$requestCode=$obj->{"code"};
$requestMsg=$obj->{"msg"};

open(MMDBLOG, "+>>$mmdbLog");
printf MMDBLOG "$gmtDatestring MMDB request pageNum: $pageNo, code: $requestCode, msg: $requestMsg\n";

my $list=$obj->{"data"}->{"list"};
$total=$obj->{"data"}->{"total"};
my $JSON =JSON->new->allow_nonref;
my $data = $JSON->encode($list);
my $arraydata=decode_json($data);

if(@$arraydata[0]->{'addrType'} == 4){
  push(@ipv4Sample, @$arraydata[0]->{'startIp'});

} elsif (@$arraydata[0]->{'addrType'} == 6){
  push(@ipv6Sample, @$arraydata[0]->{'startIp'});
};

foreach my $ref (@$arraydata) {

  if(($ref->{'addrType'}) == 4){
          $totalv4+=1;
          $treeV4->insert_range(
               $ref->{'startIp'},
               $ref->{'endIp'},
          {
              AREA_CODE => isNotBlank($ref->{'areacode'}),
              ASN       => isNotBlank($ref->{'asnumber'}),
              ISP       => isNotBlank($ref->{'isp'}),
              LATITUDE  => isNotBlank($ref->{'latitude'}),
              LONGITUDE => isNotBlank($ref->{'longitude'}),
              COUNTRY   => changeSplit(isNotBlank($ref->{'country'})),
              SUPER_ADMINISTRATIVE_AREA => changeSplit(isNotBlank($ref->{'superAdministrativeArea'})),
              ADMINISTRATIVE_AREA => changeSplit(isNotBlank($ref->{'administrativeArea'})),
              SUB_ADMINISTRATIVE_AREA => changeSplit(isNotBlank($ref->{'subAdministrativeArea'})),
              LOCALITY => changeSplit(isNotBlank($ref->{'locality'})),
              DEPENDENT_LOCALITY => changeSplit(isNotBlank($ref->{'dependentLocality'})),
              DOUBLE_DEPENDENT_LOCALITY => changeSplit(isNotBlank($ref->{'doubleDependentLocality'})),
           },
        );
  }elsif (($ref->{'addrType'}) == 6) {
        $totalv6+=1;
        $treeV6->insert_range(
            $ref->{'startIp'},
            $ref->{'endIp'},
            {
            AREA_CODE => isNotBlank($ref->{'areacode'}),
            ASN       => isNotBlank($ref->{'asnumber'}),
            ISP       => isNotBlank($ref->{'isp'}),
            LATITUDE  => isNotBlank($ref->{'latitude'}),
            LONGITUDE => isNotBlank($ref->{'longitude'}),
            COUNTRY   => changeSplit(isNotBlank($ref->{'country'})),
            SUPER_ADMINISTRATIVE_AREA => changeSplit(isNotBlank($ref->{'superAdministrativeArea'})),
            ADMINISTRATIVE_AREA => changeSplit(isNotBlank($ref->{'administrativeArea'})),
            SUB_ADMINISTRATIVE_AREA => changeSplit(isNotBlank($ref->{'subAdministrativeArea'})),
            LOCALITY => changeSplit(isNotBlank($ref->{'locality'})),
            DEPENDENT_LOCALITY => changeSplit(isNotBlank($ref->{'dependentLocality'})),
            DOUBLE_DEPENDENT_LOCALITY => changeSplit(isNotBlank($ref->{'doubleDependentLocality'})),
            },
        );
    }
  }

if(@$arraydata < $pageSize){
  print MMDBLOG "IPv4 total: $totalv4. IPv6 total: $totalv6\n";
  close( MMDBLOG );
        last;
   }
}

my $gmtDateLog = strftime "%Y-%m-%d %H:%M:%S", gmtime;
open(MMDBLOG, "+>>$mmdbLog");

open my $fhV6, '>:raw', $outfileV6;
$treeV6->write_tree($fhV6);
printf MMDBLOG "$gmtDateLog MMDB IPv6 Write Over \n";

$gmtDateLog = strftime "%Y-%m-%d %H:%M:%S", gmtime;

open my $fhV4, '>:raw', $outfileV4;
$treeV4->write_tree($fhV4);
printf MMDBLOG "$gmtDateLog MMDB IPv4 Write Over \n";

close($outfileV4);
close($outfileV6);
close( MMDBLOG );

#状态
my $totalv46 = $totalv4+$totalv6;
$requestMsg = "$requestMsg . Getting result total $totalv46, ipv4 total  $totalv4, ipv6 total $totalv6.";

print "The user-defined perl script is finished!\n";

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

  if(!$tmp) {
     return " "
  }else{
     return "$tmp"
  }
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
