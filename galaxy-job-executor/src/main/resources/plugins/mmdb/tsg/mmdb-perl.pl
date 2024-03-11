#!/usr/bin/perl
use strict;      #use this all times
use warnings;    #this too - helps a lot!
use JSON;
use MaxMind::DB::Writer::Tree;
use Getopt::Long;
use utf8;
use LWP::UserAgent;
use POSIX qw(strftime);
binmode(STDIN,":encoding(gbk)");


###################连接信息###################
my $serverHost="{{ cm_api }}";
my $token="{{ cm_api_token }}";

#自定义：0. 内置：1
my @args = @ARGV;
my $isBuiltin= @args?$args[0]:1;
my $url="http://$serverHost:8080/v1/policy/library/geoip?is_builtin=$isBuiltin&vsys_id=0";
my $fileName=$isBuiltin==0?'ip_user_defined':'ip_builtin';
my $outfile="$fileName.mmdb";
my $mmdbLog="$fileName.log";
my $mmdbStatus=$fileName.'_status.json';
print "The perl script is running, output to $outfile \n";

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

my $pageNo = 1;
my $pageSize = 2000;
my $total= 0;
my $requestMsg= '';
my $requestCode= 0;
my @ipSample= ();

my $ua = new LWP::UserAgent;
while(1==1){
my $request = new HTTP::Request("GET", "$url&page_size=$pageSize&page_no=$pageNo");
$request->header('Authorization' => $token);
my $response = $ua->request($request);
my $json = new JSON;
my $obj = $json->decode($response->content);

$requestCode=$obj->{"code"};
$requestMsg=$obj->{"msg"};
my $dbTotal=$obj->{"data"}->{"total"};

#记录日志
open(MMDBLOG, "+>>$mmdbLog");
my $gmtDatestring = strftime "%Y-%m-%d %H:%M:%S", gmtime;

my $list=$obj->{"data"}->{"geoip_list"};
my $JSON =JSON->new->allow_nonref;
my $data = $JSON->encode($list);
my $arraydata=decode_json($data);
foreach my $ref (@$arraydata) {
        $treeV6->insert_range(
            $ref->{'start_ip'},
            $ref->{'end_ip'},
            {
            AREA_CODE => isNotBlank($ref->{'areacode'}),
            ASN       => isNotBlank($ref->{'asnumber'}),
            ISP       => isNotBlank($ref->{'isp'}),
            LATITUDE  => isNotBlank($ref->{'latitude'}),
            LONGITUDE => isNotBlank($ref->{'longitude'}),
            COUNTRY   => changeSplit(isNotBlank($ref->{'country_region'})),
            SUPER_ADMINISTRATIVE_AREA => changeSplit(isNotBlank($ref->{'super_administrative_area'})),
            ADMINISTRATIVE_AREA => changeSplit(isNotBlank($ref->{'administrative_area'})),
            SUB_ADMINISTRATIVE_AREA => changeSplit(isNotBlank($ref->{'sub_administrative_area'})),
            LOCALITY => changeSplit(isNotBlank($ref->{'locality'})),
            DEPENDENT_LOCALITY => changeSplit(isNotBlank($ref->{'dependent_locality'})),
            DOUBLE_DEPENDENT_LOCALITY => changeSplit(isNotBlank($ref->{'double_dependent_locality'})),
            },
        );
        $total+=1;
}
if(@$arraydata){
  push(@ipSample, @$arraydata[0]->{'start_ip'});
}
my $percentage = $dbTotal==0?0 : int(($total/$dbTotal) * 100);
print "$gmtDatestring MMDB request $requestMsg pageNum: $pageNo, queried/total: $total/$dbTotal, progress: $percentage% .\n";
print MMDBLOG "$gmtDatestring MMDB request $requestMsg pageNum: $pageNo, queried/total: $total/$dbTotal, progress: $percentage% .\n";
$pageNo+=1;
if(@$arraydata < $pageSize){
    last;
   }
}

if($total>0){
    open my $fhV6, '>:raw', $outfile;
    $treeV6->write_tree($fhV6);
    close($outfile);
}

my %statusHash = ('status'=>$requestCode, 'updateTime'=>time(), 'total'=>$total, 'message'=>$requestMsg,'ipSample'=> \@ipSample);
my $statusJson = encode_json \%statusHash;
open my $statusFile, '>', $mmdbStatus;
print $statusFile $statusJson;

my $gmtDateLog = strftime "%Y-%m-%d %H:%M:%S", gmtime;
print MMDBLOG "$gmtDateLog MMDB IP Write Over. \n\n";
close(MMDBLOG);
print "The perl script is finished!\n";

sub changeSplit {
  my $n = scalar(@_);
  my $i = $_[0];
  $i =~ s/\\b/ /g;

  return "$i"
}

sub isNotBlank {
   my $tmp = $_[0];
   if($tmp){
     return $tmp;
   } else{
     return " ";
   }
}

