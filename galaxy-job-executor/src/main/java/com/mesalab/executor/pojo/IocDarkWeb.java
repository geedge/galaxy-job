package com.mesalab.executor.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IocDarkWeb {

    public String iocValue;
    public String iocType;
    public String nodeType;
    public String ipv6;
    public String isp;
    public String asn;
    public String operatingSystem;
    public String country;
    public String city;
    public String latitude;
    public String longtitude;
    public String torFingerprint;
    public Integer torDirPort;
    public Integer torOrPort;
    public String torFlags;
    public String torVersion;
    public String torNickname;
    public String torExitPolicy;
    public String i2pHash;
    public String i2pMethod;
    public String i2pVersion;
    public Boolean i2pIsFloodfill;
    public String i2pBandwidth;
    public Boolean i2pIsHidden;
    public Boolean i2pIsReachable;
    public String i2pPublishTime;
    public Integer mtproxyPort;
    public String mtproxySecret;
    public Integer obfs4Port;
    public String obfs4Fingerprint;
    public String obfs4Cert;
    public Integer obfs4IatMode;
    public Integer snowflakePort;
    public Long firstAppearTime;
    public Long lastAppearTime;
    public Long createTime;
    public Long updateTime;


}
