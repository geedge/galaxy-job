package com.mesalab.executor.pojo;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;


@XmlRootElement(name = "HosSpace")
public class HosSpace {

    private Long hosCapacity;

    private Long hosUsed;

    @XmlElement(name = "HosCapacity")
    public Long getHosCapacity() {
        return hosCapacity;
    }

    public void setHosCapacity(Long hosCapacity) {
        this.hosCapacity = hosCapacity;
    }

    @XmlElement(name = "HosUsed")
    public Long getHosUsed() {
        return hosUsed;
    }

    public void setHosUsed(Long hosUsed) {
        this.hosUsed = hosUsed;
    }


}
