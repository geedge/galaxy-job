package com.mesalab.executor.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class KnowledgeBase {

    private String id;
    private String name;
    private String format = "mmdb";
    private String type = "ip_location";
    private String sha256;
    private Integer isValid;
}