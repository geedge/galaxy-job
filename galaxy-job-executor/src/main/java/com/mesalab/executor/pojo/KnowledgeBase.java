package com.mesalab.executor.pojo;

import lombok.*;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class KnowledgeBase implements Serializable {

    private String kb_id;
    private String name;
    private String format = "mmdb";
    private String category;
    private String sha256;
    private Integer is_valid = 1;
    private Integer builtin;
    private Integer is_encryption = 0;
}