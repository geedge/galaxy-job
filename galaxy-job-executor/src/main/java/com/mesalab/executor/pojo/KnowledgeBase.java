package com.mesalab.executor.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class KnowledgeBase implements Serializable {

    private String kb_id;
    private String name;
    private String format = "mmdb";
    private String category;
    private String sha256;
    private Integer is_valid = 1;
    private Integer builtin;
}