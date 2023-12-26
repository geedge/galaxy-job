package com.mesalab.executor.pojo;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class IpLocateLibraryParam implements Serializable {

    private String command;
    private String path;
    private String knowledgeBaseServer;
    private List<TsgKnowledgeBase> knowledgeBaseList;



}
