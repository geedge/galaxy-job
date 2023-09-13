package com.mesalab.executor.core.connector;

import java.util.stream.Stream;

public interface Source {
    Stream<String> fetchData();
}