package com.mesalab.executor.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * @description:
 * @author: zhq
 * @create: 2022-04-08
 **/
@NoArgsConstructor
@AllArgsConstructor
@Getter
public enum DataTypeEnum {

    FILE("file"),
    MARIADB("mariadb"),
    MYSQL("mysql");

    private String name;

    public static DataTypeEnum match(String name) {
        if (name != null) {
            for (DataTypeEnum item : DataTypeEnum.values()) {
                if (item.name().equals(name)) {
                    return item;
                }
            }
        }
        return null;
    }
}
