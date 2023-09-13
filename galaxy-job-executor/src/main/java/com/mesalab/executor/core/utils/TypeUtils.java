//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.mesalab.executor.core.utils;

import cn.hutool.json.JSONNull;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.annotation.JSONType;
import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.parser.JSONScanner;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.fastjson.util.FieldInfo;
import com.alibaba.fastjson.util.IOUtils;
import com.mesalab.executor.core.connector.JdbcDialect;
import com.mesalab.executor.core.connector.MysqlDialect;
import lombok.NonNull;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.AccessControlException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.fastjson.util.TypeUtils.cast;
import static com.alibaba.fastjson.util.TypeUtils.getSuperMethodAnnotation;

public class TypeUtils {
    public static boolean compatibleWithJavaBean = false;
    public static boolean compatibleWithFieldName = false;
    private static boolean setAccessibleEnable = true;
    private static boolean oracleTimestampMethodInited = false;
    private static Method oracleTimestampMethod;
    private static boolean oracleDateMethodInited = false;
    private static Method oracleDateMethod;
    private static boolean optionalClassInited = false;
    private static Class<?> optionalClass;
    private static boolean transientClassInited = false;
    private static Class<? extends Annotation> transientClass;
    private static ConcurrentMap<String, Class<?>> mappings;
    private static Class<?> pathClass;
    private static boolean pathClass_error;

    public TypeUtils() {
    }

    public static String castToString(Object value) {
        if (value == JSONNull.NULL) {
            return "";
        }
        return Optional.ofNullable(String.valueOf(value)).orElse("");
    }

    public static Byte castToByte(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            return ((Number) value).byteValue();
        } else if (value instanceof String) {
            String strVal = (String) value;
            return strVal.length() != 0 && !"null".equals(strVal) && !"NULL".equals(strVal) ? Byte.parseByte(strVal) : null;
        } else {
            throw new JSONException("can not cast to byte, value : " + value);
        }
    }

    public static Character castToChar(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Character) {
            return (Character) value;
        } else if (value instanceof String) {
            String strVal = (String) value;
            if (strVal.length() == 0) {
                return null;
            } else if (strVal.length() != 1) {
                throw new JSONException("can not cast to char, value : " + value);
            } else {
                return strVal.charAt(0);
            }
        } else {
            throw new JSONException("can not cast to char, value : " + value);
        }
    }

    public static Short castToShort(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            return ((Number) value).shortValue();
        } else if (value instanceof String) {
            String strVal = (String) value;
            return strVal.length() != 0 && !"null".equals(strVal) && !"NULL".equals(strVal) ? Short.parseShort(strVal) : null;
        } else {
            throw new JSONException("can not cast to short, value : " + value);
        }
    }

    public static BigDecimal castToBigDecimal(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        } else if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger) value);
        } else {
            String strVal = value.toString();
            return strVal.length() == 0 ? null : new BigDecimal(strVal);
        }
    }

    public static BigInteger castToBigInteger(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof BigInteger) {
            return (BigInteger) value;
        } else if (!(value instanceof Float) && !(value instanceof Double)) {
            String strVal = value.toString();
            return strVal.length() != 0 && !"null".equals(strVal) && !"NULL".equals(strVal) ? new BigInteger(strVal) : null;
        } else {
            return BigInteger.valueOf(((Number) value).longValue());
        }
    }

    public static Float castToFloat(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            return ((Number) value).floatValue();
        } else if (value instanceof String) {
            String strVal = value.toString();
            if (strVal.length() != 0 && !"null".equals(strVal) && !"NULL".equals(strVal)) {
                if (strVal.indexOf(44) != 0) {
                    strVal = strVal.replaceAll(",", "");
                }

                return Float.parseFloat(strVal);
            } else {
                return null;
            }
        } else {
            throw new JSONException("can not cast to float, value : " + value);
        }
    }

    public static Double castToDouble(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof String) {
            String strVal = value.toString();
            if (strVal.length() != 0 && !"null".equals(strVal) && !"NULL".equals(strVal)) {
                if (strVal.indexOf(44) != 0) {
                    strVal = strVal.replaceAll(",", "");
                }

                return Double.parseDouble(strVal);
            } else {
                return null;
            }
        } else {
            throw new JSONException("can not cast to double, value : " + value);
        }
    }

    public static Date castToDate(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Date) {
            return (Date) value;
        } else if (value instanceof Calendar) {
            return ((Calendar) value).getTime();
        } else {
            long longValue = -1L;
            if (value instanceof Number) {
                longValue = ((Number) value).longValue();
                return new Date(longValue);
            } else {
                if (value instanceof String) {
                    String strVal = (String) value;
                    JSONScanner dateLexer = new JSONScanner(strVal);

                    try {
                        if (dateLexer.scanISO8601DateIfMatch(false)) {
                            Calendar calendar = dateLexer.getCalendar();
                            Date var6 = calendar.getTime();
                            return var6;
                        }
                    } finally {
                        dateLexer.close();
                    }

                    String format;
                    if (strVal.startsWith("/Date(") && strVal.endsWith(")/")) {
                        format = strVal.substring(6, strVal.length() - 2);
                        strVal = format;
                    }

                    if (strVal.indexOf(45) != -1) {
                        if (strVal.length() == JSON.DEFFAULT_DATE_FORMAT.length()) {
                            format = JSON.DEFFAULT_DATE_FORMAT;
                        } else if (strVal.length() == 10) {
                            format = "yyyy-MM-dd";
                        } else if (strVal.length() == "yyyy-MM-dd HH:mm:ss".length()) {
                            format = "yyyy-MM-dd HH:mm:ss";
                        } else {
                            format = "yyyy-MM-dd HH:mm:ss.SSS";
                        }

                        SimpleDateFormat dateFormat = new SimpleDateFormat(format, JSON.defaultLocale);
                        dateFormat.setTimeZone(JSON.defaultTimeZone);

                        try {
                            return dateFormat.parse(strVal);
                        } catch (ParseException var34) {
                            throw new JSONException("can not cast to Date, value : " + strVal);
                        }
                    }

                    if (strVal.length() == 0) {
                        return null;
                    }

                    longValue = Long.parseLong(strVal);
                }

                if (longValue < 0L) {
                    Class<?> clazz = value.getClass();
                    Object result;
                    if ("oracle.sql.TIMESTAMP".equals(clazz.getName())) {
                        if (oracleTimestampMethod == null && !oracleTimestampMethodInited) {
                            try {
                                oracleTimestampMethod = clazz.getMethod("toJdbc");
                            } catch (NoSuchMethodException var36) {
                            } finally {
                                oracleTimestampMethodInited = true;
                            }
                        }

                        try {
                            result = oracleTimestampMethod.invoke(value);
                        } catch (Exception var35) {
                            throw new JSONException("can not cast oracle.sql.TIMESTAMP to Date", var35);
                        }

                        return (Date) result;
                    } else if ("oracle.sql.DATE".equals(clazz.getName())) {
                        if (oracleDateMethod == null && !oracleDateMethodInited) {
                            try {
                                oracleDateMethod = clazz.getMethod("toJdbc");
                            } catch (NoSuchMethodException var39) {
                            } finally {
                                oracleDateMethodInited = true;
                            }
                        }

                        try {
                            result = oracleDateMethod.invoke(value);
                        } catch (Exception var38) {
                            throw new JSONException("can not cast oracle.sql.DATE to Date", var38);
                        }

                        return (Date) result;
                    } else {
                        throw new JSONException("can not cast to Date, value : " + value);
                    }
                } else {
                    return new Date(longValue);
                }
            }
        }
    }

    public static java.sql.Date castToSqlDate(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof java.sql.Date) {
            return (java.sql.Date) value;
        } else if (value instanceof Date) {
            return new java.sql.Date(((Date) value).getTime());
        } else if (value instanceof Calendar) {
            return new java.sql.Date(((Calendar) value).getTimeInMillis());
        } else {
            long longValue = 0L;
            if (value instanceof Number) {
                longValue = ((Number) value).longValue();
            }

            if (value instanceof String) {
                String strVal = (String) value;
                if (strVal.length() == 0 || "null".equals(strVal) || "NULL".equals(strVal)) {
                    return null;
                }

                if (isNumber(strVal)) {
                    longValue = Long.parseLong(strVal);
                } else {
                    JSONScanner scanner = new JSONScanner(strVal);
                    if (!scanner.scanISO8601DateIfMatch(false)) {
                        throw new JSONException("can not cast to Timestamp, value : " + strVal);
                    }

                    longValue = scanner.getCalendar().getTime().getTime();
                }
            }

            if (longValue <= 0L) {
                throw new JSONException("can not cast to Date, value : " + value);
            } else {
                return new java.sql.Date(longValue);
            }
        }
    }

    public static Timestamp castToTimestamp(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Calendar) {
            return new Timestamp(((Calendar) value).getTimeInMillis());
        } else if (value instanceof Timestamp) {
            return (Timestamp) value;
        } else if (value instanceof Date) {
            return new Timestamp(((Date) value).getTime());
        } else {
            long longValue = 0L;
            if (value instanceof Number) {
                longValue = ((Number) value).longValue();
            }

            if (value instanceof String) {
                String strVal = (String) value;
                if (strVal.length() == 0 || "null".equals(strVal) || "NULL".equals(strVal)) {
                    return null;
                }

                if (isNumber(strVal)) {
                    longValue = Long.parseLong(strVal);
                } else {
                    JSONScanner scanner = new JSONScanner(strVal);
                    if (!scanner.scanISO8601DateIfMatch(false)) {
                        throw new JSONException("can not cast to Timestamp, value : " + strVal);
                    }

                    longValue = scanner.getCalendar().getTime().getTime();
                }
            }

            if (longValue <= 0L) {
                throw new JSONException("can not cast to Timestamp, value : " + value);
            } else {
                return new Timestamp(longValue);
            }
        }
    }

    public static boolean isNumber(String str) {
        for (int i = 0; i < str.length(); ++i) {
            char ch = str.charAt(i);
            if (ch != '+' && ch != '-') {
                if (ch < '0' || ch > '9') {
                    return false;
                }
            } else if (i != 0) {
                return false;
            }
        }

        return true;
    }

    public static Long castToLong(Object value) {
        if (value == JSONNull.NULL) {
            return null;
        }
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            return ((Number) value).longValue();
        } else {
            if (value instanceof String) {
                String strVal = (String) value;
                if (strVal.length() == 0 || "null".equals(strVal) || "NULL".equals(strVal)) {
                    return null;
                }

                if (strVal.indexOf(44) != 0) {
                    strVal = strVal.replaceAll(",", "");
                }

                try {
                    return Long.parseLong(strVal);
                } catch (NumberFormatException var4) {
                    JSONScanner dateParser = new JSONScanner(strVal);
                    Calendar calendar = null;
                    if (dateParser.scanISO8601DateIfMatch(false)) {
                        calendar = dateParser.getCalendar();
                    }

                    dateParser.close();
                    if (calendar != null) {
                        return calendar.getTimeInMillis();
                    }
                }
            }

            if (value instanceof Map) {
                Map map = (Map) value;
                if (map.size() == 2 && map.containsKey("andIncrement") && map.containsKey("andDecrement")) {
                    Iterator iter = map.values().iterator();
                    iter.next();
                    Object value2 = iter.next();
                    return castToLong(value2);
                }
            }

            throw new JSONException("can not cast to long, value : " + value);
        }
    }

    public static Integer castToInt(Object value) {
        if (value == JSONNull.NULL) {
            return null;
        }
        if (value == null) {
            return null;
        } else if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            String strVal = (String) value;
            if (strVal.length() != 0 && !"null".equals(strVal) && !"NULL".equals(strVal)) {
                if (strVal.indexOf(44) != 0) {
                    strVal = strVal.replaceAll(",", "");
                }

                return Integer.parseInt(strVal);
            } else {
                return null;
            }
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1 : 0;
        } else {
            if (value instanceof Map) {
                Map map = (Map) value;
                if (map.size() == 2 && map.containsKey("andIncrement") && map.containsKey("andDecrement")) {
                    Iterator iter = map.values().iterator();
                    iter.next();
                    Object value2 = iter.next();
                    return castToInt(value2);
                }
            }

            throw new JSONException("can not cast to int, value : " + value);
        }
    }

    public static byte[] castToBytes(Object value) {
        if (value instanceof byte[]) {
            return (byte[]) ((byte[]) value);
        } else if (value instanceof String) {
            return IOUtils.decodeBase64((String) value);
        } else {
            throw new JSONException("can not cast to int, value : " + value);
        }
    }

    public static Boolean castToBoolean(Object value) {
        if (value == JSONNull.NULL) {
            return null;
        }
        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof Number) {
            return ((Number) value).intValue() == 1;
        } else {
            if (value instanceof String) {
                String strVal = (String) value;
                if (strVal.length() == 0 || "null".equals(strVal) || "NULL".equals(strVal)) {
                    return null;
                }

                if ("true".equalsIgnoreCase(strVal) || "1".equals(strVal)) {
                    return Boolean.TRUE;
                }

                if ("false".equalsIgnoreCase(strVal) || "0".equals(strVal)) {
                    return Boolean.FALSE;
                }

                if ("Y".equalsIgnoreCase(strVal) || "T".equals(strVal)) {
                    return Boolean.TRUE;
                }

                if ("F".equalsIgnoreCase(strVal) || "N".equals(strVal)) {
                    return Boolean.FALSE;
                }
            }

            throw new JSONException("can not cast to boolean, value : " + value);
        }
    }

    public static <T> T castToJavaBean(Object obj, Class<T> clazz) {
        return cast(obj, clazz, ParserConfig.getGlobalInstance());
    }


    public static Locale toLocale(String strVal) {
        String[] items = strVal.split("_");
        if (items.length == 1) {
            return new Locale(items[0]);
        } else {
            return items.length == 2 ? new Locale(items[0], items[1]) : new Locale(items[0], items[1], items[2]);
        }
    }

    private static void addBaseClassMappings() {
        mappings.put("byte", Byte.TYPE);
        mappings.put("short", Short.TYPE);
        mappings.put("int", Integer.TYPE);
        mappings.put("long", Long.TYPE);
        mappings.put("float", Float.TYPE);
        mappings.put("double", Double.TYPE);
        mappings.put("boolean", Boolean.TYPE);
        mappings.put("char", Character.TYPE);
        mappings.put("[byte", byte[].class);
        mappings.put("[short", short[].class);
        mappings.put("[int", int[].class);
        mappings.put("[long", long[].class);
        mappings.put("[float", float[].class);
        mappings.put("[double", double[].class);
        mappings.put("[boolean", boolean[].class);
        mappings.put("[char", char[].class);
        mappings.put("[B", byte[].class);
        mappings.put("[S", short[].class);
        mappings.put("[I", int[].class);
        mappings.put("[J", long[].class);
        mappings.put("[F", float[].class);
        mappings.put("[D", double[].class);
        mappings.put("[C", char[].class);
        mappings.put("[Z", boolean[].class);
        Class<?>[] classes = new Class[]{Object.class, Cloneable.class, loadClass("java.lang.AutoCloseable"), Exception.class, RuntimeException.class, IllegalAccessError.class, IllegalAccessException.class, IllegalArgumentException.class, IllegalMonitorStateException.class, IllegalStateException.class, IllegalThreadStateException.class, IndexOutOfBoundsException.class, InstantiationError.class, InstantiationException.class, InternalError.class, InterruptedException.class, LinkageError.class, NegativeArraySizeException.class, NoClassDefFoundError.class, NoSuchFieldError.class, NoSuchFieldException.class, NoSuchMethodError.class, NoSuchMethodException.class, NullPointerException.class, NumberFormatException.class, OutOfMemoryError.class, SecurityException.class, StackOverflowError.class, StringIndexOutOfBoundsException.class, TypeNotPresentException.class, VerifyError.class, StackTraceElement.class, HashMap.class, Hashtable.class, TreeMap.class, IdentityHashMap.class, WeakHashMap.class, LinkedHashMap.class, HashSet.class, LinkedHashSet.class, TreeSet.class, TimeUnit.class, ConcurrentHashMap.class, loadClass("java.util.concurrent.ConcurrentSkipListMap"), loadClass("java.util.concurrent.ConcurrentSkipListSet"), AtomicInteger.class, AtomicLong.class, Collections.EMPTY_MAP.getClass(), BitSet.class, Calendar.class, Date.class, Locale.class, UUID.class, Time.class, java.sql.Date.class, Timestamp.class, SimpleDateFormat.class, JSONObject.class, loadClass("java.awt.Rectangle"), loadClass("java.awt.Point"), loadClass("java.awt.Font"), loadClass("java.awt.Color"), loadClass("org.springframework.remoting.support.RemoteInvocation"), loadClass("org.springframework.remoting.support.RemoteInvocationResult")};
        Class[] var1 = classes;
        int var2 = classes.length;

        for (int var3 = 0; var3 < var2; ++var3) {
            Class clazz = var1[var3];
            if (clazz != null) {
                mappings.put(clazz.getName(), clazz);
            }
        }

    }

    public static void clearClassMapping() {
        mappings.clear();
        addBaseClassMappings();
    }

    public static Class<?> loadClass(String className) {
        return loadClass(className, (ClassLoader) null);
    }

    public static boolean isPath(Class<?> clazz) {
        if (pathClass == null && !pathClass_error) {
            try {
                pathClass = Class.forName("java.nio.file.Path");
            } catch (Throwable var2) {
                pathClass_error = true;
            }
        }

        return pathClass != null ? pathClass.isAssignableFrom(clazz) : false;
    }

    public static Class<?> getClassFromMapping(String className) {
        return (Class) mappings.get(className);
    }

    public static Class<?> loadClass(String className, ClassLoader classLoader) {
        if (className != null && className.length() != 0) {
            Class<?> clazz = (Class) mappings.get(className);
            if (clazz != null) {
                return clazz;
            } else if (className.charAt(0) == '[') {
                Class<?> componentType = loadClass(className.substring(1), classLoader);
                return Array.newInstance(componentType, 0).getClass();
            } else if (className.startsWith("L") && className.endsWith(";")) {
                String newClassName = className.substring(1, className.length() - 1);
                return loadClass(newClassName, classLoader);
            } else {
                try {
                    if (classLoader != null) {
                        clazz = classLoader.loadClass(className);
                        mappings.put(className, clazz);
                        return clazz;
                    }
                } catch (Throwable var6) {
                    var6.printStackTrace();
                }

                try {
                    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
                    if (contextClassLoader != null && contextClassLoader != classLoader) {
                        clazz = contextClassLoader.loadClass(className);
                        mappings.put(className, clazz);
                        return clazz;
                    }
                } catch (Throwable var5) {
                }

                try {
                    clazz = Class.forName(className);
                    mappings.put(className, clazz);
                    return clazz;
                } catch (Throwable var4) {
                    return clazz;
                }
            }
        } else {
            return null;
        }
    }

    public static List<FieldInfo> computeGettersWithFieldBase(Class<?> clazz, Map<String, String> aliasMap, boolean sorted, PropertyNamingStrategy propertyNamingStrategy) {
        Map<String, FieldInfo> fieldInfoMap = new LinkedHashMap();

        for (Class currentClass = clazz; currentClass != null; currentClass = currentClass.getSuperclass()) {
            Field[] fields = currentClass.getDeclaredFields();
            computeFields(currentClass, aliasMap, propertyNamingStrategy, fieldInfoMap, fields);
        }

        return getFieldInfos(clazz, sorted, fieldInfoMap);
    }

    public static List<FieldInfo> computeGetters(Class<?> clazz, Map<String, String> aliasMap) {
        return computeGetters(clazz, aliasMap, true);
    }

    public static List<FieldInfo> computeGetters(Class<?> clazz, Map<String, String> aliasMap, boolean sorted) {
        JSONType jsonType = (JSONType) clazz.getAnnotation(JSONType.class);
        Map<String, Field> fieldCacheMap = new HashMap();
        ParserConfig.parserAllFieldToCache(clazz, fieldCacheMap);
        return computeGetters(clazz, jsonType, aliasMap, fieldCacheMap, sorted, PropertyNamingStrategy.CamelCase);
    }

    public static List<FieldInfo> computeGetters(Class<?> clazz, JSONType jsonType, Map<String, String> aliasMap, Map<String, Field> fieldCacheMap, boolean sorted, PropertyNamingStrategy propertyNamingStrategy) {
        Map<String, FieldInfo> fieldInfoMap = new LinkedHashMap();
        Method[] var7 = clazz.getMethods();
        int var8 = var7.length;

        for (int var9 = 0; var9 < var8; ++var9) {
            Method method = var7[var9];
            String methodName = method.getName();
            int ordinal = 0;
            int serialzeFeatures = 0;
            int parserFeatures = 0;
            String label = null;
            if (!Modifier.isStatic(method.getModifiers()) && !method.getReturnType().equals(Void.TYPE) && method.getParameterTypes().length == 0 && method.getReturnType() != ClassLoader.class && (!method.getName().equals("getMetaClass") || !method.getReturnType().getName().equals("groovy.lang.MetaClass"))) {
                JSONField annotation = (JSONField) method.getAnnotation(JSONField.class);
                if (annotation == null) {
                    annotation = getSuperMethodAnnotation(clazz, method);
                }

                if (annotation != null) {
                    if (!annotation.serialize()) {
                        continue;
                    }

                    ordinal = annotation.ordinal();
                    serialzeFeatures = SerializerFeature.of(annotation.serialzeFeatures());
                    parserFeatures = Feature.of(annotation.parseFeatures());
                    if (annotation.name().length() != 0) {
                        String propertyName = annotation.name();
                        if (aliasMap != null) {
                            propertyName = (String) aliasMap.get(propertyName);
                            if (propertyName == null) {
                                continue;
                            }
                        }

                        FieldInfo fieldInfo = new FieldInfo(propertyName, method, (Field) null, clazz, (Type) null, ordinal, serialzeFeatures, parserFeatures, annotation, (JSONField) null, label);
                        fieldInfoMap.put(propertyName, fieldInfo);
                        continue;
                    }

                    if (annotation.label().length() != 0) {
                        label = annotation.label();
                    }
                }

                char c2;
                String propertyName;
                if (methodName.startsWith("get")) {
                    if (methodName.length() < 4 || methodName.equals("getClass") || methodName.equals("getDeclaringClass") && clazz.isEnum()) {
                        continue;
                    }

                    c2 = methodName.charAt(3);
                    if (!Character.isUpperCase(c2) && c2 <= 512) {
                        if (c2 == '_') {
                            propertyName = methodName.substring(4);
                        } else if (c2 == 'f') {
                            propertyName = methodName.substring(3);
                        } else {
                            if (methodName.length() < 5 || !Character.isUpperCase(methodName.charAt(4))) {
                                continue;
                            }

                            propertyName = decapitalize(methodName.substring(3));
                        }
                    } else {
                        if (compatibleWithJavaBean) {
                            propertyName = decapitalize(methodName.substring(3));
                        } else {
                            propertyName = Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4);
                        }

                        propertyName = getPropertyNameByCompatibleFieldName(fieldCacheMap, methodName, propertyName, 3);
                    }

                    boolean ignore = isJSONTypeIgnore(clazz, propertyName);
                    if (ignore) {
                        continue;
                    }

                    Field field = ParserConfig.getFieldFromCache(propertyName, fieldCacheMap);
                    if (field == null && propertyName.length() > 1) {
                        char ch = propertyName.charAt(1);
                        if (ch >= 'A' && ch <= 'Z') {
                            String javaBeanCompatiblePropertyName = decapitalize(methodName.substring(3));
                            field = ParserConfig.getFieldFromCache(javaBeanCompatiblePropertyName, fieldCacheMap);
                        }
                    }

                    JSONField fieldAnnotation = null;
                    if (field != null) {
                        fieldAnnotation = (JSONField) field.getAnnotation(JSONField.class);
                        if (fieldAnnotation != null) {
                            if (!fieldAnnotation.serialize()) {
                                continue;
                            }

                            ordinal = fieldAnnotation.ordinal();
                            serialzeFeatures = SerializerFeature.of(fieldAnnotation.serialzeFeatures());
                            parserFeatures = Feature.of(fieldAnnotation.parseFeatures());
                            if (fieldAnnotation.name().length() != 0) {
                                propertyName = fieldAnnotation.name();
                                if (aliasMap != null) {
                                    propertyName = (String) aliasMap.get(propertyName);
                                    if (propertyName == null) {
                                        continue;
                                    }
                                }
                            }

                            if (fieldAnnotation.label().length() != 0) {
                                label = fieldAnnotation.label();
                            }
                        }
                    }

                    if (aliasMap != null) {
                        propertyName = (String) aliasMap.get(propertyName);
                        if (propertyName == null) {
                            continue;
                        }
                    }

                    if (propertyNamingStrategy != null) {
                        propertyName = propertyNamingStrategy.translate(propertyName);
                    }

                    FieldInfo fieldInfo = new FieldInfo(propertyName, method, field, clazz, (Type) null, ordinal, serialzeFeatures, parserFeatures, annotation, fieldAnnotation, label);
                    fieldInfoMap.put(propertyName, fieldInfo);
                }

                if (methodName.startsWith("is") && methodName.length() >= 3 && (method.getReturnType() == Boolean.TYPE || method.getReturnType() == Boolean.class)) {
                    c2 = methodName.charAt(2);
                    if (Character.isUpperCase(c2)) {
                        if (compatibleWithJavaBean) {
                            propertyName = decapitalize(methodName.substring(2));
                        } else {
                            propertyName = Character.toLowerCase(methodName.charAt(2)) + methodName.substring(3);
                        }

                        propertyName = getPropertyNameByCompatibleFieldName(fieldCacheMap, methodName, propertyName, 2);
                    } else if (c2 == '_') {
                        propertyName = methodName.substring(3);
                    } else {
                        if (c2 != 'f') {
                            continue;
                        }

                        propertyName = methodName.substring(2);
                    }

                    Field field = ParserConfig.getFieldFromCache(propertyName, fieldCacheMap);
                    if (field == null) {
                        field = ParserConfig.getFieldFromCache(methodName, fieldCacheMap);
                    }

                    JSONField fieldAnnotation = null;
                    if (field != null) {
                        fieldAnnotation = (JSONField) field.getAnnotation(JSONField.class);
                        if (fieldAnnotation != null) {
                            if (!fieldAnnotation.serialize()) {
                                continue;
                            }

                            ordinal = fieldAnnotation.ordinal();
                            serialzeFeatures = SerializerFeature.of(fieldAnnotation.serialzeFeatures());
                            parserFeatures = Feature.of(fieldAnnotation.parseFeatures());
                            if (fieldAnnotation.name().length() != 0) {
                                propertyName = fieldAnnotation.name();
                                if (aliasMap != null) {
                                    propertyName = (String) aliasMap.get(propertyName);
                                    if (propertyName == null) {
                                        continue;
                                    }
                                }
                            }

                            if (fieldAnnotation.label().length() != 0) {
                                label = fieldAnnotation.label();
                            }
                        }
                    }

                    if (aliasMap != null) {
                        propertyName = (String) aliasMap.get(propertyName);
                        if (propertyName == null) {
                            continue;
                        }
                    }

                    if (propertyNamingStrategy != null) {
                        propertyName = propertyNamingStrategy.translate(propertyName);
                    }

                    if (!fieldInfoMap.containsKey(propertyName)) {
                        FieldInfo fieldInfo = new FieldInfo(propertyName, method, field, clazz, (Type) null, ordinal, serialzeFeatures, parserFeatures, annotation, fieldAnnotation, label);
                        fieldInfoMap.put(propertyName, fieldInfo);
                    }
                }
            }
        }

        Field[] fields = clazz.getFields();
        computeFields(clazz, aliasMap, propertyNamingStrategy, fieldInfoMap, fields);
        return getFieldInfos(clazz, sorted, fieldInfoMap);
    }

    private static List<FieldInfo> getFieldInfos(Class<?> clazz, boolean sorted, Map<String, FieldInfo> fieldInfoMap) {
        List<FieldInfo> fieldInfoList = new ArrayList();
        boolean containsAll = false;
        String[] orders = null;
        JSONType annotation = (JSONType) clazz.getAnnotation(JSONType.class);
        String[] var7;
        int var8;
        int var9;
        String item;
        if (annotation != null) {
            orders = annotation.orders();
            if (orders != null && orders.length == fieldInfoMap.size()) {
                containsAll = true;
                var7 = orders;
                var8 = orders.length;

                for (var9 = 0; var9 < var8; ++var9) {
                    item = var7[var9];
                    if (!fieldInfoMap.containsKey(item)) {
                        containsAll = false;
                        break;
                    }
                }
            } else {
                containsAll = false;
            }
        }

        if (containsAll) {
            var7 = orders;
            var8 = orders.length;

            for (var9 = 0; var9 < var8; ++var9) {
                item = var7[var9];
                FieldInfo fieldInfo = (FieldInfo) fieldInfoMap.get(item);
                fieldInfoList.add(fieldInfo);
            }
        } else {
            Iterator var12 = fieldInfoMap.values().iterator();

            while (var12.hasNext()) {
                FieldInfo fieldInfo = (FieldInfo) var12.next();
                fieldInfoList.add(fieldInfo);
            }

            if (sorted) {
                Collections.sort(fieldInfoList);
            }
        }

        return fieldInfoList;
    }

    private static void computeFields(Class<?> clazz, Map<String, String> aliasMap, PropertyNamingStrategy propertyNamingStrategy, Map<String, FieldInfo> fieldInfoMap, Field[] fields) {
        Field[] var5 = fields;
        int var6 = fields.length;

        for (int var7 = 0; var7 < var6; ++var7) {
            Field field = var5[var7];
            if (!Modifier.isStatic(field.getModifiers())) {
                JSONField fieldAnnotation = (JSONField) field.getAnnotation(JSONField.class);
                int ordinal = 0;
                int serialzeFeatures = 0;
                int parserFeatures = 0;
                String propertyName = field.getName();
                String label = null;
                if (fieldAnnotation != null) {
                    if (!fieldAnnotation.serialize()) {
                        continue;
                    }

                    ordinal = fieldAnnotation.ordinal();
                    serialzeFeatures = SerializerFeature.of(fieldAnnotation.serialzeFeatures());
                    parserFeatures = Feature.of(fieldAnnotation.parseFeatures());
                    if (fieldAnnotation.name().length() != 0) {
                        propertyName = fieldAnnotation.name();
                    }

                    if (fieldAnnotation.label().length() != 0) {
                        label = fieldAnnotation.label();
                    }
                }

                if (aliasMap != null) {
                    propertyName = (String) aliasMap.get(propertyName);
                    if (propertyName == null) {
                        continue;
                    }
                }

                if (propertyNamingStrategy != null) {
                    propertyName = propertyNamingStrategy.translate(propertyName);
                }

                if (!fieldInfoMap.containsKey(propertyName)) {
                    FieldInfo fieldInfo = new FieldInfo(propertyName, (Method) null, field, clazz, (Type) null, ordinal, serialzeFeatures, parserFeatures, (JSONField) null, fieldAnnotation, label);
                    fieldInfoMap.put(propertyName, fieldInfo);
                }
            }
        }

    }

    private static String getPropertyNameByCompatibleFieldName(Map<String, Field> fieldCacheMap, String methodName, String propertyName, int fromIdx) {
        if (compatibleWithFieldName && !fieldCacheMap.containsKey(propertyName)) {
            String tempPropertyName = methodName.substring(fromIdx);
            return fieldCacheMap.containsKey(tempPropertyName) ? tempPropertyName : propertyName;
        } else {
            return propertyName;
        }
    }

    private static boolean isJSONTypeIgnore(Class<?> clazz, String propertyName) {
        JSONType jsonType = (JSONType) clazz.getAnnotation(JSONType.class);
        if (jsonType != null) {
            String[] fields = jsonType.includes();
            int i;
            if (fields.length > 0) {
                for (i = 0; i < fields.length; ++i) {
                    if (propertyName.equals(fields[i])) {
                        return false;
                    }
                }

                return true;
            }

            fields = jsonType.ignores();

            for (i = 0; i < fields.length; ++i) {
                if (propertyName.equals(fields[i])) {
                    return true;
                }
            }
        }

        return clazz.getSuperclass() != Object.class && clazz.getSuperclass() != null && isJSONTypeIgnore(clazz.getSuperclass(), propertyName);
    }

    public static boolean isGenericParamType(Type type) {
        if (type instanceof ParameterizedType) {
            return true;
        } else if (type instanceof Class) {
            Type superType = ((Class) type).getGenericSuperclass();
            return superType == Object.class ? false : isGenericParamType(superType);
        } else {
            return false;
        }
    }

    public static Type getGenericParamType(Type type) {
        if (type instanceof ParameterizedType) {
            return type;
        } else {
            return type instanceof Class ? getGenericParamType(((Class) type).getGenericSuperclass()) : type;
        }
    }

    public static Type unwrapOptional(Type type) {
        if (!optionalClassInited) {
            try {
                optionalClass = Class.forName("java.util.Optional");
            } catch (Exception var5) {
            } finally {
                optionalClassInited = true;
            }
        }

        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            if (parameterizedType.getRawType() == optionalClass) {
                return parameterizedType.getActualTypeArguments()[0];
            }
        }

        return type;
    }

    public static Class<?> getClass(Type type) {
        if (type.getClass() == Class.class) {
            return (Class) type;
        } else if (type instanceof ParameterizedType) {
            return getClass(((ParameterizedType) type).getRawType());
        } else if (type instanceof TypeVariable) {
            Type boundType = ((TypeVariable) type).getBounds()[0];
            return (Class) boundType;
        } else {
            return Object.class;
        }
    }

    public static Field getField(Class<?> clazz, String fieldName, Field[] declaredFields) {
        Field[] var3 = declaredFields;
        int var4 = declaredFields.length;

        for (int var5 = 0; var5 < var4; ++var5) {
            Field field = var3[var5];
            if (fieldName.equals(field.getName())) {
                return field;
            }
        }

        Class<?> superClass = clazz.getSuperclass();
        if (superClass != null && superClass != Object.class) {
            return getField(superClass, fieldName, superClass.getDeclaredFields());
        } else {
            return null;
        }
    }

    public static int getSerializeFeatures(Class<?> clazz) {
        JSONType annotation = (JSONType) clazz.getAnnotation(JSONType.class);
        return annotation == null ? 0 : SerializerFeature.of(annotation.serialzeFeatures());
    }

    public static int getParserFeatures(Class<?> clazz) {
        JSONType annotation = (JSONType) clazz.getAnnotation(JSONType.class);
        return annotation == null ? 0 : Feature.of(annotation.parseFeatures());
    }

    public static String decapitalize(String name) {
        if (name != null && name.length() != 0) {
            if (name.length() > 1 && Character.isUpperCase(name.charAt(1)) && Character.isUpperCase(name.charAt(0))) {
                return name;
            } else {
                char[] chars = name.toCharArray();
                chars[0] = Character.toLowerCase(chars[0]);
                return new String(chars);
            }
        } else {
            return name;
        }
    }

    static void setAccessible(AccessibleObject obj) {
        if (setAccessibleEnable) {
            if (!obj.isAccessible()) {
                try {
                    obj.setAccessible(true);
                } catch (AccessControlException var2) {
                    setAccessibleEnable = false;
                }

            }
        }
    }

    public static Type getCollectionItemType(Type fieldType) {
        Type itemType = null;
        Class<?> clazz = null;
        Type actualTypeArgument;
        if (fieldType instanceof ParameterizedType) {
            actualTypeArgument = ((ParameterizedType) fieldType).getActualTypeArguments()[0];
            if (actualTypeArgument instanceof WildcardType) {
                WildcardType wildcardType = (WildcardType) actualTypeArgument;
                Type[] upperBounds = wildcardType.getUpperBounds();
                if (upperBounds.length == 1) {
                    actualTypeArgument = upperBounds[0];
                }
            }

            itemType = actualTypeArgument;
        } else if (fieldType instanceof Class && !(clazz = (Class) fieldType).getName().startsWith("java.")) {
            actualTypeArgument = clazz.getGenericSuperclass();
            itemType = getCollectionItemType(actualTypeArgument);
        }

        if (itemType == null) {
            itemType = Object.class;
        }

        return (Type) itemType;
    }

    public static Class<?> getCollectionItemClass(Type fieldType) {
        if (fieldType instanceof ParameterizedType) {
            Type actualTypeArgument = ((ParameterizedType) fieldType).getActualTypeArguments()[0];
            if (actualTypeArgument instanceof WildcardType) {
                WildcardType wildcardType = (WildcardType) actualTypeArgument;
                Type[] upperBounds = wildcardType.getUpperBounds();
                if (upperBounds.length == 1) {
                    actualTypeArgument = upperBounds[0];
                }
            }

            if (actualTypeArgument instanceof Class) {
                Class<?> itemClass = (Class) actualTypeArgument;
                if (!Modifier.isPublic(itemClass.getModifiers())) {
                    throw new JSONException("can not create ASMParser");
                } else {
                    return itemClass;
                }
            } else {
                throw new JSONException("can not create ASMParser");
            }
        } else {
            return Object.class;
        }
    }

    public static Collection createCollection(Type type) {
        Class<?> rawClass = getRawClass(type);
        Object list;
        if (rawClass != AbstractCollection.class && rawClass != Collection.class) {
            if (rawClass.isAssignableFrom(HashSet.class)) {
                list = new HashSet();
            } else if (rawClass.isAssignableFrom(LinkedHashSet.class)) {
                list = new LinkedHashSet();
            } else if (rawClass.isAssignableFrom(TreeSet.class)) {
                list = new TreeSet();
            } else if (rawClass.isAssignableFrom(ArrayList.class)) {
                list = new ArrayList();
            } else if (rawClass.isAssignableFrom(EnumSet.class)) {
                Object itemType;
                if (type instanceof ParameterizedType) {
                    itemType = ((ParameterizedType) type).getActualTypeArguments()[0];
                } else {
                    itemType = Object.class;
                }

                list = EnumSet.noneOf((Class) itemType);
            } else {
                try {
                    list = (Collection) rawClass.newInstance();
                } catch (Exception var4) {
                    throw new JSONException("create instance error, class " + rawClass.getName());
                }
            }
        } else {
            list = new ArrayList();
        }

        return (Collection) list;
    }

    public static Class<?> getRawClass(Type type) {
        if (type instanceof Class) {
            return (Class) type;
        } else if (type instanceof ParameterizedType) {
            return getRawClass(((ParameterizedType) type).getRawType());
        } else {
            throw new JSONException("TODO");
        }
    }

    public static boolean isProxy(Class<?> clazz) {
        Class[] var1 = clazz.getInterfaces();
        int var2 = var1.length;
        int var3 = 0;

        while (var3 < var2) {
            Class<?> item = var1[var3];
            String interfaceName = item.getName();
            if (!interfaceName.equals("net.sf.cglib.proxy.Factory") && !interfaceName.equals("org.springframework.cglib.proxy.Factory")) {
                if (!interfaceName.equals("javassist.util.proxy.ProxyObject") && !interfaceName.equals("org.apache.ibatis.javassist.util.proxy.ProxyObject")) {
                    ++var3;
                    continue;
                }

                return true;
            }

            return true;
        }

        return false;
    }


    static {
        try {
            compatibleWithJavaBean = "true".equals(IOUtils.getStringProperty("fastjson.compatibleWithJavaBean"));
            compatibleWithFieldName = "true".equals(IOUtils.getStringProperty("fastjson.compatibleWithFieldName"));
        } catch (Throwable var1) {
        }

        mappings = new ConcurrentHashMap(16, 0.75F, 1);
        addBaseClassMappings();
        pathClass_error = false;
    }
}
