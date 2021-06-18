package com.yangtzelsl.constant;

/**
 * @Description 常量类
 * @Author luis.liu
 * @Date 2020/12/23 18:30
 */
public class ConstantsConfigJava {

    /**
     * jdbc配置相关的常量
     */
    public static final String JDBC_DATASOURCE_SIZE_JAVA = "jdbc.datasource.size";
    public static final String JDBC_DRIVERNAME_JAVA = "jdbc.driverName";
    public static final String JDBC_URL_JAVA = "jdbc.url";
    public static final String JDBC_USER_JAVA = "jdbc.user";
    public static final String JDBC_PASSWORD_JAVA = "jdbc.password";

    /**
     * kafka 配置
     */
    public static final String SA_KAFKA_BROKERS_LIST = "sa_kafka.broker.list";
    public static final String SA_KAFKA_TOPICS = "sa_kafka.topics";
    public static final String SA_KAFKA_GROUP = "sa_kafka_group";

    /**
     * s3配置
     */
    public static final String S3_HUDI_TABLE_BASIC_PATH = "s3_hudi_table_basic_path";
    public static final String S3_HUDI_TABLE_NAME = "s3_hudi_table_name";

    /**
     * hudi配置
     */
    public static final String HOODIE_DATASOURCE_HIVE_SYNC_JDBC_URL = "hoodie.datasource.hive_sync.jdbcurl";

    /**
     * hudi 写 hive 配置
     */
    public static final String HIVE_DATABASE_OPT_KEY = "hive_database_opt_key";
    public static final String HIVE_TABLE_OPT_KEY = "hive_table_opt_key";

}
