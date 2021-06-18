package com.yangtzelsl.conf;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;


/**
 * @Description 配置文件加载类
 * @Author luis.liu
 * @Date 2020/10/12 10:06
 */
public class ConfigurationManagerJava {

    public static PropertiesConfiguration getPropConfig(String path) throws ConfigurationException {
        Configurations configs = new Configurations();
        // setDefaultEncoding是个静态方法,用于设置指定类型(class)所有对象的编码方式。
        // 本例中是PropertiesConfiguration,要在PropertiesConfiguration实例创建之前调用。
        FileBasedConfigurationBuilder.setDefaultEncoding(PropertiesConfiguration.class, "UTF-8");
        PropertiesConfiguration propConfig = configs.properties(ConfigurationManagerJava.class.getClassLoader().getResource(path));
        return propConfig;
    }


}
