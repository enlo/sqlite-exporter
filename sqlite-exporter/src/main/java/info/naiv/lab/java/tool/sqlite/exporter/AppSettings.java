/*
 * The MIT License
 *
 * Copyright 2016 enlo.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package info.naiv.lab.java.tool.sqlite.exporter;

import static info.naiv.lab.java.jmt.Misc.isNotBlank;
import info.naiv.lab.java.jmt.jdbc.driver.ExternalJdbcDriverSetFactoryBean;
import info.naiv.lab.java.jmt.jdbc.sql.template.config.SqlTemplateInjector;
import info.naiv.lab.java.jmt.jdbc.sql.template.mvel.ClassPathResourceMvelSqlTemplateLoader;
import info.naiv.lab.java.jmt.support.spring.ResolvablePropertiesFactoryBean;
import java.io.IOException;
import java.util.Properties;
import javax.annotation.PostConstruct;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

/**
 *
 * @author enlo
 */
@Slf4j
@Configuration
@ToString
public class AppSettings {

    @Value(value = "${jdbc.driver.directory}")
    String jdbcDriverDirectory;

    @Value("${external.properties}")
    String propertyLocation;

    @Value("${datasource.extract.tables}")
    String sourceExtractTables;
    @Value(value = "${datasource.password}")
    String sourcePassword;
    @Value("${datasource.schema}")
    String sourceSchema;
    @Value(value = "${datasource.url}")
    String sourceUrl;
    @Value("${datasource.username}")
    String sourceUsername;

    /**
     *
     * @return
     */
    @Bean
    public ClassPathResourceMvelSqlTemplateLoader classPathResourceMvelSqlTemplateLoader() {
        ClassPathResourceMvelSqlTemplateLoader bean = new ClassPathResourceMvelSqlTemplateLoader();
        return bean;
    }

    /**
     *
     * @param props
     * @return
     * @throws IOException
     */
    @Bean(name = "externalJdbcDriverSet")
    public ExternalJdbcDriverSetFactoryBean externalJdbcDriverSetFactoryBean(@Qualifier("externalProperties") Properties props) throws IOException {
        ExternalJdbcDriverSetFactoryBean bean = new ExternalJdbcDriverSetFactoryBean();
        bean.setJarDirectory(props.getProperty("jdbc.driver.directory"));
        return bean;
    }

    /**
     *
     */
    @PostConstruct
    public void postConstruct() {
        logger.info("appSettings := {}", this);
    }

    /**
     *
     * @return
     */
    @Bean(name = "externalProperties")
    public PropertiesFactoryBean propertiesFactoryBean() {

        Properties p = new Properties();
        setPropertyIfNotBlank(p, "datasource.url", sourceUrl);
        setPropertyIfNotBlank(p, "datasource.schema", sourceSchema);
        setPropertyIfNotBlank(p, "datasource.username", sourceUsername);
        setPropertyIfNotBlank(p, "datasource.password", sourcePassword);
        setPropertyIfNotBlank(p, "jdbc.driver.directory", jdbcDriverDirectory);

        PropertiesFactoryBean bean = new ResolvablePropertiesFactoryBean();
        bean.setIgnoreResourceNotFound(true);
        bean.setLocation(new FileSystemResource(propertyLocation));
        bean.setProperties(p);
        bean.setLocalOverride(true);
        return bean;
    }

    /**
     *
     * @param loader
     * @return
     */
    @Bean
    public SqlTemplateInjector sqlTemplateInjector(ClassPathResourceMvelSqlTemplateLoader loader) {
        SqlTemplateInjector bean = new SqlTemplateInjector();
        return bean;
    }

    private void setPropertyIfNotBlank(Properties p, String key, String value) {
        if (isNotBlank(value)) {
            p.setProperty(key, value);
        }
    }

}
