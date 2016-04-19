package info.naiv.lab.java.tool.sqlite.exporter.component;

import static info.naiv.lab.java.jmt.Misc.concatnate;
import info.naiv.lab.java.jmt.datetime.ClassicDateUtils;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

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
/**
 *
 * @author enlo
 */
@Slf4j
public class Destination {

    private final DataAccess dataAccess;
    @Getter
    private final JdbcTemplate jdbcTemplate;
    private final Properties props;

    /**
     *
     * @param props
     * @param dataAccess
     */
    public Destination(Properties props, DataAccess dataAccess) {

        String dir = props.getProperty("export.directory");
        String base = props.getProperty("export.basename");
        String ext = props.getProperty("export.extension");

        Calendar date = ClassicDateUtils.now();
        String ts = (new SimpleDateFormat("yyyyMMdd-HHmmss")).format(date.getTime());

        String p = concatnate(dir, base, "-", ts, ".", ext);
        String url = "jdbc:sqlite:" + p;
        this.props = props;
        logger.info("dest url = {}", url);
        DriverManagerDataSource dataSource = new DriverManagerDataSource(url);
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.dataAccess = dataAccess;
    }

    /**
     *
     * @param info
     */
    public void createSchema(TableInfo info) {
        logger.info("create schema := {}", info);
        dataAccess.createSchema(jdbcTemplate, info);
    }

}
