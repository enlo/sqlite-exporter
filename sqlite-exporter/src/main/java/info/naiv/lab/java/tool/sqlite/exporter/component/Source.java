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
package info.naiv.lab.java.tool.sqlite.exporter.component;

import info.naiv.lab.java.jmt.Misc;
import static info.naiv.lab.java.jmt.Misc.isBlank;
import static info.naiv.lab.java.jmt.Misc.isEmpty;
import info.naiv.lab.java.jmt.fx.Predicate1;
import info.naiv.lab.java.jmt.io.NIOUtils;
import info.naiv.lab.java.jmt.jdbc.JdbcType;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.sql.DataSource;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.mvel2.templates.CompiledTemplate;
import org.mvel2.templates.TemplateCompiler;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.DatabaseMetaDataCallback;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.MetaDataAccessException;
import org.springframework.util.AntPathMatcher;
import static org.springframework.util.StringUtils.commaDelimitedListToSet;

/**
 *
 * @author enlo
 */
@Slf4j
public class Source {

    private static final AntPathMatcher matcher = new AntPathMatcher();

    private Resource classTemplate;
    private DataAccess dataAccess;
    private Set<String> excludeTables;
    @Getter
    private JdbcTemplate jdbcTemplate;
    private String schema;
    Properties props;

    /**
     *
     * @param props
     * @param dataAccess
     */
    public Source(Properties props, DataAccess dataAccess) {

        String url = props.getProperty("datasource.url");
        if (isBlank(url)) {
            throw new IllegalArgumentException("DataSource url is blank");
        }

        String userid = props.getProperty("datasource.username");
        String password = props.getProperty("datasource.password");
        String tables = props.getProperty("datasource.exclude.tables");
        this.excludeTables = commaDelimitedListToSet(tables);
        this.props = props;
        this.schema = props.getProperty("datasource.schema");
        if ("@null".equals(this.schema)) {
            this.schema = null;
        }
        this.classTemplate = new ClassPathResource("sqliteSchema.sql");
        this.dataAccess = dataAccess;
        this.jdbcTemplate = new JdbcTemplate(new DriverManagerDataSource(url, userid, password));
    }

    /**
     *
     * @param info
     * @return
     */
    public int countRows(TableInfo info) {
        return dataAccess.countRows(jdbcTemplate, info);
    }

    /**
     *
     * @return @throws IOException
     */
    public CompiledTemplate getClassTemplate() throws IOException {
        try (InputStream is = classTemplate.getInputStream()) {
            String templ = NIOUtils.toString(is, StandardCharsets.UTF_8);
            return TemplateCompiler.compileTemplate(templ);
        }
    }

    /**
     *
     * @return @throws MetaDataAccessException
     * @throws IOException
     */
    public List<TableInfo> getCreateSchemaSql() throws MetaDataAccessException, IOException {
        DataSource dataSource = jdbcTemplate.getDataSource();
        List<TableInfo> sqls = (List<TableInfo>) JdbcUtils.extractDatabaseMetaData(dataSource, new SchemaCreator());
        return sqls;
    }

    class SchemaCreator implements DatabaseMetaDataCallback {

        @Override
        public List<TableInfo> processMetaData(DatabaseMetaData dbmd) throws SQLException, MetaDataAccessException {
            List<TableInfo> result = new ArrayList<>();

            List<String> tableNames = getTableNames(dbmd);
            for (String tableName : tableNames) {
                if (isExcludeTarget(tableName)) {
                    logger.info("{} is exclude.", tableName);
                    continue;
                }
                List<Field> fields = makeFields(dbmd, tableName);
                Map<String, Object> param = new HashMap<>();
                param.put("tableName", tableName);
                param.put("fields", fields);
                TableInfo info = new TableInfo(tableName, fields);
                result.add(info);
            }
            return result;
        }

        protected List<String> getTableNames(DatabaseMetaData dbmd) throws SQLException {
            List<String> tableNames = new ArrayList<>();
            try (ResultSet rs = dbmd.getTables(null, schema, "%", new String[]{"TABLE"})) {
                while (rs.next()) {
                    tableNames.add(rs.getString("TABLE_NAME"));
                }
            }
            return tableNames;
        }

        protected List<String> getTableTypes(DatabaseMetaData dbmd) throws SQLException {
            List<String> tableTypes = new ArrayList<>();
            try (ResultSet rs = dbmd.getTableTypes()) {
                while (rs.next()) {
                    tableTypes.add(rs.getString("TABLE_TYPE"));
                }
            }
            return tableTypes;
        }

        protected JdbcType getType(final ResultSet rs) throws SQLException {
            int dataType = rs.getInt("DATA_TYPE");
            JdbcType type = JdbcType.valueOf(dataType);
            return type;
        }

        protected boolean isExcludeTarget(final String tableName) {
            if (isEmpty(excludeTables)) {
                return false;
            }
            return Misc.contains(excludeTables, new Predicate1<String>() {
                @Override
                public boolean test(String obj) {
                    return matcher.match(obj, tableName);
                }
            });
        }

        protected List<Field> makeFields(DatabaseMetaData meta, String tablename) throws SQLException {
            List<Field> fields = new ArrayList<>();
            try (ResultSet rs = meta.getColumns(null, schema, tablename, "%")) {
                while (rs.next()) {
                    Field field = new Field();
                    field.setOriginalName(rs.getString("COLUMN_NAME"));
                    field.setOriginalTypeName(rs.getString("TYPE_NAME"));
                    field.makeNameFromOriginalName();
                    JdbcType clz = getType(rs);
                    int nullable = rs.getInt("NULLABLE");
                    field.setNonNull(nullable == DatabaseMetaData.columnNoNulls);
                    field.setTypeInfo(clz);
                    fields.add(field);
                }
            }
            return fields;
        }
    }
}
