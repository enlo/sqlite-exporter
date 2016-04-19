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

import info.naiv.lab.java.jmt.jdbc.sql.Query;
import info.naiv.lab.java.jmt.jdbc.sql.template.SqlTemplate;
import info.naiv.lab.java.jmt.jdbc.sql.template.config.InjectSql;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

/**
 *
 * @author enlo
 */
@Component
@Getter
@Slf4j
public class DataAccess {

    static final int BATCH_SIZE = 1000;

    @InjectSql
    SqlTemplate countRows;
    @InjectSql
    SqlTemplate createSchema;

    @InjectSql
    SqlTemplate insertSql;
    @InjectSql
    SqlTemplate selectTables;

    /**
     *
     * @param sourceTempl
     * @param destTempl
     * @param info
     * @param valueHandler
     */
    public void copy(final JdbcTemplate sourceTempl, JdbcTemplate destTempl, TableInfo info, final ValueHandler valueHandler) {

        final Query sq = selectTables.merge(info);

        Query q = insertSql.merge(info);
        final List<Field> fields = info.getFields();
        q.execute(destTempl, new PreparedStatementCallback<Void>() {

            @Override
            public Void doInPreparedStatement(final PreparedStatement ps) throws SQLException, DataAccessException {
                ps.getConnection().setAutoCommit(false);
                try {
                    List<Integer> list = sq.query(sourceTempl, new RowMapper<Integer>() {

                        @Override
                        public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {
                            int col = 1;
                            for (Field field : fields) {
                                Object val = valueHandler.handleValue(field, rs);
                                ps.setObject(col, val);
                            }
                            ps.addBatch();
                            if (rowNum % BATCH_SIZE == 0) {
                                logger.info("execBatch: {}", rowNum);
                                ps.executeBatch();
                            }
                            return rowNum;
                        }
                    });

                    int total = list.size();
                    logger.info("total Batch: {}", total);
                    if (total % BATCH_SIZE != 0) {
                        ps.executeBatch();
                    }
                    ps.getConnection().commit();
                }
                catch (SQLException | RuntimeException e) {
                    ps.getConnection().rollback();
                    throw e;
                }
                return null;
            }
        });
    }

    /**
     *
     * @param templ
     * @param info
     * @return
     */
    public int countRows(JdbcTemplate templ, TableInfo info) {
        return countRows.merge(info).queryForObject(templ, Integer.class);
    }

    /**
     *
     * @param templ
     * @param info
     */
    public void createSchema(JdbcTemplate templ, TableInfo info) {
        createSchema.merge(info).update(templ);
    }

    /**
     *
     * @param templ
     * @param info
     * @return
     */
    public SqlRowSet rowSet(JdbcTemplate templ, TableInfo info) {
        return selectTables.merge(info).queryForRowSet(templ);
    }

}
