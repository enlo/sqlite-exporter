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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import org.springframework.util.StringUtils;

/**
 *
 * @author enlo
 */
public class ValueHandlerImpl implements ValueHandler {

    /**
     *
     * @param field
     * @param rowSet
     * @return
     * @throws SQLException
     */
    @Override
    public Object handleValue(Field field, ResultSet rowSet) throws SQLException {
        String name = field.getOriginalName();
        switch (field.getTypeInfo()) {
            case CHAR:
            case NCHAR:
                return StringUtils.trimTrailingWhitespace(rowSet.getString(name));
            case DATE:
                return Objects.toString(rowSet.getDate(name), null);
            case TIME:
                return Objects.toString(rowSet.getTime(name), null);
            case TIMESTAMP:
                return Objects.toString(rowSet.getTimestamp(name), null);
            default:
                return rowSet.getObject(name);
        }
    }

}
