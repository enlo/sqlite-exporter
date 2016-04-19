package info.naiv.lab.java.tool.sqlite.exporter.component;

import info.naiv.lab.java.jmt.jdbc.JdbcType;
import lombok.Data;
import static org.springframework.jdbc.support.JdbcUtils.convertUnderscoreNameToPropertyName;

/**
 *
 * @author enlo
 */
@Data
public class Field {

    private String name;
    private String originalName;
    private JdbcType typeInfo;
    private String originalTypeName;
    private boolean nonNull;
    private boolean primaryKey;
    private int primaryKeyIndex;

    public void makeNameFromOriginalName() {
        setName(convertUnderscoreNameToPropertyName(getOriginalName()));
    }

    public void setType(Class clazz) {
        typeInfo = JdbcType.valueOf(clazz);
    }
}
