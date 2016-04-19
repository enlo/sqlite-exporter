package info.naiv.lab.java.tool.sqlite.exporter.component;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.support.MetaDataAccessException;
import org.springframework.stereotype.Component;

/**
 *
 * @author enlo
 */
@Slf4j
@Component
public class Exporter {

    @Autowired
    DataAccess dao;
    Destination dest;

    @Autowired
    @Qualifier("externalProperties")
    Properties externalProperties;

    Source source;

    /**
     *
     */
    public void export() {

        try {
            List<TableInfo> sqls = source.getCreateSchemaSql();
            for (TableInfo info : sqls) {
                dest.createSchema(info);
                int rowCount = source.countRows(info);
                logger.info("{} rows {}", info.getTableName(), rowCount);
                if (0 < rowCount) {
                    dao.copy(source.getJdbcTemplate(), dest.getJdbcTemplate(), info, new ValueHandlerImpl());
                }
            }
        }
        catch (MetaDataAccessException | IOException ex) {
            logger.error("sqlerror", ex);
        }

    }

    /**
     *
     */
    @PostConstruct
    public void init() {
        source = new Source(externalProperties, dao);
        dest = new Destination(externalProperties, dao);
    }

}
