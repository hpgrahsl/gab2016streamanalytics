package at.grahsl.gab2016.storm.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.jdbc.trident.state.JdbcState;
import org.apache.storm.jdbc.trident.state.JdbcStateFactory;

import backtype.storm.tuple.ITuple;

@SuppressWarnings("rawtypes")
public class MyJdbcUtils {
	
	private static class UpsertJdbcMapper extends SimpleJdbcMapper {

		private static final long serialVersionUID = -7249562084868857693L;

		public UpsertJdbcMapper(List<Column> schemaColumns) {
			super(schemaColumns);
		}

		@Override
		public List<Column> getColumns(ITuple tuple) {
			List<Column> cols = super.getColumns(tuple);
			
			//for MySQL UPSERT add the counter a second time
			//to enable the 3rd param to be bound for the stmt
			cols.add(new Column<Long>("counter", tuple.getLongByField("counter"),
									java.sql.Types.BIGINT));
			return cols;
		}
		
	}
	
	private static final ConnectionProvider CP;
	private static final JdbcMapper MAPPER;
	private static final JdbcStateFactory JDBC_FACTORY;
	
	//TODO: change the insert stmt to get the upsert semantic
	//according to your underlying (R)DBMS
	//example here works with MySQL
	public static final String UPSERT_STMT =
			"INSERT INTO emoji_freqs_storm (ecode,counter) "
			+ "VALUES (?,?) ON DUPLICATE KEY UPDATE counter=counter+?";
	
	static {
		
		Map<String,Object> jdbcConfig = new HashMap<>();
		
		//TODO: add your local/remote data source here settings here...
		jdbcConfig.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
		jdbcConfig.put("dataSource.url", "jdbc:mysql://localhost/YOUR_DATABASE");
		jdbcConfig.put("dataSource.user","YOUR_DB_USER");
		jdbcConfig.put("dataSource.password","YOUR_DB_PASSWORD");
				
		CP = new HikariCPConnectionProvider(jdbcConfig);
		
		List<Column> tableSchema = new ArrayList<>();
		tableSchema.add(new Column<String>("ecode", java.sql.Types.VARCHAR));
		tableSchema.add(new Column<Long>("counter", java.sql.Types.BIGINT));
		MAPPER = new UpsertJdbcMapper(tableSchema);	
		
		JdbcState.Options options = new JdbcState.Options()
		        .withConnectionProvider(CP)
		        .withMapper(MAPPER)
		        .withInsertQuery(UPSERT_STMT)
		        .withQueryTimeoutSecs(10);		  
		JDBC_FACTORY = new JdbcStateFactory(options);
	}
	
	public static ConnectionProvider getConnectionProvider() {
		return CP;
	}
	
	public static JdbcMapper getJdbcMapper() {
		return MAPPER;
	}
	
	public static JdbcStateFactory getJdbcFactory() {
		return JDBC_FACTORY;
	}
}
