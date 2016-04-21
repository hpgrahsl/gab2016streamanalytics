package at.grahsl.gab2016.storm.basic;

import org.apache.storm.jdbc.bolt.JdbcInsertBolt;

import at.grahsl.gab2016.storm.common.MyJdbcUtils;

public class PersistenceBolt extends JdbcInsertBolt {
	
	private static final long serialVersionUID = 2177232564264928830L;
	
	public PersistenceBolt() {
		super(MyJdbcUtils.getConnectionProvider(),
				MyJdbcUtils.getJdbcMapper());
		withInsertQuery(MyJdbcUtils.UPSERT_STMT);
	}

}
