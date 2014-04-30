package org.apache.cassandra.hadoop.pig.redis;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class CassandraClient {
	
	private String keyspace;
	private String columnFamily;
	
	public CassandraClient(String keyspace, String columnFamily) {
		this.keyspace = keyspace;
		this.columnFamily = columnFamily;
	}
	
	public void selectAll(){
		
		ColumnFamily<Integer, String> CF_STANDARD1 = ColumnFamily
	            .newColumnFamily(this.columnFamily,
	            		IntegerSerializer.get(),
	                    StringSerializer.get());
		
		AstyanaxContext<Keyspace> ctx = new AstyanaxContext.Builder()
			.forKeyspace(this.keyspace)
			.withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
		        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
		        .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE)
		    )
			.withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("CassandraClientPool")
		        .setPort(9160)
		        .setMaxConnsPerHost(3)
		        .setSeeds("127.0.0.1:9160")
		     )
		    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
			.buildKeyspace(ThriftFamilyFactory.getInstance());
		ctx.start();

		Keyspace keyspace = ctx.getClient();
		
		try {
		    OperationResult<CqlResult<Integer, String>> result
		        = keyspace.prepareQuery(CF_STANDARD1)
		            .withCql("SELECT * FROM "+this.columnFamily+";")
		            .execute();
		    for (Row<Integer, String> row : result.getResult().getRows()) {
		    	System.out.println(row.getColumns().getIntegerValue("id", null));
		    	System.out.println(row.getColumns().getStringValue("name", null));
		    }
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
	}

}
