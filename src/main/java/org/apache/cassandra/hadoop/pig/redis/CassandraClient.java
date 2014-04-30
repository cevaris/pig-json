package org.apache.cassandra.hadoop.pig.redis;

import java.util.List;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.migration.avro.CfDef;
import org.apache.cassandra.db.migration.avro.ColumnDef;
import org.apache.cassandra.db.migration.avro.KsDef;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.ColumnDefinition;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.FieldMetadata;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.recipes.reader.AllRowsReader;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftCql2Factory;
import com.netflix.astyanax.thrift.ThriftCqlSchema;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;


public class CassandraClient {
	
	private String keyspace;
	private String columnFamily;
	private ColumnFamily<String, String> CF_TWEETS;
	
//	private ColumnFamily<String, String> CF_TWEETS = ColumnFamily
//            .newColumnFamily(
//            		this.columnFamily,
//            		StringSerializer.get(),
//                    StringSerializer.get());
	
	public CassandraClient(String keyspace, String columnFamily) {
		this.keyspace = keyspace;
		this.columnFamily = columnFamily;

		CF_TWEETS = ColumnFamily.newColumnFamily(
    		this.columnFamily,
    		StringSerializer.get(),
            StringSerializer.get());
	}
	
	public void selectOne(String rowKey, String name){
		
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
		
		Column<String> result;
		try {
			result = keyspace.prepareQuery(CF_TWEETS)
				    .getKey(rowKey)
				    .getColumn(name)
				    .execute().getResult();
			String value = result.getStringValue();
			System.out.println(String.format("%s[%s][%s] = %s", this.columnFamily, rowKey, name, value));
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
		
	}

	public void selectAll(){
		
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
		    OperationResult<CqlResult<String, String>> result
		        = keyspace.prepareQuery(CF_TWEETS)
		            .withCql("SELECT * FROM "+this.columnFamily+";")
		            .execute();
		    for (Row<String, String> row : result.getResult().getRows()) {
		    	System.out.println(row.getKey());
		    	
		    	ColumnList<String> cnames = row.getColumns();
		    	for(String cname: cnames.getColumnNames()){
		    		System.out.println(cname);
		    	}
		    	System.out.println("\n");
//		    	System.out.println(row.getColumns().getIntegerValue("id", null));
//		    	System.out.println(row.getColumns().getStringValue("name", null));
		    }
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
	}

}
