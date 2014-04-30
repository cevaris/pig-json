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
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
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
	
	private String seeds = "127.0.0.1:9160";
	private String keyspace;
	private String columnFamily;
	private ColumnFamily<String, String> CF_TWEETS;
	
	
	private Keyspace ksClient;
	
	
	public CassandraClient(String keyspace, String columnFamily) {
		this.keyspace = keyspace;
		this.columnFamily = columnFamily;
		init();
	}
	
	public CassandraClient(String seeds, String keyspace, String columnFamily) {
		this.seeds = seeds;
		this.keyspace = keyspace;
		this.columnFamily = columnFamily;
		init();
	}
	
	private void init(){
		CF_TWEETS = ColumnFamily.newColumnFamily(
	    		this.columnFamily,
	    		StringSerializer.get(),
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
	        .setSeeds(this.seeds)
	     )
	    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
		.buildKeyspace(ThriftFamilyFactory.getInstance());
		ctx.start();
		ksClient = ctx.getClient();
	}
	
	public String selectOne(String rowKey, String name){
		
		Column<String> result;
		try {
			result = this.ksClient.prepareQuery(CF_TWEETS)
				    .getKey(rowKey)
				    .getColumn(name)
				    .execute().getResult();
			String value = result.getStringValue();
//			System.out.println(String.format("%s[%s][%s] = %s", this.columnFamily, rowKey, name, value));
			return value;
		} catch (NotFoundException e)  {
			System.err.println(String.format("Could not find %s %s", rowKey, name));
		} catch (ConnectionException e) {
			e.printStackTrace();
		} 
		return null;
	}

	public void selectAll(){
		
		
		try {
		    OperationResult<CqlResult<String, String>> result
		        = this.ksClient.prepareQuery(CF_TWEETS)
		            .withCql("SELECT * FROM "+this.columnFamily+";")
		            .execute();
		    for (Row<String, String> row : result.getResult().getRows()) {
		    	System.out.println(row.getKey());
		    	
		    	ColumnList<String> cnames = row.getColumns();
		    	for(String cname: cnames.getColumnNames()){
		    		System.out.println(cname);
		    	}
		    }
		} catch (ConnectionException e) {
			e.printStackTrace();
		} 
	}

}
