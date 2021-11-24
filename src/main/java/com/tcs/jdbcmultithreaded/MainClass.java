package com.tcs.jdbcmultithreaded;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.h2.jdbcx.JdbcDataSource;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MainClass {
	public static final int FIXED_THREADPOOL_SIZE = 100;

	public static void main(String[] args) throws Exception {
		JdbcDataSource dataSource = new JdbcDataSource();
		dataSource.setURL("jdbc:h2:mem:demo");
		Connection conn = dataSource.getConnection();
		PreparedStatement ps1 = conn.prepareStatement(
				"CREATE TABLE data ( \r\n" + "   id INT NOT NULL, \r\n" + "   data VARCHAR(1000) NOT NULL \r\n" + ")");
		ps1.execute();
		ps1.close();
		for (int i = 1; i < 10000; i++) {
			PreparedStatement ps2 = conn.prepareStatement("insert into data values(?,?)");
			ps2.setInt(1, i);
			ps2.setString(2, "{\"userId\":\"" + i + "\"}");
			ps2.execute();
			ps2.close();
		}

		/** Without thread **/
		List results1 = new ArrayList();
		PreparedStatement ps4 = conn.prepareStatement("select * from data");
		ResultSet rs = ps4.executeQuery();
		
		long ts1 = System.currentTimeMillis();
		while(rs.next()) {
			ObjectMapper mapper = new ObjectMapper();
			results1.add(mapper.readValue(rs.getString(2), LinkedHashMap.class));
		}
		ts1 = System.currentTimeMillis() - ts1;

		rs.close();
		ps4.close();
		

		/** With Thread **/
		List results2 = new ArrayList();
		PreparedStatement ps3 = conn.prepareStatement("select * from data");
		ResultSet rs2 = ps3.executeQuery();

		ExecutorService executor = Executors.newFixedThreadPool(FIXED_THREADPOOL_SIZE);
		List<Callable<Object>> callableTasks = new ArrayList<>();

		long ts2 = System.currentTimeMillis();
		while (rs2.next()) {
			final String jsonStr = rs2.getString(2);
			callableTasks.add(new Callable<Object>() {

				@Override
				public Object call() throws Exception {
					ObjectMapper mapper = new ObjectMapper();
					return mapper.readValue(jsonStr.getBytes(), LinkedHashMap.class);
				}
			});
		}

		List<Future<Object>> futures = executor.invokeAll(callableTasks);
		if (futures != null && futures.size() > 0) {
			for (Future f : futures) {
				results2.add(f.get());
			}
		}
		ts2 = System.currentTimeMillis() - ts2;
		
		System.out.println("ETA\nSingle Thread: "+ ts1+"ms, Multi Thread: " + ts2+ "ms");
		
		executor.shutdown();
		rs2.close();
		ps3.close();

	}
}
