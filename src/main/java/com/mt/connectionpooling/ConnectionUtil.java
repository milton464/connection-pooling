package com.mt.connectionpooling;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

/**
 * @author MILTON
 */
@Slf4j
public class ConnectionUtil {

	private static Map<String, HikariDataSource> masterDataSource = new HashMap<>();

	private static Map<String, HikariDataSource> replicaDataSource = new HashMap<>();
	
	private static final String DB_NAME ="test";
	
	private static final int CONNECTION_TIMEOUT = 300000;
	
	private static final int MAX_CONNECTION = 50;
	
	private static final int IDLE_TIMEOUT = 120000;
	
	private static final int LEAK_DETECTION_THRESHOLD = 300000;
	
	private static final int MINIMUM_IDLE = 5;
	
	private static final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
	
	private static final String CACHE_PREP_STMTS = "cachePrepStmts";
	
	private static final String PREP_STMT_CACHE_SIZE = "prepStmtCacheSize";
	
	private static final String PREP_STMT_CACHE_SQL_LIMIT = "prepStmtCacheSqlLimit";
	
	private static int tenantId = 0;
	
	
	
	
	
	private static String buildDataSourceKey() {
		return new StringBuilder().append("datasource:").append(tenantId).toString();  
	}
	
	@SuppressWarnings("unused")
	public static Connection getReadConnection()throws SQLException{
		HikariDataSource hikariDataSource = replicaDataSource.get(buildDataSourceKey());

		System.out.println("getReadConnection:hikariDataSource: {}"+ hikariDataSource);
		
		synchronized (ConnectionUtil.class) {
			if (hikariDataSource == null) {

				buildDataSource();
				
				hikariDataSource = replicaDataSource.get(buildDataSourceKey());
			}
		}
		System.out.println("getReadConnection:hikariDataSource: "+hikariDataSource);
		return hikariDataSource == null ? null : hikariDataSource.getConnection();
	}
	
	@SuppressWarnings("unused")
	public static Connection getWriteConnection() throws SQLException {
		HikariDataSource hikariDataSource = masterDataSource.get(buildDataSourceKey());

		synchronized (ConnectionUtil.class) {
			if (hikariDataSource == null) {
				
				buildDataSource();
				
				hikariDataSource = masterDataSource.get(buildDataSourceKey());
			}
		}

		return hikariDataSource == null ? null : hikariDataSource.getConnection();
	}
	
	
	private static void buildDataSource() {

		String masterJdbcUrl = new StringBuilder().append("jdbc:mysql://localhost:3306/").append("pypepro").toString();
		
		if(StringUtils.isNotBlank(masterJdbcUrl)) {
			
			HikariConfig hikariConfig =  new HikariConfig();
			hikariConfig.setJdbcUrl(masterJdbcUrl);
			hikariConfig.setUsername("root");
			hikariConfig.setPassword("password");
			hikariConfig.setPoolName(DB_NAME+"_master_pool");
			
			hikariConfig.setConnectionTimeout(CONNECTION_TIMEOUT);
			hikariConfig.setMaximumPoolSize(MAX_CONNECTION);
			hikariConfig.setIdleTimeout(IDLE_TIMEOUT);
			hikariConfig.setLeakDetectionThreshold(LEAK_DETECTION_THRESHOLD);
			hikariConfig.setDriverClassName(DRIVER_CLASS_NAME);
			hikariConfig.setMinimumIdle(MINIMUM_IDLE);
			hikariConfig.addDataSourceProperty(CACHE_PREP_STMTS, "true");
			hikariConfig.addDataSourceProperty(PREP_STMT_CACHE_SIZE, "250");
			hikariConfig.addDataSourceProperty(PREP_STMT_CACHE_SQL_LIMIT, "2048");
			
			
			HikariDataSource hikariDataSource = new HikariDataSource(hikariConfig);
			
			masterDataSource.put(buildDataSourceKey(), hikariDataSource);
			System.out.println("masterDataSource: "+masterDataSource);
		}
		
		String replicaJdbcUrl = new StringBuilder().append("jdbc:mysql://localhost:3306/").append("pypepro").toString();
		if(StringUtils.isNotBlank(replicaJdbcUrl)) {
			HikariConfig hikariConfig =  new HikariConfig();
			hikariConfig.setJdbcUrl(replicaJdbcUrl);
			hikariConfig.setUsername("root");
			hikariConfig.setPassword("password");
			hikariConfig.setPoolName(DB_NAME+"_master_pool");
			
			hikariConfig.setConnectionTimeout(CONNECTION_TIMEOUT);
			hikariConfig.setMaximumPoolSize(MAX_CONNECTION);
			hikariConfig.setIdleTimeout(IDLE_TIMEOUT);
			hikariConfig.setLeakDetectionThreshold(LEAK_DETECTION_THRESHOLD);
			hikariConfig.setDriverClassName(DRIVER_CLASS_NAME);
			hikariConfig.setMinimumIdle(MINIMUM_IDLE);
			hikariConfig.addDataSourceProperty(CACHE_PREP_STMTS, "true");
			hikariConfig.addDataSourceProperty(PREP_STMT_CACHE_SIZE, "250");
			hikariConfig.addDataSourceProperty(PREP_STMT_CACHE_SQL_LIMIT, "2048");
			
			replicaDataSource.put(buildDataSourceKey(), new HikariDataSource(hikariConfig));
			System.out.println("replicaDataSource: "+replicaDataSource);
		}
		
	}
	
}
