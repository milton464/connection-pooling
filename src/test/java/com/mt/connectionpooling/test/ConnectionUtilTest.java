package com.mt.connectionpooling.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import com.mt.connectionpooling.ConnectionUtil;

import lombok.extern.slf4j.Slf4j;

/**
 * @author MILTON
 */
@Slf4j
public class ConnectionUtilTest {

	@Test
	void getReadConnectionTest() {
		try(Connection connection = ConnectionUtil.getReadConnection();
				Statement statement = connection.createStatement();) {
			String sql =  new StringBuilder().append("select * from users limit 10").toString();
			ResultSet result =  statement.executeQuery(sql);
			while (result.next()) {
				int id = result.getInt("id");
				System.out.println("id: "+id);
			}
			
		} catch (Exception e) {
			log.error("ERROR:{}", e);
		}
	}
}
