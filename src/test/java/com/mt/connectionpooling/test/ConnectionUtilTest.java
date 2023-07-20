package com.mt.connectionpooling.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;

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
		try (Connection connection = ConnectionUtil.getReadConnection();) {

			String sql = new StringBuilder().append("select * from users where id =?").toString();
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setInt(1, 1);
			
			ResultSet result = preparedStatement.executeQuery();
			while (result.next()) {
				String  name = result.getString("full_name");
				System.out.println("name:" + name);
			}

		} catch (Exception e) {
			log.error("ERROR:{}", e);
		}
	}

	@Test
	void getWriteConnectionTest() {
		try(Connection connection =  ConnectionUtil.getWriteConnection();) {
			
			String sql = new StringBuilder().append("insert into sample_emails(sender, email_subject, email_body,campaign_id, comments,status,type,created_at,updated_at)")
					.append(" values(?,?,?,?,?,?,?,?,?)" )
					.toString();
			
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			preparedStatement.setString(1, "nylas");
			preparedStatement.setString(2, "Hello");
			preparedStatement.setString(3, "java developer");
			preparedStatement.setString(4, "55");
			preparedStatement.setString(5, "");
			preparedStatement.setString(6, "1");
			preparedStatement.setString(7, "1");
			preparedStatement.setString(8, LocalDateTime.now().toString());
			preparedStatement.setString(9, LocalDateTime.now().toString());
			
			System.out.println("sql: "+sql);
			
			int id = preparedStatement.executeUpdate();
			System.out.println("id: " + id);
			
		}catch (SQLException e) {
			e.printStackTrace();
		}
		catch (Exception e) {
			log.error("ERROR:{}", e);
		}
	}
}
