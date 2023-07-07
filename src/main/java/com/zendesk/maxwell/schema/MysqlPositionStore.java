package com.zendesk.maxwell.schema;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


import com.zendesk.maxwell.MaxwellConfig;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.errors.DuplicateProcessException;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.util.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MysqlPositionStore {
	static final Logger LOGGER = LoggerFactory.getLogger(MysqlPositionStore.class);
	private static final Long DEFAULT_GTID_SERVER_ID = new Long(0);
	private final Long serverID;
	private String clientID;
	private final boolean gtidMode;
	private final ConnectionPool connectionPool;

	public MysqlPositionStore(ConnectionPool pool, Long serverID, String clientID, boolean gtidMode) {
		this.connectionPool = pool;
		this.clientID = clientID;
		this.gtidMode = gtidMode;
		if (gtidMode) {
			// we don't use server id for position store in gtid mode
			this.serverID = DEFAULT_GTID_SERVER_ID;
		} else {
			this.serverID = serverID;
		}
	}

	public void set(Position newPosition) throws SQLException, DuplicateProcessException {
		if ( newPosition == null )
			return;

		Long heartbeat = newPosition.getLastHeartbeatRead();

		String sql = "INSERT INTO `positions` set "
				+ "server_id = ?, "
				+ "gtid_set = ?, "
				+ "binlog_file = ?, "
				+ "binlog_position = ?, "
				+ "last_heartbeat_read = ?, "
				+ "client_id = ? "
				+ "ON DUPLICATE KEY UPDATE "
				+ "last_heartbeat_read = ?, "
				+ "gtid_set = ?, binlog_file = ?, binlog_position=?";

		BinlogPosition binlogPosition = newPosition.getBinlogPosition();
		connectionPool.withSQLRetry(1, (c) -> {
			try ( PreparedStatement s = c.prepareStatement(sql) ) {
				LOGGER.debug("Writing binlog position to {}.positions: {}, last heartbeat read: {}",
						c.getCatalog(), newPosition, heartbeat);
				s.setLong(1, serverID);
				s.setString(2, binlogPosition.getGtidSetStr());
				s.setString(3, binlogPosition.getFile());
				s.setLong(4, binlogPosition.getOffset());
				s.setLong(5, heartbeat);
				s.setString(6, clientID);
				s.setLong(7, heartbeat);
				s.setString(8, binlogPosition.getGtidSetStr());
				s.setString(9, binlogPosition.getFile());
				s.setLong(10, binlogPosition.getOffset());

				s.execute();
			}
		});
	}

	public long heartbeat() throws Exception {
		long heartbeatValue = System.currentTimeMillis();
		heartbeat(heartbeatValue);
		return heartbeatValue;
	}

	public synchronized void heartbeat(long heartbeatValue) throws SQLException, DuplicateProcessException {
		connectionPool.withSQLRetry(1, (c) ->  {
			heartbeat(c, heartbeatValue);
		});
	}

	/*
	 * the heartbeat system performs two functions:
	 * 1 - it leaves pointers in the binlog in order to facilitate master recovery
	 * 2 - it detects duplicate maxwell processes configured with the same client_id, aborting if we detect a dupe.
	 */

	private Long lastHeartbeat = null;

	private Long insertHeartbeat(Connection c, Long thisHeartbeat) throws SQLException, DuplicateProcessException {
		String heartbeatInsert = "insert into `heartbeats` set `heartbeat` = ?, `server_id` = ?, `client_id` = ?";


		try ( PreparedStatement s = c.prepareStatement(heartbeatInsert) ) {
			s.setLong(1, thisHeartbeat);
			s.setLong(2, serverID);
			s.setString(3, clientID);

			s.execute();
			return thisHeartbeat;
		} catch ( SQLIntegrityConstraintViolationException e ) {
			throw new DuplicateProcessException("Found heartbeat row for client,position while trying to insert.  Is another maxwell running?");
		}
	}

	private void heartbeat(Connection c, long thisHeartbeat) throws SQLException, DuplicateProcessException {
		if ( lastHeartbeat == null ) {
			try ( PreparedStatement s = c.prepareStatement("SELECT `heartbeat` from `heartbeats` where server_id = ? and client_id = ?") ) {
				s.setLong(1, serverID);
				s.setString(2, clientID);

				try ( ResultSet rs = s.executeQuery() ) {
					if ( !rs.next() ) {
						insertHeartbeat(c, thisHeartbeat);
						lastHeartbeat = thisHeartbeat;
						return;
					} else {
						lastHeartbeat = rs.getLong("heartbeat");
					}
				}
			}
		}

		String heartbeatUpdate = "update `heartbeats` set `heartbeat` = ? where `server_id` = ? and `client_id` = ? and `heartbeat` = ?";

		final int nRows;
		try ( PreparedStatement s = c.prepareStatement(heartbeatUpdate) ) {
			s.setLong(1, thisHeartbeat);
			s.setLong(2, serverID);
			s.setString(3, clientID);
			s.setLong(4, lastHeartbeat);

			LOGGER.debug("writing heartbeat: {} (last heartbeat written: {})", thisHeartbeat, lastHeartbeat);
			nRows = s.executeUpdate();
		}
		if ( nRows != 1 ) {
			String msg = String.format(
				"Expected a heartbeat value of %d but didn't find it.  Is another Maxwell process running with the same client_id?",
				lastHeartbeat
			);

			throw new DuplicateProcessException(msg);
		}

		lastHeartbeat = thisHeartbeat;
	}

	public Long getLastHeartbeatSent() {
		return lastHeartbeat;
	}

	private Position positionFromResultSet(ResultSet rs) throws SQLException {
		if ( !rs.next() )
			return null;

		return MysqlPositionStore.positionFromResultSet(rs, this.gtidMode);
	}

	public static Position positionFromResultSet(ResultSet rs, boolean gtidMode) throws SQLException {
		String gtid = gtidMode ? rs.getString("gtid_set") : null;
		BinlogPosition pos = new BinlogPosition(
			gtid,
			null,
			rs.getLong("binlog_position"),
			rs.getString("binlog_file")
		);

		return new Position(pos, rs.getLong("last_heartbeat_read"));
	}

	public Position getLatestFromAnyClient() throws SQLException {
		try ( Connection c = connectionPool.getConnection();
			  PreparedStatement s = c.prepareStatement("SELECT * from `positions` where server_id = ? ORDER BY last_heartbeat_read desc limit 1") ) {
			s.setLong(1, serverID);

			try ( ResultSet rs = s.executeQuery() ) {
				return positionFromResultSet(rs);
			}
		}
	}

	public Position get() throws SQLException {
		try ( Connection c = connectionPool.getConnection();
			  PreparedStatement s = c.prepareStatement("SELECT * from `positions` where server_id = ? and client_id = ?") ) {
			s.setLong(1, serverID);
			s.setString(2, clientID);

			try ( ResultSet rs = s.executeQuery() ) {
				return positionFromResultSet(rs);
			}
		}
	}
}
