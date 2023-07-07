package com.zendesk.maxwell.schema;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;
import com.zendesk.maxwell.*;
import java.sql.ResultSet;
import java.util.List;

import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.errors.DuplicateProcessException;
import com.zendesk.maxwell.replication.Position;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

public class MysqlPositionStoreTest extends MaxwellTestWithIsolatedServer {
	private MysqlPositionStore buildStore() throws Exception {
		return buildStore(buildContext());
	}

	private MysqlPositionStore buildStore(MaxwellContext context) throws Exception {
		return buildStore(context, context.getServerID());
	}

	private MysqlPositionStore buildStore(MaxwellContext context, Long serverID) throws Exception {
		return new MysqlPositionStore(context.getMaxwellConnectionPool(), serverID, "maxwell", MaxwellTestSupport.inGtidMode());
	}

	private MysqlPositionStore buildStore(MaxwellContext context, Long serverID, String clientId) throws Exception {
		return new MysqlPositionStore(context.getMaxwellConnectionPool(), serverID, clientId, MaxwellTestSupport.inGtidMode());
	}

	@Test
	public void testSetBinlogPosition() throws Exception {
		MysqlPositionStore store = buildStore();
		BinlogPosition binlogPosition;
		if (MaxwellTestSupport.inGtidMode()) {
			String gtid = "123:1-100";
			binlogPosition = new BinlogPosition(gtid, null, 12345, "foo");
		} else {
			binlogPosition = new BinlogPosition(12345, "foo");
		}
		Position position = new Position(binlogPosition, 100L);
		store.set(position);
		assertEquals(buildStore().get(), position);
	}

	@Test
	public void testHeartbeat() throws Exception {
		MysqlPositionStore store = buildStore();
		store.set(new Position(new BinlogPosition(12345, "foo"), 0L));

		Long preHeartbeat = System.currentTimeMillis();
		store.heartbeat();

		ResultSet rs = server.getConnection().createStatement().executeQuery("select * from maxwell.heartbeats");
		rs.next();

		assertTrue(rs.getLong("heartbeat") >= preHeartbeat);
	}

	@Test
	public void testHeartbeatDuplicate() throws Exception {
		MysqlPositionStore store = buildStore();
		store.set(new Position(new BinlogPosition(12345, "foo"), 0L));

		store.heartbeat();
		buildStore().heartbeat();


		Exception exception = null;

		try {
			store.heartbeat();
		} catch (DuplicateProcessException d) {
			exception = d;
		}

		assertNotNull(exception);
	}
}
