package com.zendesk.maxwell.replication;

import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.SchemaStoreException;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.util.StoppableTask;

/**
 * Created by ben on 10/23/16.
 */
public interface Replicator extends StoppableTask {
	void startReplicator() throws Exception;
	RowMap getRow() throws Exception;
	Long getLastHeartbeatRead();
	Schema getSchema() throws SchemaStoreException;
	Long getSchemaId() throws SchemaStoreException;


	// for binlogreplicator
	String getFilter();
	void resetFilter(Filter filter);
	void addFilter(String filterString) throws Exception;
	void removeFilter(String filterString) throws Exception;

	void stopAtHeartbeat(long heartbeat);
	void runLoop() throws Exception;
}
