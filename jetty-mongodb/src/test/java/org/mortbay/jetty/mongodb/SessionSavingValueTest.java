package org.mortbay.jetty.mongodb;

import org.eclipse.jetty.server.session.AbstractSessionValueSavingTest;
import org.eclipse.jetty.server.session.AbstractTestServer;
import org.junit.Ignore;

public class SessionSavingValueTest extends AbstractSessionValueSavingTest {


    public AbstractTestServer createServer(int port, int max, int scavenge)
    {
        MongoTestServer server = new MongoTestServer(port,max,scavenge, true);
                       
        return server;
    }

	@Override
	public void testSessionValueSaving() throws Exception 
	{
		super.testSessionValueSaving();
	}
   
}
