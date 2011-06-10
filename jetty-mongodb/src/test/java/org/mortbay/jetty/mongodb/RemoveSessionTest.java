package org.mortbay.jetty.mongodb;

import org.eclipse.jetty.server.session.AbstractRemoveSessionTest;
import org.eclipse.jetty.server.session.AbstractTestServer;
import org.junit.Test;

public class RemoveSessionTest extends AbstractRemoveSessionTest
{ 

    public AbstractTestServer createServer(int port, int max, int scavenge)
    {
        return new MongoTestServer(port,max,scavenge);
    }
    
    @Test
    public void testRemoveSession() throws Exception
    {
        super.testRemoveSession();
    }

}
