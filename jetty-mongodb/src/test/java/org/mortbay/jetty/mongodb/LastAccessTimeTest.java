package org.mortbay.jetty.mongodb;

import org.eclipse.jetty.server.session.AbstractLastAccessTimeTest;
import org.eclipse.jetty.server.session.AbstractTestServer;

public class LastAccessTimeTest extends AbstractLastAccessTimeTest
{

    public AbstractTestServer createServer(int port, int max, int scavenge)
    {
        return new MongoTestServer(port,max,scavenge);
    }

    @Override
    public void testLastAccessTime() throws Exception
    {
        super.testLastAccessTime();
    }
}
