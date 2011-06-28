// ========================================================================
// Copyright 2004-2005 Mort Bay Consulting Pty. Ltd.
// ------------------------------------------------------------------------
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ========================================================================

package org.mortbay.jetty.mongodb;



import java.net.UnknownHostException;

import org.eclipse.jetty.server.SessionIdManager;
import org.eclipse.jetty.server.SessionManager;
import org.eclipse.jetty.server.session.AbstractTestServer;
import org.eclipse.jetty.server.session.HashSessionIdManager;
import org.eclipse.jetty.server.session.SessionHandler;


/**
 * @version $Revision$ $Date$
 */
public class MongoTestServer extends AbstractTestServer
{
    
    static MongoSessionIdManager _idManager;

    public MongoTestServer(int port)
    {
        super(port, 30, 10);
    }

    public MongoTestServer(int port, int maxInactivePeriod, int scavengePeriod)
    {
        super(port, maxInactivePeriod, scavengePeriod);
    }


    public SessionIdManager newSessionIdManager()
    {
        if ( _idManager != null )
        {
            try
            {
                _idManager.stop();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
            
            _idManager.setScavengeDelay(_scavengePeriod + 1000);
            _idManager.setScavengePeriod(_maxInactivePeriod);       
            
            try
            {
                _idManager.start();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
            
            return _idManager;
        }
        
        try
        {
            System.err.println("MongoTestServer:SessionIdManager:" + _maxInactivePeriod + "/" + _scavengePeriod);
            _idManager = new MongoSessionIdManager(_server);
            
            _idManager.setScavengeDelay(_scavengePeriod);
            _idManager.setScavengePeriod(_maxInactivePeriod);                  
            
            return _idManager;
        }
        catch (Exception e)
        {
            throw new IllegalStateException();
        }
    }

    public SessionManager newSessionManager()
    {
        MongoSessionManager manager;
        try
        {
            manager = new MongoSessionManager();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        
        //manager.setScavengePeriod((int)TimeUnit.SECONDS.toMillis(_scavengePeriod));
        return manager;
    }

    public SessionHandler newSessionHandler(SessionManager sessionManager)
    {
        return new SessionHandler(sessionManager);
    }
    
    public static void main(String... args) throws Exception
    {
        MongoTestServer server8080 = new MongoTestServer(8080);
        server8080.addContext("/").addServlet(SessionDump.class,"/");
        server8080.start();
        
        MongoTestServer server8081 = new MongoTestServer(8081);
        server8081.addContext("/").addServlet(SessionDump.class,"/");
        server8081.start();
        
        server8080.join();
        server8081.join();
    }

}
