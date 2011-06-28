package org.mortbay.jetty.mongodb;

import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.SessionManager;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.session.AbstractSessionIdManager;
import org.eclipse.jetty.server.session.SessionHandler;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

/**
 * Based partially on the jdbc session id manager...
 *
 * Theory is that we really only need the session id manager for the local 
 * instance so we have something to scavenge on, namely the list of known ids
 * 
 * this class has a timer that runs at the scavenge delay that runs a query
 *  for all id's known to this node and that have and old accessed value greater
 *  then the scavengeDelay.
 *  
 * these found sessions are then run through the invalidateAll(id) method that 
 * is a bit hinky but is supposed to notify all handlers this id is now DOA and 
 * ought to be cleaned up.  this ought to result in a save operation on the session
 * that will change the valid field to false (this conjecture is unvalidated atm)
 */
public class MongoSessionIdManager extends AbstractSessionIdManager
{
    final static DBObject __version_1 = new BasicDBObject("version",1);
    final DBCollection _sessions;
    protected Server _server;
    private Timer _timer;
    private TimerTask _task;
    
    private long _scavengeDelay = 30 * 60 * 1000; // every 30 minutes
    private long _scavengePeriod = 10 * 6 * 1000; // wait at least 10 minutes
    
    protected final HashSet<String> _sessionsIds = new HashSet<String>();
    

    /* ------------------------------------------------------------ */
    public MongoSessionIdManager(Server server) throws UnknownHostException, MongoException
    {
        this(server, new Mongo().getDB("HttpSessions").getCollection("sessions"));
    }

    /* ------------------------------------------------------------ */
    public MongoSessionIdManager(Server server, DBCollection sessions)
    {
        super(new Random());
        
        _server = server;
        _sessions = sessions;

        _sessions.ensureIndex(
                BasicDBObjectBuilder.start().add("id",1).get(),
                BasicDBObjectBuilder.start().add("unique",true).add("sparse",false).get());
        _sessions.ensureIndex(
                BasicDBObjectBuilder.start().add("id",1).add("version",1).get(),
                BasicDBObjectBuilder.start().add("unique",true).add("sparse",false).get());

    }
    
    /* ------------------------------------------------------------ */
    public DBCollection getSessions()
    {
        return _sessions;
    }
    
    /* ------------------------------------------------------------ */
    private void scavenge()
    {
        //System.err.println("SessionIdManager:scavenge:called");

        BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
                
        synchronized (_sessionsIds)
        {
            for ( String sessionId : _sessionsIds )
            {
                System.err.println("SessionIdManager:scavenge:checking " + sessionId + "/" + (System.currentTimeMillis() - _scavengeDelay) );
                builder.add("id", sessionId );
            }
            
            BasicDBObject query = new BasicDBObject();
            
            query.put("id",new BasicDBObject("$in", _sessionsIds ));

            query.put("accessed", new BasicDBObject("$lt",System.currentTimeMillis() - _scavengeDelay));
            
            // TODO limit by pulling back specific fields!
            DBCursor checkSessions = _sessions.find(query);
            
            //System.out.println("SessionIdManager:scavenge:found " + checkSessions.count());
            
            for ( DBObject session : checkSessions )
            {
                System.err.println("SessionIdManager:scavenge:old session:" + (String)session.get("id"));
                
                // TODO - also need to set the valid=false directly in case this session is not in memory anywhere in this node.
                
                invalidateAll((String)session.get("id"));
            }
        } 
        
    }

    /* ------------------------------------------------------------ */
    /**
     * sets the scavengeDelay
     */
    public void setScavengeDelay(long scavengeDelay)
    {
        this._scavengeDelay = scavengeDelay;  
    }


    /* ------------------------------------------------------------ */
    public void setScavengePeriod(long scavengePeriod)
    {
        this._scavengePeriod = scavengePeriod;
    }

    /* ------------------------------------------------------------ */
    @Override
    protected void doStart() throws Exception
    {
        System.out.println("MongoSessionIdManager:starting");
        
        if (_scavengeDelay > 0)
        {
            _timer = new Timer("MongoSessionScavenger",true);

            synchronized (this)
            {
                if (_task != null)
                    _task.cancel();
                _task = new TimerTask()
                {
                    @Override
                    public void run()
                    {
                        scavenge();
                    }
                };
                _timer.schedule(_task,_scavengeDelay,_scavengePeriod);
            }
        }
    }
    
    /* ------------------------------------------------------------ */
    @Override
    protected void doStop() throws Exception
    {
        if (_timer != null)
        {
            _timer.cancel();
            _timer = null;
        }
        
        super.doStop();
    }

    /* ------------------------------------------------------------ */
    /**
     * is the session id known to mongo, and is it valid
     */
    @Override
    public boolean idInUse(String sessionId)
    {

        DBObject o = _sessions.findOne(new BasicDBObject("id",sessionId),__version_1);
        
        if ( o != null )
        {
            Boolean valid = (Boolean)o.get("valid");
            
            if ( valid == null )
            {
                return false;
            }
            
            return valid;
        }
        
        return false;
    }

    /* ------------------------------------------------------------ */
    @Override
    public void addSession(HttpSession session)
    {
        if (session == null)
        {
            return;
        }
        
        /*
         * already a part of the index in mongo...
         */
        
        System.out.println("MongoSessionIdManager:addSession:" + session.getId());
        
        synchronized (_sessionsIds)
        {
            _sessionsIds.add(session.getId());
        }
        
    }

    /* ------------------------------------------------------------ */
    @Override
    public void removeSession(HttpSession session)
    {
        if (session == null)
        {
            return;
        }
        
        synchronized (_sessionsIds)
        {
            _sessionsIds.remove(session.getId());
        }
    }

    /* ------------------------------------------------------------ */
    @Override
    public void invalidateAll(String sessionId)
    {
        synchronized (_sessionsIds)
        {
            _sessionsIds.remove(sessionId);
            
            
            //tell all contexts that may have a session object with this id to
            //get rid of them
            Handler[] contexts = _server.getChildHandlersByClass(ContextHandler.class);
            for (int i=0; contexts!=null && i<contexts.length; i++)
            {
                SessionHandler sessionHandler = (SessionHandler)((ContextHandler)contexts[i]).getChildHandlerByClass(SessionHandler.class);
                if (sessionHandler != null) 
                {
                    SessionManager manager = sessionHandler.getSessionManager();

                    if (manager != null && manager instanceof MongoSessionManager)
                    {
                        ((MongoSessionManager)manager).invalidateSession(sessionId);
                    }
                }
            }
        }      
    }

    /* ------------------------------------------------------------ */
    // TODO not sure if this is correct
    @Override
    public String getClusterId(String nodeId)
    {
        int dot=nodeId.lastIndexOf('.');
        return (dot>0)?nodeId.substring(0,dot):nodeId;
    }

    /* ------------------------------------------------------------ */
    // TODO not sure if this is correct
    @Override
    public String getNodeId(String clusterId, HttpServletRequest request)
    {
        if (_workerName!=null)
            return clusterId+'.'+_workerName;

        return clusterId;
    }

}
