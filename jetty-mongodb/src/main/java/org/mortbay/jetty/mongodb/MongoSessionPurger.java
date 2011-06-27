package org.mortbay.jetty.mongodb;

import java.util.Timer;
import java.util.TimerTask;

import org.eclipse.jetty.util.component.AbstractLifeCycle;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * Component that can be setup to remove aged sessions from mongo
 * 
 * TODO: this ought to validate they are invalid as well?  so we never scrape things
 * still marked valid=true as a safety measure?
 * 
 * Theory on this class is that we require some other process that will 
 * crawl through the back end and clean out the old sessions.  This only 
 * needs be configured on one node in a cluster and its sole purpose is to 
 * lazily walk through the sessions that match the criteria and remove them
 * completely from the back end.
 * 
 * 
 */
public class MongoSessionPurger extends AbstractLifeCycle implements NoSqlPurger
{
    private DBCollection _sessions;
    private Timer _timer;
    private TimerTask _task;
    
    private long _purgeDelay = 60 * 60 * 1000; // default 1 hr
    private long _purgePeriod = 0;
    private long _minimalPurgeAge = 24* 60 * 60 * 1000; // default 1 day

    public MongoSessionPurger(DBCollection sessions)
    {
        _sessions = sessions;
        
        // No idea when purger might run so best make sure things are correct
        // before things start going
        _sessions.ensureIndex(
                BasicDBObjectBuilder.start()
                .add("id",1).get(),
                BasicDBObjectBuilder.start()
                .add("unique",true)
                .add("sparse",false)
                .get());
        _sessions.ensureIndex(
                BasicDBObjectBuilder.start()
                .add("id",1)
                .add("version",1).get(),
                BasicDBObjectBuilder.start()
                .add("unique",true)
                .add("sparse",false)
                .get());
        
    }

    public void purge()
    {
        BasicDBObject query = new BasicDBObject();

        /*
         * this ought to factor in valid = true, perhaps that is enough and drop the $lt check?
         */
        query.put("accessed",new BasicDBObject("$lt",System.currentTimeMillis() - _minimalPurgeAge));

        DBCursor oldSessions = _sessions.find(query);

        for (DBObject session : oldSessions)
        {
            String id = (String)session.get("id");
            System.out.println("scavenging " + id);
            
            _sessions.remove(session);
        }

    }

    @Override
    public void doStart() throws Exception
    {

        _timer = new Timer("MongoSessionPurger",true);

        synchronized (this)
        {
            if (_task != null)
                _task.cancel();
            _task = new TimerTask()
            {
                @Override
                public void run()
                {
                    purge();
                }
            };
            _timer.schedule(_task,_purgeDelay, _purgePeriod);
        }

        super.doStart();

    }

    @Override
    public void doStop() throws Exception
    {
        synchronized (this)
        {
            if (_task != null)
                _task.cancel();
            if (_timer != null)
                _timer.cancel();
            _timer = null;
        }
        super.doStop();
    }

    public long getPurgeDelay()
    {
        return _purgeDelay;
    }

    public void setPurgeDelay(long purgeDelay)
    {
        if ( isRunning() )
        {
            throw new IllegalStateException();
        }
        
        this._purgeDelay = purgeDelay;
    }

    public long getPurgePeriod()
    {
        return _purgePeriod;
    }

    public void setPurgePeriod(long purgePeriod)
    {
        if ( isRunning() )
        {
            throw new IllegalStateException();
        }
        
        this._purgePeriod = purgePeriod;
    }

    public long getMinimalPurgeAge()
    {
        return _minimalPurgeAge;
    }

    public void setMinimalPurgeAge(long minimalPurgeAge)
    {
        if ( isRunning() )
        {
            throw new IllegalStateException();
        }
        
        this._minimalPurgeAge = minimalPurgeAge;
    } 
}
