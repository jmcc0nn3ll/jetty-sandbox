package org.mortbay.jetty.mongodb;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.jetty.server.session.AbstractSession;


/* ------------------------------------------------------------ */
public class NoSqlSession extends AbstractSession
{
    private final NoSqlSessionManager _manager;
    private Set<String> _dirty;
    private final AtomicInteger _active = new AtomicInteger();
    private Object _version;
    private long _lastSync;

    /* ------------------------------------------------------------ */
    protected NoSqlSession(NoSqlSessionManager manager, long created, long accessed, String clusterId)
    {
        super(manager, created,accessed,clusterId);
        _manager=manager;
        save(true);
        _active.incrementAndGet();
    }
    
    /* ------------------------------------------------------------ */
    protected NoSqlSession(NoSqlSessionManager manager, long created, long accessed, String clusterId, Object version)
    {
        super(manager, created,accessed,clusterId);
        _manager=manager;
        _version=version;
    }
    
    /* ------------------------------------------------------------ */
    @Override
    protected Object doPutOrRemove(String name, Object value)
    {
        synchronized (this)
        {
            if (_dirty==null)
                _dirty=new HashSet<String>();
            _dirty.add(name);
            Object old = super.doPutOrRemove(name,value);
            if (_manager.getSavePeriod()==-2)
                save(true);
            return old;
        }
    }

    /* ------------------------------------------------------------ */
    @Override
    protected void checkValid() throws IllegalStateException
    {
        super.checkValid();
    }

    /* ------------------------------------------------------------ */
    @Override
    protected boolean access(long time)
    {
        System.err.println("access "+_active);
        if (_active.incrementAndGet()==1)
        {
            int period=_manager.getStalePeriod()*1000;
            if (period==0)
                refresh();
            else if (period>0)
            {
                long stale=time-_lastSync;
                System.err.println("stale "+stale);
                if (stale>period)
                    refresh();
            }
        }

        return super.access(time);
    }

    /* ------------------------------------------------------------ */
    @Override
    protected void complete()
    {
        super.complete();
        if(_active.decrementAndGet()==0)
        {
            switch(_manager.getSavePeriod())
            {
                case 0: 
                    save(isValid());
                    break;
                case 1:
                    if (isDirty())
                        save(isValid());
                    break;

            }
        }
    }

    /* ------------------------------------------------------------ */
    @Override
    protected void doInvalidate() throws IllegalStateException
    {
        super.doInvalidate();
        save(false);
    }
    
    /* ------------------------------------------------------------ */
    protected void save(boolean activateAfterSave)
    {
        synchronized (this)
        {
            _version=_manager.save(this,_version,activateAfterSave);
            _lastSync=getAccessed();
        }
    }


    /* ------------------------------------------------------------ */
    protected void refresh()
    {
        synchronized (this)
        {
            _version=_manager.refresh(this,_version);
        }
    }

    /* ------------------------------------------------------------ */
    public boolean isDirty()
    {
        synchronized (this)
        {
            return _dirty!=null && !_dirty.isEmpty();
        }
    }
    
    /* ------------------------------------------------------------ */
    public Set<String> takeDirty()
    {
        synchronized (this)
        {
            Set<String> dirty=_dirty;
            if (dirty==null)
                dirty=Collections.emptySet();
            else
                _dirty=null;
            return dirty;
        }
    }

}
