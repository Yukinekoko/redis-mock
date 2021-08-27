package com.github.zxl0714.redismock;

import com.github.zxl0714.redismock.pattern.KeyPattern;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class RedisBase {

    private static final Logger LOGGER = Logger.getLogger(RedisBase.class.getName());

    private final RedisDataBase[] dataBases;
    // TODO : (snowmeow, 2021-7-22) 考虑主从模式下指令的操作限制
    private final List<RedisBase> syncBases = Lists.newArrayList();

    private final Map<String, Set<Socket>> channels = Maps.newHashMap();

    public RedisBase() {
        this(16);
    }

    public RedisBase(int dataBaseCount) {
        dataBases = new RedisDataBase[dataBaseCount];
        for (int i = 0; i < dataBases.length; i++) {
            dataBases[i] = new RedisDataBase();
        }
    }

    public int getDataBaseCount() {
        return dataBases.length;
    }

    public void addSyncBase(RedisBase base) {
        syncBases.add(base);
    }

    @Nullable
    public synchronized Slice rawGet(Slice key) {
        return rawGet(key, selectIndex());
    }

    @Nullable
    public synchronized Slice rawGet(Slice key, int index) {
        Preconditions.checkNotNull(key);
        RedisDataBase dataBase = dataBases[index];
        Long deadline = dataBase.getDeadlines().get(key);
        if (deadline != null && deadline != -1 && deadline <= System.currentTimeMillis()) {
            dataBase.getBase().remove(key);
            dataBase.getDeadlines().remove(key);
            return null;
        }
        return dataBase.getBase().get(key);
    }

    public synchronized List<Slice> rawKeys(Slice pattern) {
        RedisDataBase dataBase = selectDataBase();
        KeyPattern p = new KeyPattern(pattern);
        List<Slice> keys = new ArrayList<>();
        for(Map.Entry<Slice, Slice> entry : dataBase.getBase().entrySet()) {
            if(p.match(entry.getKey())){
                Long deadline = dataBase.getDeadlines().get(entry.getKey());
                if (deadline != null && deadline != -1 && deadline <= System.currentTimeMillis()) {
                    dataBase.getBase().remove(entry.getKey());
                    dataBase.getDeadlines().remove(entry.getKey());
                }else {
                    keys.add(entry.getKey());
                }
            }
        }
        return keys;
    }

    @Nullable
    public synchronized Long getTTL(Slice key) {
        Preconditions.checkNotNull(key);
        RedisDataBase dataBase = selectDataBase();
        Long deadline = dataBase.getDeadlines().get(key);
        if (deadline == null) {
            return null;
        }
        if (deadline == -1) {
            return deadline;
        }
        long now = System.currentTimeMillis();
        if (now < deadline) {
            return deadline - now;
        }
        dataBase.getBase().remove(key);
        dataBase.getDeadlines().remove(key);
        return null;
    }

    public synchronized long setTTL(Slice key, long ttl) {
        return setTTL(key, ttl, selectIndex());
    }

    public synchronized long setTTL(Slice key, long ttl, int index) {
        Preconditions.checkNotNull(key);
        RedisDataBase dataBase = dataBases[index];
        if (dataBase.getBase().containsKey(key)) {
            dataBase.getDeadlines().put(key, ttl + System.currentTimeMillis());
            for(RedisBase base : syncBases) {
                base.setTTL(key, ttl, index);
            }
            return 1L;
        }
        return 0L;
    }

    public synchronized long setDeadline(Slice key, long deadline) {
        return setDeadline(key, deadline, selectIndex());
    }

    public synchronized long setDeadline(Slice key, long deadline, int index) {
        Preconditions.checkNotNull(key);
        RedisDataBase dataBase = dataBases[index];
        if (dataBase.getBase().containsKey(key)) {
            dataBase.getDeadlines().put(key, deadline);
            for(RedisBase base : syncBases) {
                base.setDeadline(key, deadline, index);
            }
            return 1L;
        }
        return 0L;
    }

    public synchronized void rawPut(Slice key, Slice value, @Nullable Long ttl) {
        rawPut(key, value, ttl, selectIndex());
    }

    public synchronized void rawPut(Slice key, Slice value, @Nullable Long ttl, int index) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        RedisDataBase dataBase = dataBases[index];
        dataBase.getBase().put(key, value);
        if (ttl != null) {
            if (ttl != -1) {
                dataBase.getDeadlines().put(key, ttl + System.currentTimeMillis());
            } else {
                dataBase.getDeadlines().put(key, -1L);
            }
        }
        for(RedisBase base : syncBases) {
            base.rawPut(key, value, ttl, index);
        }
    }

    public synchronized void del(Slice key) {
        del(key, selectIndex());
    }

    public synchronized void del(Slice key, int index) {
        Preconditions.checkNotNull(key);

        dataBases[index].getBase().remove(key);
        dataBases[index].getDeadlines().remove(key);

        for(RedisBase base : syncBases) {
            base.del(key, index);
        }
    }

    public synchronized void subscribe(Slice channelKey, Socket socket) {
        String channelName = channelKey.toString();
        Set<Socket> channel = channels.computeIfAbsent(channelName, k -> Sets.newHashSet());
        channel.add(socket);
    }

    public synchronized boolean unSubscribe(Slice channelKey, Socket socket) {
        String channelName = channelKey.toString();
        Set<Socket> channel = channels.get(channelName);
        if (channel != null) {
            return channel.remove(socket);
        }
        return false;
    }

    public synchronized int unSubscribeAll(Socket socket) {
        int resp = 0;
        for (Set<Socket> socketSet : channels.values()) {
            if (socketSet.remove(socket)) {
                resp++;
            }
        }
        return resp;
    }

    public synchronized int publish(Slice channelKey, Slice messageKey) {
        String channelName = channelKey.toString();
        Set<Socket> channel = channels.get(channelName);
        if (channel == null) {
            return 0;
        }
        Iterator<Socket> iterator = channel.iterator();
        while (iterator.hasNext()) {
            Socket socket = iterator.next();
            try {
                OutputStream outputStream = socket.getOutputStream();
                ImmutableList.Builder<Slice> builder = new ImmutableList.Builder<>();
                builder.add(Response.bulkString(new Slice("message")));
                builder.add(Response.bulkString(channelKey));
                builder.add(Response.bulkString(messageKey));
                Slice resp = Response.array(builder.build());
                outputStream.write(resp.data());
                outputStream.flush();
            } catch (IOException e) {
                LOGGER.warning(channelName + " 订阅者socket.outputStream获取异常：" + e.getMessage());
                // socket被关闭了就从订阅者Set中移除
                iterator.remove();
            }
        }
        return channel.size();
    }

    /**
     * 选择当前socket指定的数据库
     * */
    private synchronized RedisDataBase selectDataBase() {
        int index = selectIndex();
        return dataBases[index];
    }

    /**
     * 获得当前socket指定的数据库index
     *  */
    private synchronized int selectIndex() {
        SocketAttributes socketAttributes = SocketContextHolder.getSocketAttributes();
        Preconditions.checkNotNull(socketAttributes);

        int index = socketAttributes.getDatabaseIndex();
        if (index >= dataBases.length || index < 0) {
            index = 0;
        }
        return index;
    }
}
