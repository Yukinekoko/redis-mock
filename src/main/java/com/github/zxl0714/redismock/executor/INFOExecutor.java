package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;

import java.io.IOException;
import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO : (snowmeow:2021/8/23) 待编写
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/23
 */
public class INFOExecutor extends AbstractExecutor {

    private Map<String, String> infoMap;

    private String redisVersion = "4.0.9";

    public INFOExecutor() {
        initInfo();
    }

    private void initInfo() {
        infoMap = new LinkedHashMap<>(8);
        StringBuilder serverSb = new StringBuilder();
        serverSb.append("# Server\r\n")
            .append("redis_version:4.0.9\r\n")
            .append("redis_git_sha1:1\r\n")
            .append("redis_git_dirty:0\r\n")
            .append("redis_build_id:1\r\n")
            .append("redis_mode:standalone\r\n")
            .append("os:Windows\r\n")
            .append("arch_bits:64\r\n")
            .append("multiplexing_api:WinSock_IOCP\r\n")
            .append("tcp_port:6379\r\n")
            .append("uptime_in_seconds:334\r\n")
            .append("uptime_in_days:0\r\n")
            .append("hz:10\r\n")
            .append("configured_hz:10\r\n")
            .append("lru_clock:1\r\n")
            .append("executable:/home/redis.exe\r\n")
            .append("config_file:/home/redis.conf\r\n");
        /*Map<String, String> serverMap = new LinkedHashMap<>(32);
        serverMap.put("redis_version", redisVersion);
        serverMap.put("redis_git_sha1", "1");
        serverMap.put("redis_git_dirty", "0");*/

        infoMap.put("server", serverSb.toString());
    }

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberLess(params, 2);
        if (params.size() == 1) {
            String responseInfo = infoMap.getOrDefault(params.get(0).toString(), "");
            return Response.bulkString(new Slice(responseInfo));
        }
        StringBuilder sb = new StringBuilder();
        for (String value : infoMap.values()) {
            sb.append(value);
            sb.append("\r\n");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.deleteCharAt(sb.length() - 1);
        return Response.bulkString(new Slice(sb.toString()));
    }
}
