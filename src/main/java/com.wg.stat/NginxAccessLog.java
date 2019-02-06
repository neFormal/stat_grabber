package com.wg.stat;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by neformal on 12.06.18.
 */
public class NginxAccessLog implements Serializable {

    private static final Logger logger = Logger.getLogger("Access");

    // Example Nginx log line:
    // 127.0.0.1 [2018-06-12T22:24:40+03:00] "foo=bar"
    // 127.0.0.1 [2018-06-12T22:25:35+03:00] "msg=test&a1=1&a2=2&a3=3"
    // 127.0.0.1 [2018-06-12T22:25:43+03:00] "msg=test&a1=5&a22=2&a3=83"
    private static final String LOG_ENTRY_PATTERN =
            // 1:IP  2:date time 3:req
            "^(\\S+) \\[([\\w:\\-\\+]+)\\] \"(\\S+)\"";
    // /(\d{4})-(\d{2})-(\d{2})T(\d{2})\:(\d{2})\:(\d{2})[+-](\d{2})\:(\d{2})/
    // ^(?:[1-9]\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\d|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[1-9]\d(?:0[48]|[2468][048]|[13579][26])|(?:[2468][048]|[13579][26])00)-02-29)T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:Z|[+-][01]\d:[0-5]\d)$
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    public static NginxAccessLog parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            logger.log(Level.ALL, "Cannot parse logline" + logline);
            throw new RuntimeException("Error parsing logline");
        }

        return new NginxAccessLog(m.group(1), m.group(2), m.group(3));
    }

    @Override
    public String toString() {
        return String.format("%s [%s] %s",
                ipAddress, dateTime, request);
    }

    private static final String ARGS_PATTERN =
            // 1:IP  2:date time 3:req
            "^(\\S+) \\[([\\w:\\-\\+]+)\\] \"(\\S+)\"";

    public Map<String, String> getArgs() {
        Map<String, String> args = new LinkedHashMap<String, String>() {{
            put("ip", ipAddress);
            put("datetime", dateTime);
        }};
        String[] parts = this.request.split("&");
        for (String x : parts) {
            System.out.println("part: " + x);
            if (x.length() <= 0)
                continue;

            String[] keyValue = x.split("=");
            System.out.println("keyvals: " + keyValue.length);
            if (keyValue.length <= 1)
                continue;

            args.put(keyValue[0], keyValue[1]);
        }

        return args;
    }

    public static String parseToCsv(String filename, String... fields) throws IOException {

//        new File(filename);
//        FileReader reader = new FileReader(filename);
//        reader.close();

        return Files.lines(new File(filename).toPath())
                .map(NginxAccessLog::parseFromLogLine)
                .map(log -> log.getArgs())
                .map(args -> Arrays.stream(fields)
                        .map(x -> args.getOrDefault(x, ""))
                        .collect(Collectors.joining(",")) )
                .collect(Collectors.joining("\n"));
//                .map(args -> {
////                    for (Map.Entry entry : args.entrySet())
//                    return args.entrySet().stream().filter(e -> {
//                        for (int i = 0; i < fields.length; i++) {
//                            if (fields[i].equals(e.getKey()))
//                                return true;
//                        }
//                        return false;
//                    })
//                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//                })
    }

    private String ipAddress;
    private String dateTime;
    private String request;

    private NginxAccessLog(String ipAddress, String dateTime, String request) {
        this.ipAddress = ipAddress;
        this.dateTime = dateTime;
        this.request = request;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }
}
