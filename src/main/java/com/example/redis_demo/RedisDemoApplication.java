package com.example.redis_demo;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.NonTransientDataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@AllArgsConstructor
@SpringBootApplication
public class RedisDemoApplication implements CommandLineRunner {

    private static final String LUA_SCRIPT_PUT_INDEX = getLuaScript("scripts/push.lua");

    private final RedisTemplate<String, String> redisTemplate;

    private final RedisSerializer<String> stringSerializer = RedisSerializer.string();


    public static void main(String[] args) {
        SpringApplication.run(RedisDemoApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        RedisScript<String> script = RedisScript.of(LUA_SCRIPT_PUT_INDEX, String.class);

        var res = redisTemplate.execute((RedisCallback<String>) conn -> {
            return this.executeScript(conn, script, List.of("bar"), List.of("idx1:AAA", "idx4:DDD", "idx3:CCC"), stringSerializer, stringSerializer);
        });

        log.info(res);
    }

    private <T> T executeScript(
            RedisConnection conn,
            RedisScript<T> script,
            final List<String> keys,
            final List<String> args,
            final RedisSerializer<String> inSerializer,
            final RedisSerializer<T> outSerializer) {

        final ReturnType returnType = ReturnType.fromJavaType(script.getResultType());
        final byte[][] keysAndArgs = keysAndArgs(keys, args, inSerializer, inSerializer);
        final int keySize = keys != null ? keys.size() : 0;

        T result;
        try {
            result = conn.scriptingCommands().evalSha(script.getSha1(), returnType, keySize, keysAndArgs);
        } catch (Exception ex) {
            if (!exceptionContainsNoScriptError(ex)) {
                throw ex;
            }
            // EVAL will also cache the script
            log.info("EVAL and LOAD Script...");
            result = conn.scriptingCommands().eval(scriptBytes(script), returnType, keySize, keysAndArgs);
        }

        // deserialize result
        if (script.getResultType() == null) {
            return null;
        }
        return deserializeResult(outSerializer, result);
    }

    private static byte[] scriptBytes(RedisScript<?> script) {
        return script.getScriptAsString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Checks whether given {@link Throwable} contains a {@code NOSCRIPT} error. {@code NOSCRIPT} is reported if a script
     * was attempted to execute using {@code EVALSHA}.
     *
     * @param e the exception.
     * @return {@literal true} if the exception or one of its causes contains a {@literal NOSCRIPT} error.
     */
    private static boolean exceptionContainsNoScriptError(Throwable e) {
        if (!(e instanceof NonTransientDataAccessException)) {
            return false;
        }
        Throwable current = e;
        while (current != null) {
            String exMessage = current.getMessage();
            if (exMessage != null && exMessage.contains("NOSCRIPT")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> T deserializeResult(RedisSerializer<T> resultSerializer, Object result) {
        if (result instanceof byte[] resultBytes) {
            return resultSerializer.deserialize(resultBytes);
        }

        if (result instanceof List listResult) {
            List<Object> results = new ArrayList<>(listResult.size());
            for (Object obj : listResult) {
                results.add(deserializeResult(resultSerializer, obj));
            }
            return (T) results;
        }
        return (T) result;
    }

    private static byte[][] keysAndArgs(
            final List<String> keys,
            final List<String> args,
            final RedisSerializer<String> keySerializer,
            final RedisSerializer<String> argsSerializer) {

        final int keySize = keys != null ? keys.size() : 0;
        final int argSize = args != null ? args.size() : 0;
        byte[][] keysAndArgs = new byte[keySize + argSize][];

        int i = 0;
        if (keys != null) {
            for (String key : keys) {
                keysAndArgs[i++] = keySerializer.serialize(key);
            }
        }
        if (args != null) {
            for (String arg : args) {
                keysAndArgs[i++] = argsSerializer.serialize(arg);
            }
        }
        return keysAndArgs;
    }

    public static String getLuaScript(String filePath) {
        try {
            return new ClassPathResource(filePath).getContentAsString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
