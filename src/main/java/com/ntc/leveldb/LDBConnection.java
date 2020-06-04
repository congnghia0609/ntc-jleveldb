/*
 * Copyright 2016 nghiatc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ntc.leveldb;

import java.io.File;
import java.io.IOException;
import java.util.*;
import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;
import static org.fusesource.leveldbjni.JniDBFactory.factory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author nghiatc
 * @since Jul 28, 2016
 */
public class LDBConnection {

    private final Logger logger = LoggerFactory.getLogger(LDBConnection.class);

    private String dbDirectory;
    private DB db;
    private Options options;
    private NLSerializer nls;

    public String getDbDirectory() {
        return dbDirectory;
    }

    public DB getDb() {
        return db;
    }

    public Options getOptions() {
        return options;
    }
    
    public NLSerializer getNLSerializer() {
        return nls;
    }

    private LDBConnection() {
        options = new Options().createIfMissing(true);
        options.cacheSize(50 * 1048576); // 50MB cache
    }
    
    private void init(String pathDB) throws IOException {
        if (pathDB == null || pathDB.isEmpty()) {
            throw new ExceptionInInitializerError("Path to DB not empty.");
        }
        dbDirectory = pathDB;
        File dbDir = new File(pathDB);
        if (!dbDir.exists()) {
            if (!dbDir.mkdirs()) {
                throw new ExceptionInInitializerError("Path directory database can not created for: " + pathDB);
            }
        }
        nls = new NLSerializer();
        db = factory.open(dbDir, options);
    }

    public LDBConnection(String pathDB) throws IOException {
        options = new Options().createIfMissing(true);
        options.cacheSize(50 * 1048576); // 50MB cache
        init(pathDB);
    }

    public LDBConnection(String pathDB, Options opts) throws IOException {
        if (opts == null) {
            opts = new Options().createIfMissing(true);
            opts.cacheSize(50 * 1048576); // 50MB cache
        }
        this.options = opts;
        init(pathDB);
    }

//    public void openLDB(){
//        try {
//            db = factory.open(new File(dbDirectory), options);
//        } catch (Exception ex) {
//            logger.error("openLDB: ", ex);
//        }
//    }
    
    public void close() {
        try {
            if (db != null) {
                // Make sure you close the db to shutdown the
                // database and avoid resource leaks.
                db.close();
            }
        } catch (IOException ex) {
            logger.error("closeLDB: " + ex.toString(), ex);
        }
    }

    public void put(String key, String value) {
        try {
            if (key != null && !key.isEmpty() && value != null && !value.isEmpty()) {
                db.put(bytes(key), bytes(value));
            }
        } catch (Exception ex) {
            logger.error("put: ", ex);
        }
    }

    public void putBatch(Map<String, String> mapData) throws IOException {
        if (mapData != null && !mapData.isEmpty()) {
            WriteBatch batch = db.createWriteBatch();
            try {
                for (String key : mapData.keySet()) {
                    String value = mapData.get(key);
                    if (key != null && !key.isEmpty() && value != null && !value.isEmpty()) {
                        batch.put(bytes(key), bytes(value));
                    }
                }
                db.write(batch);
            } catch (Exception ex) {
                logger.error("putBatch: ", ex);
            } finally {
                // Make sure you close the batch to avoid resource leaks.
                if (batch != null) {
                    batch.close();
                }
            }
        }
    }

    public void putByte(byte[] key, byte[] value) {
        try {
            if (key != null && key.length > 0 && value != null && value.length > 0) {
                db.put(key, value);
            }
        } catch (Exception ex) {
            logger.error("putByte: ", ex);
        }
    }

    public void putBatchByte(Map<byte[], byte[]> mapData) throws IOException {
        if (mapData != null && !mapData.isEmpty()) {
            WriteBatch batch = db.createWriteBatch();
            try {
                for (byte[] key : mapData.keySet()) {
                    byte[] value = mapData.get(key);
                    if (key != null && key.length > 0 && value != null && value.length > 0) {
                        batch.put(key, value);
                    }
                }
                db.write(batch);
            } catch (Exception ex) {
                logger.error("putBatch: ", ex);
            } finally {
                // Make sure you close the batch to avoid resource leaks.
                if (batch != null) {
                    batch.close();
                }
            }
        }
    }

    public String get(String key) {
        try {
            if (key != null && !key.isEmpty()) {
                String rs = asString(db.get(bytes(key)));
                return rs;
            }
        } catch (Exception ex) {
            logger.error("get: ", ex);
        }
        return "";
    }

    public Map<String, String> getList(List<String> listKey) {
        Map<String, String> rs = new LinkedHashMap<>();
        if (listKey != null && !listKey.isEmpty()) {
            for (String key : listKey) {
                if (key != null && !key.isEmpty()) {
                    String value = asString(db.get(bytes(key)));
                    rs.put(key, value);
                }
            }
        }
        return rs;
    }

    public byte[] getByte(byte[] key) {
        try {
            if (key != null && key.length > 0) {
                byte[] rs = db.get(key);
                return rs;
            }
        } catch (Exception ex) {
            logger.error("getByte: ", ex);
        }
        return null;
    }

    public Map<byte[], byte[]> getListByte(List<byte[]> listKey) {
        Map<byte[], byte[]> rs = new LinkedHashMap<>();
        if (listKey != null && !listKey.isEmpty()) {
            for (byte[] key : listKey) {
                if (key != null && key.length > 0) {
                    byte[] value = db.get(key);
                    rs.put(key, value);
                }
            }
        }
        return rs;
    }

    public void delete(String key) {
        try {
            if (key != null && !key.isEmpty()) {
                db.delete(bytes(key));
            }
        } catch (Exception ex) {
            logger.error("delete: ", ex);
        }
    }

    public void deleteList(List<String> listKey) {
        if (listKey != null && !listKey.isEmpty()) {
            for (String key : listKey) {
                if (key != null && !key.isEmpty()) {
                    db.delete(bytes(key));
                }
            }
        }
    }

    public void deleteBatch(List<String> listKey) throws IOException {
        if (listKey != null && !listKey.isEmpty()) {
            WriteBatch batch = db.createWriteBatch();
            try {
                for (String key : listKey) {
                    if (key != null && !key.isEmpty()) {
                        batch.delete(bytes(key));
                    }
                }
                db.write(batch);
            } catch (Exception ex) {
                logger.error("deleteBatch: ", ex);
            } finally {
                // Make sure you close the batch to avoid resource leaks.
                if (batch != null) {
                    batch.close();
                }
            }
        }
    }

    public void deleteByte(byte[] key) {
        try {
            if (key != null && key.length > 0) {
                db.delete(key);
            }
        } catch (Exception ex) {
            logger.error("deleteByte: ", ex);
        }
    }

    public void deleteListByte(List<byte[]> listKey) {
        if (listKey != null && !listKey.isEmpty()) {
            for (byte[] key : listKey) {
                if (key != null && key.length > 0) {
                    db.delete(key);
                }
            }
        }
    }

    public void deleteBatchByte(List<byte[]> listKey) throws IOException {
        if (listKey != null && !listKey.isEmpty()) {
            WriteBatch batch = db.createWriteBatch();
            try {
                for (byte[] key : listKey) {
                    if (key != null && key.length > 0) {
                        batch.delete(key);
                    }
                }
                db.write(batch);
            } catch (Exception ex) {
                logger.error("deleteBatchByte: ", ex);
            } finally {
                // Make sure you close the batch to avoid resource leaks.
                if (batch != null) {
                    batch.close();
                }
            }
        }
    }
}
