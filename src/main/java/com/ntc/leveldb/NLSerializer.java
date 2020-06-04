/*
 * Copyright 2015 nghiatc.
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author nghiatc
 * @since Aug 20, 2015
 */
public class NLSerializer {

    private static final Logger logger = LoggerFactory.getLogger(NLSerializer.class);

    public byte[] serializeBool(boolean b) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            dos.writeBoolean(b);
            byte[] bb = bos.toByteArray();
            dos.close();
            bos.close();
            return bb;
        } catch (Exception ex) {
            logger.error("NLSerializer.serializeBool " + ex.toString(), ex);
        }
        return null;
    }

    public boolean deserializeBool(byte[] b) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(b, 0, b.length);
            DataInputStream dis = new DataInputStream(bis);
            boolean rs = dis.readBoolean();
            dis.close();
            bis.close();
            return rs;
        } catch (Exception ex) {
            logger.error("NLSerializer.deserializeBool " + ex.toString(), ex);
        }
        return false;
    }

    public byte[] serializeInt(int i) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            dos.writeInt(i);
            byte[] bb = bos.toByteArray();
            dos.close();
            bos.close();
            return bb;
        } catch (Exception ex) {
            logger.error("NLSerializer.serializeInt " + ex.toString(), ex);
        }
        return null;
    }

    public int deserializeInt(byte[] b) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(b, 0, b.length);
            DataInputStream dis = new DataInputStream(bis);
            int rs = dis.readInt();
            dis.close();
            bis.close();
            return rs;
        } catch (Exception ex) {
            logger.error("NLSerializer.deserializeInt " + ex.toString(), ex);
        }
        return 0;
    }

    public byte[] serializeLong(long l) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            dos.writeLong(l);
            byte[] bb = bos.toByteArray();
            dos.close();
            bos.close();
            return bb;
        } catch (Exception ex) {
            logger.error("NLSerializer.serializeLong " + ex.toString(), ex);
        }
        return null;
    }

    public long deserializeLong(byte[] b) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(b, 0, b.length);
            DataInputStream dis = new DataInputStream(bis);
            long rs = dis.readLong();
            dis.close();
            bis.close();
            return rs;
        } catch (Exception ex) {
            logger.error("NLSerializer.deserializeLong " + ex.toString(), ex);
        }
        return 0;
    }

    public byte[] serializeFloat(float f) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            dos.writeFloat(f);
            byte[] bb = bos.toByteArray();
            dos.close();
            bos.close();
            return bb;
        } catch (Exception ex) {
            logger.error("NLSerializer.serializeFloat " + ex.toString(), ex);
        }
        return null;
    }

    public float deserializeFloat(byte[] b) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(b, 0, b.length);
            DataInputStream dis = new DataInputStream(bis);
            float rs = dis.readFloat();
            dis.close();
            bis.close();
            return rs;
        } catch (Exception ex) {
            logger.error("NLSerializer.deserializeFloat " + ex.toString(), ex);
        }
        return 0;
    }

    public byte[] serializeDouble(double d) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            dos.writeDouble(d);
            byte[] bb = bos.toByteArray();
            dos.close();
            bos.close();
            return bb;
        } catch (Exception ex) {
            logger.error("NLSerializer.serializeDouble " + ex.toString(), ex);
        }
        return null;
    }

    public double deserializeDouble(byte[] b) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(b, 0, b.length);
            DataInputStream dis = new DataInputStream(bis);
            double rs = dis.readDouble();
            dis.close();
            bis.close();
            return rs;
        } catch (Exception ex) {
            logger.error("NLSerializer.deserializeDouble " + ex.toString(), ex);
        }
        return 0;
    }

    public byte[] serializeString(String s) {
        try {
            return s.getBytes("UTF-8");
        } catch (Exception ex) {
            logger.error("NLSerializer.serializeString " + ex.toString(), ex);
        }
        return null;
    }

    public String deserializeString(byte[] s) {
        try {
            return new String(s, "UTF-8");
        } catch (Exception ex) {
            logger.error("NLSerializer.deserializeString " + ex.toString(), ex);
        }
        return null;
    }
}
