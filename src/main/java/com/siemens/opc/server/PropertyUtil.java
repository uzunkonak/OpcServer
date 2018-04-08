package com.siemens.opc.server;

import java.io.*;
import java.util.Properties;

public final class PropertyUtil {
    public static Properties readPropertiesFromFile(String filePath) {
        Properties properties = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream(filePath);

            properties.load(input);

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return properties;
    }
}
