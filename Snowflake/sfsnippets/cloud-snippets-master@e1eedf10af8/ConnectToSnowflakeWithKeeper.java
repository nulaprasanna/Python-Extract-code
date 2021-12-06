package com.cisco.random;

import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Base64;
import java.util.HashMap;
import java.util.Properties;
import com.bettercloud.vault.json.Json;
import com.bettercloud.vault.json.JsonObject;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 *  POM Dependencies
 *          <dependency>
 *             <groupId>com.bettercloud</groupId>
 *             <artifactId>vault-java-driver</artifactId>
 *             <version>4.0.0</version>
 *         </dependency>
 *
 *         <dependency>
 *             <groupId>net.snowflake</groupId>
 *             <artifactId>snowflake-jdbc</artifactId>
 *             <version>3.6.4</version>
 *         </dependency>
 */

public class ConnectToSnowflakeWithKeeper {

    // Replace with your values
    final static private String userName = "PLATFORM_OPS_SVC";
    final static private String accountName = "ciscodev";
    final static private String accountURL = "jdbc:snowflake://ciscodev.snowflakecomputing.com/?role=PLATFORM_OPS_ROLE&warehouse=PLATFORM_OPS_WH";
    final static private String keeperToken = "s.XiDc....";
    final static private String keeperPath = "secret/snowflake/dev/platform_ops_svc/key";

    public static void main(String[] args) {
        try {
            HashMap<String, String> credentials = fetchKeeperCredentials(keeperToken,keeperPath);
            Properties prop = createSnowflakeProperties(credentials);
            openDatabaseConnectionWithPEM(prop);
            close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Connection connection = null;

    // Open a database connection with the collected properties file
    public static void openDatabaseConnectionWithPEM(Properties properties) throws SFMetaDataException {
        try {
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
            String connectionString = properties.getProperty("url");
            connection = DriverManager.getConnection(connectionString, properties);
            connection.setAutoCommit(false);
        } catch (Exception e) {
            throw new SFMetaDataException(e);
        }
    }

    /**
     * Perform a commit and then close the Snowflake connection.
     */
    public static void close() {
        try {
            if (connection != null) {
                connection.commit();
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // For the provided path and token, collect the Keeper secrets.
    public static HashMap<String, String> fetchKeeperCredentials(String keeperToken, String keeperPath) {
        HashMap<String, String> credentials = new HashMap<String, String>();

        ProcessBuilder process = new ProcessBuilder(
                "curl"
                , "-H"
                , "X-Vault-Namespace:cloudDB"
                , "-H"
                , "X-Vault-Token:" + keeperToken
                , "https://east.keeper.cisco.com/v1/" + keeperPath);

        Process p;

        try {
            p = process.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            StringBuilder builder = new StringBuilder();
            String line = null;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
                builder.append(System.getProperty("line.separator"));
            }
            String result = builder.toString();
            JsonObject jsonObject = Json.parse(result).asObject();
            final JsonObject dataJsonObject = jsonObject.get("data").asObject();

            // Note, we are storing the key and passphrase in these key values.
            credentials.put("SNOWSQL_PRIVATE_KEY_PASSPHRASE",dataJsonObject.getString("SNOWSQL_PRIVATE_KEY_PASSPHRASE", ""));
            credentials.put("private_key",dataJsonObject.getString("private_key", ""));
        } catch (IOException e) {
            System.out.print("error");
        }

        return credentials;
    }


    // Prepare the Snowflake properties file by encoding the key and adding the properties
    public static Properties createSnowflakeProperties(HashMap<String, String> credentials) throws SFMetaDataException {
        Properties prop = new Properties();

        try {
            byte[] keyBytes = credentials.get("private_key").getBytes(Charset.forName("UTF-8"));
            String encrypted = new String(keyBytes);
            String passphrase = credentials.get("SNOWSQL_PRIVATE_KEY_PASSPHRASE");
            encrypted = encrypted.replace("-----BEGIN ENCRYPTED PRIVATE KEY-----", "");
            encrypted = encrypted.replace("-----END ENCRYPTED PRIVATE KEY-----", "").trim();
            EncryptedPrivateKeyInfo pkInfo = new EncryptedPrivateKeyInfo(Base64.getMimeDecoder().decode(encrypted));
            PBEKeySpec keySpec = new PBEKeySpec(passphrase.toCharArray());
            SecretKeyFactory pbeKeyFactory = SecretKeyFactory.getInstance(pkInfo.getAlgName());
            PKCS8EncodedKeySpec encodedKeySpec = pkInfo.getKeySpec(pbeKeyFactory.generateSecret(keySpec));
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            PrivateKey encryptedPrivateKey = keyFactory.generatePrivate(encodedKeySpec);

            prop.put("url", accountURL);
            prop.put("user", userName);
            prop.put("account", accountName);
            prop.put("privateKey", encryptedPrivateKey);

        } catch (Exception e) {
            throw new SFMetaDataException(e);
        }

        return prop;
    }

}

// Build an exception handler (Optional)
class SFMetaDataException extends Exception {

    private String message = null;

    public SFMetaDataException() {
        super();
    }

    public SFMetaDataException(String message) {
        super(message);
        this.message = message;
    }

    public SFMetaDataException(Throwable cause) {
        super(cause);
    }

    @Override
    public String toString() {
        return message;
    }

    @Override
    public String getMessage() {
        return message;
    }
}