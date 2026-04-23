import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class TestDdlParser {
    public static void main(String[] args) {
        Map<String, String> originalDdls = new HashMap<>();
        try (InputStream is = new FileInputStream("worker-java/src/br/com/lcsistemas/syspdv/resource/banco_novo.sql");
             BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            
            StringBuilder fullSql = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                fullSql.append(line).append("\n");
            }
            
            String sql = fullSql.toString();
            String[] parts = sql.split("DROP TABLE IF EXISTS `");
            System.out.println("Parts count: " + parts.length);
            for (int i = 1; i < parts.length; i++) {
                String part = parts[i];
                int endTableName = part.indexOf("`");
                if (endTableName > 0) {
                    String tableName = part.substring(0, endTableName).toLowerCase();
                    int createIndex = part.indexOf("CREATE TABLE");
                    if (createIndex != -1) {
                        int endCreate = part.indexOf(";", createIndex);
                        if (endCreate != -1) {
                            String ddl = "DROP TABLE IF EXISTS `" + tableName + part.substring(endTableName, endCreate + 1);
                            originalDdls.put(tableName, ddl);
                        }
                    }
                }
            }
            System.out.println("Parsed DDLs: " + originalDdls.size());
            System.out.println("Has produto? " + originalDdls.containsKey("produto"));
            System.out.println("Has grupotributacao? " + originalDdls.containsKey("grupotributacao"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
