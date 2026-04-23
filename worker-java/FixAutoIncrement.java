import java.io.*;
import java.nio.file.*;
import java.util.stream.*;

public class FixAutoIncrement {
    public static void main(String[] args) throws Exception {
        Path startDir = Paths.get("c:/Users/ruanp/NETBENAS - COWORK/migrador-web/worker-java/src/br/com/lcsistemas");
        Files.walk(startDir)
             .filter(Files::isRegularFile)
             .filter(p -> p.toString().endsWith(".java"))
             .forEach(p -> {
                 try {
                     String content = new String(Files.readAllBytes(p), "UTF-8");
                     if (content.contains("AUTO_INCREMENT = 1\"")) {
                         String newContent = content.replaceAll("(?m)^(\\s*)(exec.*AUTO_INCREMENT = 1.*)$", "$1// $2");
                         if (!content.equals(newContent)) {
                             Files.write(p, newContent.getBytes("UTF-8"));
                             System.out.println("Fixed: " + p.getFileName());
                         }
                     }
                 } catch (Exception e) {
                     e.printStackTrace();
                 }
             });
    }
}
