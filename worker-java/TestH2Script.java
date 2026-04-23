import java.sql.*;

public class TestH2Script {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:h2:mem:test2;MODE=MySQL;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
        try (Connection conn = DriverManager.getConnection(url, "sa", "")) {
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE SCHEMA IF NOT EXISTS lc_sistemas");
                st.execute("SET SCHEMA lc_sistemas");
                st.execute("CREATE TABLE unidade (id INT PRIMARY KEY, nome VARCHAR(100))");
                
                System.out.println("== RUNNING SCRIPT ==");
                try (ResultSet rs = st.executeQuery("SCRIPT NOPASSWORDS NOSETTINGS TABLE unidade")) {
                    while (rs.next()) {
                        System.out.println(rs.getString(1));
                    }
                }
            }
        }
    }
}
