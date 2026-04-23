package br.com.lcsistemas.gdoor.versao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Logger;

/**
 * Detecta automaticamente a versão do Firebird em execução.
 *
 * <p>Executa a consulta de sistema do próprio Firebird:
 * {@code SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION') FROM rdb$database}
 *
 * <p>Regras de segurança:
 * <ul>
 *   <li>Nunca lança erro fatal — qualquer falha resulta em {@link VersaoFirebird#DESCONHECIDA}.</li>
 *   <li>Nunca modifica o estado do banco.</li>
 *   <li>Compatível com todas as versões do Jaybird (2.x / 3.x / 4.x).</li>
 * </ul>
 *
 * Uso:
 * <pre>
 *   VersaoFirebird versao = DetectorFirebird.detectar(origemConn);
 * </pre>
 */
public final class DetectorFirebird {

    private static final Logger LOG = Logger.getLogger(DetectorFirebird.class.getName());

    private static final String SQL_VERSAO =
        "SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION') FROM rdb$database";

    /** Classe utilitária — não instanciar. */
    private DetectorFirebird() {}

    /**
     * Detecta a versão do Firebird a partir de uma conexão JDBC já aberta.
     *
     * @param conn Conexão ativa com o banco Firebird (origem).
     * @return {@link VersaoFirebird} mapeada, ou {@link VersaoFirebird#DESCONHECIDA} em falha.
     */
    public static VersaoFirebird detectar(Connection conn) {
        if (conn == null) {
            LOG.warning("[DetectorFirebird] Conexão nula — retornando DESCONHECIDA.");
            return VersaoFirebird.DESCONHECIDA;
        }

        Statement stmt = null;
        ResultSet rs   = null;

        try {
            stmt = conn.createStatement();
            rs   = stmt.executeQuery(SQL_VERSAO);

            if (rs.next()) {
                String versaoStr = rs.getString(1);
                if (versaoStr != null && !versaoStr.trim().isEmpty()) {
                    VersaoFirebird versao = mapear(versaoStr.trim());
                    LOG.info("[DetectorFirebird] Versão detectada: " + versaoStr.trim()
                           + " → " + versao);
                    return versao;
                }
            }

            LOG.warning("[DetectorFirebird] Query retornou vazio — retornando DESCONHECIDA.");

        } catch (Exception e) {
            LOG.warning("[DetectorFirebird] Falha ao detectar versão: " + e.getMessage()
                      + " — retornando DESCONHECIDA.");
        } finally {
            fechar(rs, stmt);
        }

        return VersaoFirebird.DESCONHECIDA;
    }

    // ── Mapeamento da string de versão para o enum ────────────────────────────

    private static VersaoFirebird mapear(String versaoStr) {
        if (versaoStr.startsWith("2.5")) return VersaoFirebird.FB25;
        if (versaoStr.startsWith("2."))  return VersaoFirebird.FB25; // fallback 2.x → FB25
        if (versaoStr.startsWith("3."))  return VersaoFirebird.FB30;
        if (versaoStr.startsWith("4."))  return VersaoFirebird.FB40;
        if (versaoStr.startsWith("5."))  return VersaoFirebird.FB50;
        return VersaoFirebird.DESCONHECIDA;
    }

    // ── Helper de fechamento seguro ───────────────────────────────────────────

    private static void fechar(ResultSet rs, Statement stmt) {
        if (rs != null)   try { rs.close();   } catch (Exception ignored) {}
        if (stmt != null) try { stmt.close(); } catch (Exception ignored) {}
    }
}
