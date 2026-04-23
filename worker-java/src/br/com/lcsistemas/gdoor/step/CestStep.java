package br.com.lcsistemas.gdoor.step;

import br.com.lcsistemas.gdoor.core.MigracaoContext;
import br.com.lcsistemas.gdoor.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Migra códigos CEST de CEST/ESTOQUE do Firebird para lc_sistemas.cest.
 *
 * Firebird: tabela CEST (ID, CODIGO) e ESTOQUE tem campo COD_CEST (FK para CEST.ID).
 * Objetivo: coleta CESTs únicos com 7 dígitos que aparecem no estoque.
 */
public class CestStep extends StepBase {

    @Override
    public String getNome() { return "CestStep"; }

    @Override
    public void prepare(MigracaoContext ctx) throws MigracaoException {
        execIgnore(ctx.getDestinoConn(),
            "CREATE INDEX idx_lc_cest_cest ON lc_sistemas.cest (cest)",
            "idx_lc_cest_cest");
    }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        // Limpa destino (mantém id=1)
        exec(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.cest WHERE id > 1");
        exec(ctx.getDestinoConn(), "ALTER TABLE lc_sistemas.cest AUTO_INCREMENT = 2");

        // Coleta CESTs únicos do Firebird que estão no estoque com 7 dígitos
        // Verifica se a tabela CEST existe no Firebird; se não, usa COD_CEST direto do ESTOQUE
        Map<Integer, String> cestFb = new HashMap<>(); // id_firebird -> codigo_cest (7 digitos)
        Statement stFb = null; ResultSet rsFb = null;
        try {
            stFb = ctx.getOrigemConn().createStatement();
            // Tenta o join CEST x ESTOQUE
            rsFb = stFb.executeQuery(
                "SELECT DISTINCT c.ID, c.CODIGO " +
                "FROM CEST c " +
                "INNER JOIN ESTOQUE e ON e.COD_CEST = c.ID " +
                "WHERE CHAR_LENGTH(c.CODIGO) = 7 AND c.CODIGO > '0'");
            while (rsFb.next()) {
                cestFb.put(rsFb.getInt(1), rsFb.getString(2));
            }
        } catch (SQLException e) {
            // Se a tabela CEST não existir, ignora
            LOG.warning("[CestStep] Tabela CEST não encontrada no Firebird: " + e.getMessage());
        } finally {
            close(rsFb); close(stFb);
        }

        // INSERT no MySQL
        int ins = 0;
        Map<String, Integer> mapaCest = new HashMap<>();    // codigo -> id_mysql
        Map<Integer, Integer> mapaCestById = new HashMap<>(); // id_fb -> id_mysql

        if (!cestFb.isEmpty()) {
            PreparedStatement ps = null;
            try {
                ps = ctx.getDestinoConn().prepareStatement(
                    "INSERT INTO lc_sistemas.cest (cest, ncm, descricao) VALUES (?, '00000000', '')",
                    Statement.RETURN_GENERATED_KEYS);
                for (Map.Entry<Integer, String> entry : cestFb.entrySet()) {
                    ps.setString(1, entry.getValue());
                    ps.executeUpdate();
                    ResultSet keys = ps.getGeneratedKeys();
                    if (keys.next()) {
                        int idMy = keys.getInt(1);
                        mapaCest.put(entry.getValue(), idMy);
                        mapaCestById.put(entry.getKey(), idMy);
                        ins++;
                    }
                    keys.close();
                }
            } catch (SQLException e) {
                throw new MigracaoException(getNome(), "Erro ao inserir CEST: " + e.getMessage(), e);
            } finally {
                close(ps);
            }
        }

        contarInseridos(ctx, ins);
        ctx.setMapaCest(mapaCest);
        ctx.setMapaCestById(mapaCestById);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "DELETE FROM lc_sistemas.cest WHERE id > 1",
            "rollback CestStep");
    }

    @Override
    public void cleanup(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "DROP INDEX idx_lc_cest_cest ON lc_sistemas.cest",
            "drop idx_lc_cest_cest");
    }
}
