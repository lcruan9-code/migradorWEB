package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;

/**
 * SYSPDV (Firebird) → lc_sistemas.fabricante
 *
 * Lê fabricantes da tabela FABRICANTE do Firebird (FABDES).
 * Sem mapa necessário — produto SYSPDV não referencia fabricante diretamente.
 */
public class FabricanteStep extends StepBase {

    @Override
    public String getNome() { return "FabricanteStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb = ctx.getOrigemConn();
        Connection my = ctx.getDestinoConn();

        exec(my, "DELETE FROM lc_sistemas.fabricante WHERE id > 1");
        // execIgnore(my, "ALTER TABLE lc_sistemas.fabricante AUTO_INCREMENT = 1", "reset AI fabricante");

        int ins = 0;
        PreparedStatement pst = null;
        Statement stFb = null;
        ResultSet rs = null;
        try {
            stFb = fb.createStatement();
            rs = stFb.executeQuery("SELECT FABDES FROM FABRICANTE WHERE CHAR_LENGTH(TRIM(FABDES)) >= 1");

            pst = my.prepareStatement(
                "INSERT INTO lc_sistemas.fabricante(nome, datahora_alteracao, ativo) VALUES (?, NOW(), 1)");

            while (rs.next()) {
                String fabdes = rs.getString(1);
                if (fabdes == null || fabdes.trim().isEmpty()) continue;
                pst.setString(1, fabdes.trim());
                try { pst.executeUpdate(); ins++; }
                catch (SQLException e) { /* duplicado */ }
            }
            my.commit();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo FABRICANTE do Firebird: " + e.getMessage(), e);
        } finally {
            close(rs); close(stFb); close(pst);
        }

        contarInseridos(ctx, ins);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.fabricante WHERE id > 1", "rollback FabricanteStep");
    }
}
