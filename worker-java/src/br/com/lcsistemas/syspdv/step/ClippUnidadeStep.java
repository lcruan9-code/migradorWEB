package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Clipp (Firebird) → lc_sistemas.unidade
 * Lê TB_UNI_MEDIDA.UNIDADE.
 */
public class ClippUnidadeStep extends StepBase {

    @Override public String getNome() { return "UnidadeStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb = ctx.getOrigemConn();
        Connection my = ctx.getDestinoConn();

        exec(my, "DELETE FROM lc_sistemas.unidade WHERE id > 36");
        // execIgnore(my, "ALTER TABLE lc_sistemas.unidade AUTO_INCREMENT = 1", "reset AI unidade");

        Set<String> existentes = new HashSet<>();
        Statement stMy0 = null; ResultSet rsMy0 = null;
        try {
            stMy0 = my.createStatement();
            rsMy0 = stMy0.executeQuery("SELECT descricao FROM lc_sistemas.unidade");
            while (rsMy0.next()) { String d = rsMy0.getString(1); if (d != null) existentes.add(d.trim().toUpperCase()); }
        } catch (SQLException e) { LOG.warning("[ClippUnidadeStep] " + e.getMessage()); }
        finally { close(rsMy0); close(stMy0); }

        int ins = 0;
        PreparedStatement pst = null; Statement stFb = null; ResultSet rs = null;
        try {
            stFb = fb.createStatement();
            rs = stFb.executeQuery("SELECT DISTINCT UNIDADE FROM TB_UNI_MEDIDA WHERE CHAR_LENGTH(TRIM(UNIDADE)) >= 1 AND STATUS <> 'I'");
            pst = my.prepareStatement(
                "INSERT INTO lc_sistemas.unidade (descricao, nome, fator_conversao, datahora_alteracao, ativo) " +
                "VALUES (?, ?, '1.000', NOW(), '1')");
            while (rs.next()) {
                String und = rs.getString(1);
                if (und == null || und.trim().isEmpty()) continue;
                und = und.trim();
                if (existentes.contains(und.toUpperCase())) continue;
                pst.setString(1, und); pst.setString(2, und);
                try { pst.executeUpdate(); ins++; existentes.add(und.toUpperCase()); } catch (SQLException e) { /* dup */ }
            }
            my.commit();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo TB_UNI_MEDIDA: " + e.getMessage(), e);
        } finally { close(rs); close(stFb); close(pst); }

        Map<String, Integer> mapa = new HashMap<>();
        Statement stMy = null; ResultSet rsMy = null;
        try {
            stMy = my.createStatement();
            rsMy = stMy.executeQuery("SELECT id, descricao FROM lc_sistemas.unidade");
            while (rsMy.next()) mapa.put(rsMy.getString(2).trim(), rsMy.getInt(1));
        } catch (SQLException e) { LOG.warning("[ClippUnidadeStep] mapa: " + e.getMessage()); }
        finally { close(rsMy); close(stMy); }

        ctx.setMapaUnidade(mapa);
        contarInseridos(ctx, ins);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.unidade WHERE id > 36", "rollback ClippUnidadeStep");
    }
}
