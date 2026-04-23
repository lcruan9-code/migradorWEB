package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * SYSPDV (Firebird) → lc_sistemas.unidade
 *
 * Lê unidades únicas de PRODUTO.PROUNID do Firebird.
 * Insere apenas as que não existem já em lc_sistemas.unidade.
 * Salva mapa PROUNID→id em ctx para uso no ProdutoStep.
 */
public class UnidadeStep extends StepBase {

    @Override
    public String getNome() { return "UnidadeStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb = ctx.getOrigemConn();
        Connection my = ctx.getDestinoConn();

        exec(my, "DELETE FROM lc_sistemas.unidade WHERE id > 36");
        // execIgnore(my, "ALTER TABLE lc_sistemas.unidade AUTO_INCREMENT = 1", "reset AI unidade");

        // Unidades já existentes no destino
        Set<String> existentes = new HashSet<String>();
        Statement stMy0 = null;
        ResultSet rsMy0 = null;
        try {
            stMy0 = my.createStatement();
            rsMy0 = stMy0.executeQuery("SELECT descricao FROM lc_sistemas.unidade");
            while (rsMy0.next()) {
                String d = rsMy0.getString(1);
                if (d != null) existentes.add(d.trim().toUpperCase());
            }
        } catch (SQLException e) {
            LOG.warning("[UnidadeStep] Erro lendo unidades existentes: " + e.getMessage());
        } finally {
            close(rsMy0); close(stMy0);
        }

        int ins = 0;
        PreparedStatement pst = null;
        Statement stFb = null;
        ResultSet rs = null;
        try {
            stFb = fb.createStatement();
            rs = stFb.executeQuery(
                "SELECT DISTINCT PROUNID FROM PRODUTO WHERE CHAR_LENGTH(TRIM(PROUNID)) >= 1");

            pst = my.prepareStatement(
                "INSERT INTO unidade (descricao, nome, fator_conversao, datahora_alteracao, ativo) " +
                "VALUES (?, ?, '1.000', NOW(), '1')");

            while (rs.next()) {
                String prounid = rs.getString(1);
                if (prounid == null || prounid.trim().isEmpty()) continue;
                String und = prounid.trim();
                
                // Verifica se já existe por descrição (case insensitive)
                if (existentes.contains(und.toUpperCase())) continue;
                
                pst.setString(1, und);
                pst.setString(2, und);
                try { 
                    pst.executeUpdate(); 
                    ins++; 
                    existentes.add(und.toUpperCase()); 
                } catch (SQLException e) { 
                    // Loga erro mas continua
                    LOG.warning("[UnidadeStep] Erro ao inserir unidade '" + und + "': " + e.getMessage());
                }
            }
            my.commit();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo PROUNID do Firebird: " + e.getMessage(), e);
        } finally {
            close(rs); close(stFb); close(pst);
        }

        // Mapa completo PROUNID → id_unidade (Usado pelo ProdutoStep)
        Map<String, Integer> mapa = new HashMap<String, Integer>();
        Statement stMy = null;
        ResultSet rsMy = null;
        try {
            stMy = my.createStatement();
            rsMy = stMy.executeQuery("SELECT id, descricao FROM unidade");
            while (rsMy.next()) {
                String desc = rsMy.getString(2);
                if (desc != null) {
                    mapa.put(desc.trim().toUpperCase(), rsMy.getInt(1));
                }
            }
        } catch (SQLException e) {
            LOG.warning("[UnidadeStep] Erro lendo mapa unidade: " + e.getMessage());
        } finally {
            close(rsMy); close(stMy);
        }

        ctx.setMapaUnidade(mapa);
        contarInseridos(ctx, ins);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM unidade WHERE id > 36", "rollback UnidadeStep");
    }
}
