package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * SYSPDV (Firebird) → lc_sistemas.categoria
 *
 * Lê seções da tabela SECAO do Firebird (SECCOD, SECDES).
 * Salva mapa SECCOD→categoria_id em ctx para uso no ProdutoStep.
 */
public class CategoriaStep extends StepBase {

    @Override
    public String getNome() { return "CategoriaStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb = ctx.getOrigemConn();
        Connection my = ctx.getDestinoConn();

        exec(my, "DELETE FROM lc_sistemas.categoria WHERE id > 1");
        // execIgnore(my, "ALTER TABLE lc_sistemas.categoria AUTO_INCREMENT = 1", "reset AI categoria");

        int ins = 0;
        PreparedStatement pst = null;
        Statement stFb = null;
        ResultSet rs = null;
        // mapa SECCOD → id_categoria
        Map<String, Integer> mapa = new HashMap<String, Integer>();

        try {
            stFb = fb.createStatement();
            rs = stFb.executeQuery(
                "SELECT SECCOD, SECDES FROM SECAO " +
                "WHERE CHAR_LENGTH(TRIM(SECDES)) >= 1 AND SECCOD <> '00'");

            pst = my.prepareStatement(
                "INSERT INTO lc_sistemas.categoria(nome, comissao, pode_gourmet, datahora_alteracao, ativo) " +
                "VALUES (?, 0.000, 'SIM', NOW(), 1)");

            while (rs.next()) {
                String seccod = rs.getString(1);
                String secdes = rs.getString(2);
                if (secdes == null || secdes.trim().isEmpty()) continue;
                pst.setString(1, secdes.trim());
                try {
                    pst.executeUpdate();
                    ins++;
                } catch (SQLException e) { /* duplicado */ }
            }
            my.commit();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo SECAO do Firebird: " + e.getMessage(), e);
        } finally {
            close(rs); close(stFb); close(pst);
        }

        // Reconstrói mapa lendo de volta do MySQL: nome_secao → id
        // Depois cruza com SECAO do Firebird para montar SECCOD → id
        Map<String, Integer> mapaSecdes = new HashMap<String, Integer>();
        Statement stMy = null;
        ResultSet rsMy = null;
        try {
            stMy = my.createStatement();
            rsMy = stMy.executeQuery("SELECT id, nome FROM lc_sistemas.categoria");
            while (rsMy.next()) mapaSecdes.put(rsMy.getString(2).trim(), rsMy.getInt(1));
        } catch (SQLException e) {
            LOG.warning("[CategoriaStep] Erro lendo mapa categoria: " + e.getMessage());
        } finally {
            close(rsMy); close(stMy);
        }

        // Segunda passagem Firebird para montar SECCOD → id_categoria
        Statement stFb2 = null;
        ResultSet rs2 = null;
        try {
            stFb2 = fb.createStatement();
            rs2 = stFb2.executeQuery("SELECT SECCOD, SECDES FROM SECAO");
            while (rs2.next()) {
                String seccod = rs2.getString(1);
                String secdes = rs2.getString(2);
                if (secdes == null) continue;
                Integer id = mapaSecdes.get(secdes.trim());
                if (id != null && seccod != null) mapa.put(seccod.trim(), id);
            }
        } catch (SQLException e) {
            LOG.warning("[CategoriaStep] Erro montando mapa SECCOD: " + e.getMessage());
        } finally {
            close(rs2); close(stFb2);
        }

        ctx.setMapaCategoria(mapa);
        contarInseridos(ctx, ins);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.categoria WHERE id > 1", "rollback CategoriaStep");
    }
}
