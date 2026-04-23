package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * SYSPDV (Firebird) → lc_sistemas.subcategoria
 *
 * Lê grupos da tabela GRUPO do Firebird (GRPCOD, GRPDES, SECCOD).
 * Salva mapa composto SECCOD+"|"+GRPCOD → subcategoria_id em ctx.
 *
 * Chave composta necessária pois GRPCOD pode se repetir entre seções.
 */
public class SubcategoriaStep extends StepBase {

    @Override
    public String getNome() { return "SubcategoriaStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb = ctx.getOrigemConn();
        Connection my = ctx.getDestinoConn();

        exec(my, "DELETE FROM lc_sistemas.subcategoria WHERE id > 1");
        // execIgnore(my, "ALTER TABLE lc_sistemas.subcategoria AUTO_INCREMENT = 1", "reset AI subcategoria");

        int ins = 0;
        PreparedStatement pst = null;
        Statement stFb = null;
        ResultSet rs = null;
        // mapa "SECCOD|GRPCOD" → id_subcategoria
        Map<String, Integer> mapa = new HashMap<String, Integer>();

        try {
            stFb = fb.createStatement();
            rs = stFb.executeQuery(
                "SELECT GRPCOD, GRPDES, SECCOD FROM GRUPO WHERE GRPDES <> 'GRUPO PADRAO'");

            pst = my.prepareStatement(
                "INSERT INTO lc_sistemas.subcategoria(id_categoria, nome, datahora_alteracao, ativo) " +
                "VALUES (1, ?, NOW(), 1)");

            while (rs.next()) {
                String grpcod = rs.getString(1);
                String grpdes = rs.getString(2);
                String seccod = rs.getString(3);
                if (grpdes == null || grpdes.trim().isEmpty()) continue;
                pst.setString(1, grpdes.trim());
                try { pst.executeUpdate(); ins++; }
                catch (SQLException e) { /* duplicado */ }
            }
            my.commit();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo GRUPO do Firebird: " + e.getMessage(), e);
        } finally {
            close(rs); close(stFb); close(pst);
        }

        // Mapa grpdes → id_subcategoria no MySQL
        Map<String, Integer> mapaGrpdes = new HashMap<String, Integer>();
        Statement stMy = null;
        ResultSet rsMy = null;
        try {
            stMy = my.createStatement();
            rsMy = stMy.executeQuery("SELECT id, nome FROM lc_sistemas.subcategoria");
            while (rsMy.next()) mapaGrpdes.put(rsMy.getString(2).trim(), rsMy.getInt(1));
        } catch (SQLException e) {
            LOG.warning("[SubcategoriaStep] Erro lendo mapa subcategoria: " + e.getMessage());
        } finally {
            close(rsMy); close(stMy);
        }

        // Segunda passagem Firebird: SECCOD|GRPCOD → id_subcategoria
        Statement stFb2 = null;
        ResultSet rs2 = null;
        try {
            stFb2 = fb.createStatement();
            rs2 = stFb2.executeQuery("SELECT GRPCOD, GRPDES, SECCOD FROM GRUPO");
            while (rs2.next()) {
                String grpcod = rs2.getString(1);
                String grpdes = rs2.getString(2);
                String seccod = rs2.getString(3);
                if (grpdes == null || grpcod == null || seccod == null) continue;
                Integer id = mapaGrpdes.get(grpdes.trim());
                if (id != null) mapa.put(seccod.trim() + "|" + grpcod.trim(), id);
            }
        } catch (SQLException e) {
            LOG.warning("[SubcategoriaStep] Erro montando mapa GRPCOD: " + e.getMessage());
        } finally {
            close(rs2); close(stFb2);
        }

        ctx.put("mapaSubcategoria", mapa);
        contarInseridos(ctx, ins);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.subcategoria WHERE id > 1", "rollback SubcategoriaStep");
    }
}
