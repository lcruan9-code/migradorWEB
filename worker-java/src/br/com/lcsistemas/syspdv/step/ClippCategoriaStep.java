package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Clipp (Firebird) → lc_sistemas.categoria
 * Lê TB_EST_GRUPO. Salva mapa ID_GRUPO(int) → id_categoria em ctx.
 */
public class ClippCategoriaStep extends StepBase {

    @Override public String getNome() { return "CategoriaStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb = ctx.getOrigemConn();
        Connection my = ctx.getDestinoConn();

        exec(my, "DELETE FROM lc_sistemas.categoria WHERE id > 1");
        // execIgnore(my, "ALTER TABLE lc_sistemas.categoria AUTO_INCREMENT = 1", "reset AI categoria");

        int ins = 0;
        Map<Integer, Integer> mapaGrupoId = new HashMap<>();
        PreparedStatement pst = null; Statement stFb = null; ResultSet rs = null;
        try {
            stFb = fb.createStatement();
            rs = stFb.executeQuery("SELECT ID_GRUPO, DESCRICAO FROM TB_EST_GRUPO WHERE CHAR_LENGTH(TRIM(DESCRICAO)) >= 1");
            pst = my.prepareStatement(
                "INSERT INTO lc_sistemas.categoria(nome, comissao, pode_gourmet, datahora_alteracao, ativo) VALUES (?,0.000,'SIM',NOW(),1)",
                Statement.RETURN_GENERATED_KEYS);
            while (rs.next()) {
                int    idGrupo = rs.getInt(1);
                String descr   = rs.getString(2);
                if (descr == null || descr.trim().isEmpty()) continue;
                pst.setString(1, descr.trim());
                try {
                    pst.executeUpdate();
                    ResultSet gk = pst.getGeneratedKeys();
                    if (gk.next()) mapaGrupoId.put(idGrupo, gk.getInt(1));
                    close(gk);
                    ins++;
                } catch (SQLException e) { LOG.warning("[ClippCategoriaStep] " + e.getMessage()); }
            }
            my.commit();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo TB_EST_GRUPO: " + e.getMessage(), e);
        } finally { close(rs); close(stFb); close(pst); }

        // Salva como mapa de String para compatibilidade com outros steps (chave = String do id)
        Map<String, Integer> mapaCateg = new HashMap<>();
        for (Map.Entry<Integer, Integer> e : mapaGrupoId.entrySet())
            mapaCateg.put(String.valueOf(e.getKey()), e.getValue());
        ctx.setMapaCategoria(mapaCateg);
        ctx.put("mapaGrupoIdClipp", mapaGrupoId);
        contarInseridos(ctx, ins);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.categoria WHERE id > 1", "rollback ClippCategoriaStep");
    }
}
