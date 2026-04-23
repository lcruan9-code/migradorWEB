package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Clipp (Firebird) → lc_sistemas.subcategoria
 * Lê TB_EST_SUBGRUPO. Salva mapa ID_SUBGRUPO(int) → id_subcategoria em ctx.
 */
public class ClippSubcategoriaStep extends StepBase {

    @Override public String getNome() { return "SubcategoriaStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb = ctx.getOrigemConn();
        Connection my = ctx.getDestinoConn();

        exec(my, "DELETE FROM lc_sistemas.subcategoria WHERE id > 1");
        // execIgnore(my, "ALTER TABLE lc_sistemas.subcategoria AUTO_INCREMENT = 1", "reset AI subcategoria");

        @SuppressWarnings("unchecked")
        Map<Integer, Integer> mapaGrupoId = (Map<Integer, Integer>) ctx.get("mapaGrupoIdClipp");
        if (mapaGrupoId == null) mapaGrupoId = new HashMap<>();

        int ins = 0;
        Map<Integer, Integer> mapaSubId = new HashMap<>();
        PreparedStatement pst = null; Statement stFb = null; ResultSet rs = null;
        try {
            stFb = fb.createStatement();
            rs = stFb.executeQuery(
                "SELECT ID_SUBGRUPO, DESCRICAO, ID_GRUPO FROM TB_EST_SUBGRUPO WHERE CHAR_LENGTH(TRIM(DESCRICAO)) >= 1");
            pst = my.prepareStatement(
                "INSERT INTO lc_sistemas.subcategoria(nome, id_categoria, datahora_alteracao, ativo) VALUES (?,?,NOW(),1)",
                Statement.RETURN_GENERATED_KEYS);
            while (rs.next()) {
                int    idSub   = rs.getInt(1);
                String descr   = rs.getString(2);
                int    idGrupo = rs.getInt(3);
                if (descr == null || descr.trim().isEmpty()) continue;
                Integer idCateg = mapaGrupoId.get(idGrupo);
                pst.setString(1, descr.trim());
                if (idCateg != null) pst.setInt(2, idCateg); else pst.setNull(2, java.sql.Types.INTEGER);
                try {
                    pst.executeUpdate();
                    ResultSet gk = pst.getGeneratedKeys();
                    if (gk.next()) mapaSubId.put(idSub, gk.getInt(1));
                    close(gk);
                    ins++;
                } catch (SQLException e) { LOG.warning("[ClippSubcategoriaStep] " + e.getMessage()); }
            }
            my.commit();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo TB_EST_SUBGRUPO: " + e.getMessage(), e);
        } finally { close(rs); close(stFb); close(pst); }

        ctx.put("mapaSubIdClipp", mapaSubId);
        // Compatível com ProdutoStep legado (String key)
        Map<String, Integer> mapaSub = new HashMap<>();
        for (Map.Entry<Integer, Integer> e : mapaSubId.entrySet())
            mapaSub.put(String.valueOf(e.getKey()), e.getValue());
        ctx.put("mapaSubcategoria", mapaSub);
        contarInseridos(ctx, ins);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.subcategoria WHERE id > 1", "rollback ClippSubcategoriaStep");
    }
}
