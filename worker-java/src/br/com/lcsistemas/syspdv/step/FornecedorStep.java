package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * SYSPDV (Firebird) → lc_sistemas.fornecedor
 *
 * Lê FORNECEDOR do Firebird. Resolve id_cidade/id_estado via FORCODIBGE contra
 * lc_sistemas.cidades (MySQL). Salva mapaFornecedor: FORCOD→id em ctx.
 *
 * Colunas Firebird: FORCOD, FORDES, FORFAN, FOREND, FORBAI, FORCEP, FORNUM,
 *                   FORCGC, FORCGF, FOREMAIL, FORTEL, FORFAX, FORCODIBGE
 */
public class FornecedorStep extends StepBase {

    @Override
    public String getNome() { return "FornecedorStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb  = ctx.getOrigemConn();
        Connection my  = ctx.getDestinoConn();
        int cidadeDef  = ctx.getConfig().getCidadeDefaultId();
        int estadoDef  = ctx.getConfig().getEstadoDefaultId();

        exec(my, "DELETE FROM lc_sistemas.fornecedor WHERE id > 2");
        // execIgnore(my, "ALTER TABLE lc_sistemas.fornecedor AUTO_INCREMENT = 1", "reset AI fornecedor");

        Map<Integer, int[]> mapaCidade = carregarMapaCidade(my, getNome());

        int ins = 0, err = 0;
        PreparedStatement pst = null;
        Statement stFb = null;
        ResultSet rs = null;
        Map<Integer, Integer> mapaFornecedor = new HashMap<Integer, Integer>();

        try {
            stFb = fb.createStatement();
            rs = stFb.executeQuery(
                "SELECT FORCOD, FORDES, FORFAN, FOREND, FORBAI, FORCEP, FORNUM, " +
                "FORCGC, FORCGF, FOREMAIL, FORTEL, FORFAX, FORCODIBGE FROM FORNECEDOR");

            pst = my.prepareStatement(
                "INSERT INTO lc_sistemas.fornecedor" +
                "(razao_social, nome, endereco, bairro, cep, numero, cnpj_cpf, ie," +
                " email_site, obs, id_cidade, id_estado, id_empresa) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1)",
                Statement.RETURN_GENERATED_KEYS);

            while (rs.next()) {
                int    forcod    = rs.getInt(1);
                String fordes    = rs.getString(2);
                if (forcod == 0 || fordes == null || fordes.trim().isEmpty()) continue;
                String forfan    = rs.getString(3);
                String forend    = rs.getString(4);
                String forbai    = rs.getString(5);
                String forcep    = rs.getString(6);
                String fornum    = rs.getString(7);
                String forcgc    = rs.getString(8);
                String forcgf    = rs.getString(9);
                String foremail  = rs.getString(10);
                String fortel    = rs.getString(11);
                String forfax    = rs.getString(12);
                int    forcodibge = rs.getInt(13);

                int[] cid = mapaCidade.get(forcodibge);
                int idCidade = (cid != null) ? cid[0] : cidadeDef;
                int idEstado = (cid != null) ? cid[1] : estadoDef;

                String nome = (forfan != null && !forfan.trim().isEmpty()) ? forfan.trim() : fordes.trim();
                String obs = "TELEFONE: " + (fortel != null ? fortel : "") +
                             "\nFAX: " + (forfax != null ? forfax : "");

                try {
                    pst.setString(1, fordes.trim());
                    pst.setString(2, nome);
                    pst.setString(3, forend != null ? forend.trim() : "");
                    pst.setString(4, forbai != null ? forbai.trim() : "");
                    pst.setString(5, forcep != null ? forcep.trim() : "");
                    pst.setString(6, fornum != null ? fornum.trim() : "");
                    pst.setString(7, forcgc != null ? forcgc.trim() : "");
                    pst.setString(8, forcgf != null ? forcgf.trim() : "");
                    pst.setString(9, foremail != null ? foremail.trim() : "");
                    pst.setString(10, obs);
                    pst.setInt(11, idCidade);
                    pst.setInt(12, idEstado);
                    pst.executeUpdate();
                    ResultSet gk = pst.getGeneratedKeys();
                    if (gk.next()) mapaFornecedor.put(forcod, gk.getInt(1));
                    close(gk);
                    ins++;
                } catch (SQLException e) {
                    LOG.warning("[FornecedorStep] Erro forcod=" + forcod + ": " + e.getMessage());
                    err++;
                }
                if ((ins + err) % 200 == 0) {
                    try { my.commit(); } catch (SQLException ex) { /* ignora */ }
                }
            }
            my.commit();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo FORNECEDOR do Firebird: " + e.getMessage(), e);
        } finally {
            close(rs); close(stFb); close(pst);
        }

        ctx.setMapaFornecedor(mapaFornecedor);
        contarInseridos(ctx, ins);
        contarErros(ctx, err);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.fornecedor WHERE id > 2", "rollback FornecedorStep");
    }
}
