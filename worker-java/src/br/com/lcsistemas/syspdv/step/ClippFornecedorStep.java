package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Clipp (Firebird) → lc_sistemas.fornecedor
 * TB_FORNECEDOR.ID_CIDADE = código IBGE (VARCHAR) → lc_sistemas.cidades.codigocidade.
 */
public class ClippFornecedorStep extends StepBase {

    @Override public String getNome() { return "FornecedorStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb  = ctx.getOrigemConn();
        Connection my  = ctx.getDestinoConn();
        int cidadeDef  = ctx.getConfig().getCidadeDefaultId();
        int estadoDef  = ctx.getConfig().getEstadoDefaultId();

        exec(my, "DELETE FROM lc_sistemas.fornecedor WHERE id > 2");
        // execIgnore(my, "ALTER TABLE lc_sistemas.fornecedor AUTO_INCREMENT = 1", "reset AI fornecedor");

        Map<Integer, int[]>    mapaCidade         = carregarMapaCidade(my, getNome());
        Map<Integer, String[]> mapaClippMunicipio = carregarMunicipioClipp(fb, getNome());

        int ins = 0, err = 0;
        PreparedStatement pst = null; Statement stFb = null; ResultSet rs = null;
        Map<Integer, Integer> mapaFornecedor = new HashMap<>();
        try {
            stFb = fb.createStatement();
            rs = stFb.executeQuery(
                "SELECT ID_FORNEC, NOME, NOME_FANTA, END_LOGRAD, END_BAIRRO, END_CEP, END_NUMERO, " +
                "CNPJ, INSC_ESTAD, EMAIL_CONT, DDD_COMER, FONE_COMER, DDD_CELUL, FONE_CELUL, ID_CIDADE " +
                "FROM TB_FORNECEDOR WHERE ID_FORNEC > 0");
            pst = my.prepareStatement(
                "INSERT INTO lc_sistemas.fornecedor" +
                "(razao_social, nome, endereco, bairro, cep, numero, cnpj_cpf, ie," +
                " email_site, obs, id_cidade, id_estado, id_empresa) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1)",
                Statement.RETURN_GENERATED_KEYS);
            while (rs.next()) {
                int    id      = rs.getInt(1);
                String nome    = rs.getString(2);
                if (nome == null || nome.trim().isEmpty()) continue;
                String fanta   = rs.getString(3);
                String end     = rs.getString(4);
                String bai     = rs.getString(5);
                String cep     = rs.getString(6);
                String num     = rs.getString(7);
                String cnpj    = rs.getString(8);
                String ie      = rs.getString(9);
                String email   = rs.getString(10);
                String dddCom  = rs.getString(11);
                String foneCom = rs.getString(12);
                String dddCel  = rs.getString(13);
                String foneCel = rs.getString(14);
                String cidIbge = rs.getString(15);

                int[] cid    = resolverCidade(cidIbge, mapaClippMunicipio, mapaCidade, cidadeDef, estadoDef);
                int idCidade = cid[0];
                int idEstado = cid[1];

                String exibir = (fanta != null && !fanta.trim().isEmpty()) ? fanta.trim() : nome.trim();
                String tel = (dddCom != null ? dddCom.trim() : "") + (foneCom != null ? foneCom.trim() : "");
                String cel = (dddCel != null ? dddCel.trim() : "") + (foneCel != null ? foneCel.trim() : "");
                String obs = "TEL: " + tel + (cel.isEmpty() ? "" : " | CEL: " + cel);

                try {
                    pst.setString(1, nome.trim()); pst.setString(2, exibir);
                    pst.setString(3, end   != null ? end.trim()  : "");
                    pst.setString(4, bai   != null ? bai.trim()  : "");
                    pst.setString(5, cep   != null ? cep.trim()  : "");
                    pst.setString(6, num   != null ? num.trim()  : "");
                    pst.setString(7, cnpj  != null ? cnpj.trim() : "");
                    pst.setString(8, ie    != null ? ie.trim()   : "");
                    pst.setString(9, email != null ? email.trim(): "");
                    pst.setString(10, obs);
                    pst.setInt(11, idCidade); pst.setInt(12, idEstado);
                    pst.executeUpdate();
                    ResultSet gk = pst.getGeneratedKeys();
                    if (gk.next()) mapaFornecedor.put(id, gk.getInt(1));
                    close(gk); ins++;
                } catch (SQLException e) {
                    LOG.warning("[ClippFornecedorStep] id=" + id + ": " + e.getMessage()); err++;
                }
                if ((ins + err) % 200 == 0) { try { my.commit(); } catch (SQLException ex) {} }
            }
            my.commit();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo TB_FORNECEDOR: " + e.getMessage(), e);
        } finally { close(rs); close(stFb); close(pst); }

        ctx.setMapaFornecedor(mapaFornecedor);
        contarInseridos(ctx, ins); contarErros(ctx, err);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.fornecedor WHERE id > 2", "rollback ClippFornecedorStep");
    }
}
