package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.Map;

/**
 * SYSPDV (Firebird) -> lc_sistemas.pagar
 *
 * Le CONTAPAGAR do Firebird. Filtra ctptippag = 'A'.
 * Usa mapaFornecedor para resolucao de FK.
 *
 * Colunas Firebird: CTPNUM, CTPDATEMI, CTPDATVNC, CTPVLRNOM, CTPOBS,
 *   CTPTIPPAG, FORCOD
 */
public class PagarStep extends StepBase {

    @Override
    public String getNome() { return "PagarStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb = ctx.getOrigemConn();
        Connection my = ctx.getDestinoConn();

        exec(my, "DELETE FROM lc_sistemas.pagar");
        // execIgnore(my, "ALTER TABLE lc_sistemas.pagar AUTO_INCREMENT = 1", "reset AI pagar");

        Map<Integer, Integer> mapaFornecedor = ctx.getMapaFornecedor();

        int ins = 0, ign = 0, err = 0;
        PreparedStatement pst = null;
        Statement stFb = null; ResultSet rs = null;
        try {
            stFb = fb.createStatement();
            rs = stFb.executeQuery(
                "SELECT CTPNUM, CTPDATEMI, CTPDATVNC, CTPVLRNOM, CTPOBS, FORCOD " +
                "FROM CONTAPAGAR WHERE CTPTIPPAG = 'A'");

            pst = my.prepareStatement(
                "INSERT INTO lc_sistemas.pagar(" +
                "id_usuario,id_empresa,id_pagamento,id_planocontas,id_fornecedor," +
                "id_contamovimento,tipo,status,documento,numero_nf," +
                "lancamento,emissao,vencimento,parcela," +
                "valor_original,valor,valor_desc,valor_pag,data_pag," +
                "juros_pag,multa_pag,multa,juros,agencia,conta,conciliado," +
                "numero_cheque,numero_boleto,historico,valor_arec,obs)" +
                " VALUES (1,1,1,38,?,1,'FP','CA',?,NULL,?,?,?,'1/1',?,?,0,0,NULL,0,0,0,0,'','',NULL,'','',?,0,?)");

            while (rs.next()) {
                String ctpnum   = rs.getString(1);
                Date   datemi   = rs.getDate(2);
                Date   datvnc   = rs.getDate(3);
                double vlrnom   = rs.getDouble(4);
                String ctpobs   = rs.getString(5);
                int    forcod   = rs.getInt(6);

                Integer idForn = mapaFornecedor.get(forcod);
                if (idForn == null) { ign++; continue; }

                Timestamp lancamento = datemi != null ? new Timestamp(datemi.getTime()) : new Timestamp(System.currentTimeMillis());

                try {
                    pst.setInt(1, idForn);
                    pst.setString(2, ctpnum != null ? ctpnum.trim() : "00");
                    pst.setTimestamp(3, lancamento);
                    pst.setTimestamp(4, lancamento);
                    pst.setDate(5, datvnc);
                    pst.setDouble(6, vlrnom);
                    pst.setDouble(7, vlrnom);
                    pst.setString(8, ctpobs != null ? ctpobs.trim() : "");
                    pst.setString(9, "MIGRACAO SYSPDV\nDATA REALIZACAO: " + new java.util.Date());
                    pst.executeUpdate();
                    ins++;
                } catch (SQLException e) {
                    LOG.warning("[PagarStep] Erro ctpnum=" + ctpnum + ": " + e.getMessage());
                    err++;
                }
                if ((ins + err + ign) % 200 == 0) { try { my.commit(); } catch (SQLException ex) {} }
            }
            my.commit();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo CONTAPAGAR do Firebird: " + e.getMessage(), e);
        } finally { close(rs); close(stFb); close(pst); }

        contarInseridos(ctx, ins);
        contarIgnorados(ctx, ign);
        contarErros(ctx, err);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.pagar", "rollback PagarStep");
    }
}
