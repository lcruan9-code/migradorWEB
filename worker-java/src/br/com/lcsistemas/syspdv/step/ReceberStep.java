package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.Map;

/**
 * SYSPDV (Firebird) -> lc_sistemas.receber
 *
 * Le CONTARECEBER do Firebird. Filtra ctrtippag IN ('A','P').
 * Usa mapaCliente para resolucao de FK.
 *
 * Colunas Firebird: TRNSEQ, CLICOD, CTRDATEMI, CTRDATVNC, CTRVLRDEV,
 *   CTRTIPPAG, CTRDATPGT
 */
public class ReceberStep extends StepBase {

    @Override
    public String getNome() { return "ReceberStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb = ctx.getOrigemConn();
        Connection my = ctx.getDestinoConn();

        exec(my, "DELETE FROM lc_sistemas.receber");
        // execIgnore(my, "ALTER TABLE lc_sistemas.receber AUTO_INCREMENT = 1", "reset AI receber");

        Map<Integer, Integer> mapaCliente = ctx.getMapaCliente();

        int ins = 0, ign = 0, err = 0;
        PreparedStatement pst = null;
        Statement stFb = null; ResultSet rs = null;
        try {
            stFb = fb.createStatement();
            rs = stFb.executeQuery(
                "SELECT TRNSEQ, CLICOD, CTRDATEMI, CTRDATVNC, CTRVLRDEV, CTRTIPPAG " +
                "FROM CONTARECEBER WHERE CTRTIPPAG IN ('A','P')");

            pst = my.prepareStatement(
                "INSERT INTO lc_sistemas.receber(" +
                "id_cliente,id_operador,id_cobrador,id_vendedor,id_usuario,id_empresa," +
                "id_pagamento,id_planocontas,id_contamovimento,id_convenio," +
                "tipo,status,documento,n_documento," +
                "lancamento,emissao,vencimento,parcela," +
                "valor_original,valor_rec,data_rec,juros_rec,multa_rec," +
                "valor_arec,valor_desconto,agencia,conta,conciliado," +
                "numero_cheque,numero_boleto,historico,obs)" +
                " VALUES (?,0,0,0,1,1,6,28,1,0,'FD','CA','00',?,?,?,?,'1/1',?,0,NULL,0,0,0,0,'','','','','','',?)");

            while (rs.next()) {
                int    trnseq   = rs.getInt(1);
                int    clicod   = rs.getInt(2);
                Date   datemi   = rs.getDate(3);
                Date   datvnc   = rs.getDate(4);
                double vlrdev   = rs.getDouble(5);

                Integer idCliente = mapaCliente.get(clicod);
                if (idCliente == null) { ign++; continue; }

                Timestamp lancamento = datemi != null ? new Timestamp(datemi.getTime()) : new Timestamp(System.currentTimeMillis());

                try {
                    pst.setInt(1, idCliente);
                    pst.setString(2, String.valueOf(trnseq));
                    pst.setTimestamp(3, lancamento);
                    pst.setTimestamp(4, lancamento);
                    pst.setDate(5, datvnc);
                    pst.setDouble(6, vlrdev);
                    pst.setString(7, "MIGRACAO SYSPDV\nDATA REALIZACAO: " + new java.util.Date());
                    pst.executeUpdate();
                    ins++;
                } catch (SQLException e) {
                    LOG.warning("[ReceberStep] Erro trnseq=" + trnseq + ": " + e.getMessage());
                    err++;
                }
                if ((ins + err + ign) % 200 == 0) { try { my.commit(); } catch (SQLException ex) {} }
            }
            my.commit();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo CONTARECEBER do Firebird: " + e.getMessage(), e);
        } finally { close(rs); close(stFb); close(pst); }

        contarInseridos(ctx, ins);
        contarIgnorados(ctx, ign);
        contarErros(ctx, err);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.receber", "rollback ReceberStep");
    }
}
