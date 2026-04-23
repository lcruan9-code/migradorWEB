package br.com.lcsistemas.gdoor.step;

import br.com.lcsistemas.gdoor.core.MigracaoContext;
import br.com.lcsistemas.gdoor.core.MigracaoException;

import java.sql.*;
import java.util.Map;

/**
 * Migra contas a receber do Firebird (RECEBER) para lc_sistemas.receber.
 *
 * Usa mapaCliente (codigo_gdoor -> id_mysql) gerado pelo ClienteStep.
 * Itera RECEBER no Firebird e faz JOIN com CLIENTE para obter codigo_cliente.
 * Filtra registros já quitados: valor_dup - (valor_rec + valor_des) <= 0.
 */
public class ReceberStep extends StepBase {

    @Override
    public String getNome() { return "ReceberStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Map<Integer, Integer> mapaCli = ctx.getMapaCliente();

        exec(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.receber");
        // exec(ctx.getDestinoConn(), "ALTER TABLE lc_sistemas.receber AUTO_INCREMENT = 1");

        String selectSql =
            "SELECT r.DOCUMENTO, r.EMISSAO, r.VENCIMENTO, r.HISTORICO," +
            "       r.VALOR_DUP, r.VALOR_REC, r.VALOR_DES," +
            "       c.CODIGO AS COD_CLI " +
            "FROM RECEBER r " +
            "LEFT JOIN CLIENTE c ON c.CODIGO = r.COD_CLIENTE";

        String insertSql =
            "INSERT INTO lc_sistemas.receber " +
            "  (id_cliente, id_empresa, id_usuario, id_cobrador, id_vendedor, id_operador," +
            "   id_pagamento, id_planocontas, id_contamovimento, id_convenio," +
            "   tipo, status, documento, n_documento," +
            "   lancamento, emissao, vencimento, parcela," +
            "   valor_original, valor_rec, data_rec, juros_rec, multa_rec," +
            "   retencao_rec, valor_arec, valor_desconto," +
            "   agencia, conta, conciliado, numero_cheque, numero_boleto," +
            "   historico, obs) " +
            "VALUES (?,?,?,?,?,?, ?,?,?,?, ?,?,?,?, ?,?,?,?, ?,0.000,NULL,0.000,0.000, 0.000,0.000,0.000, '','','','','', ?,?)";

        int ins = 0;
        Statement stFb = null; ResultSet rsFb = null;
        PreparedStatement psIns = null;
        try {
            stFb  = ctx.getOrigemConn().createStatement();
            rsFb  = stFb.executeQuery(selectSql);
            psIns = ctx.getDestinoConn().prepareStatement(insertSql);

            while (rsFb.next()) {
                int    codCli    = rsFb.getInt("COD_CLI");
                int    idCli     = mapaCli.getOrDefault(codCli, 0);
                if (idCli == 0)  { contarIgnorados(ctx, 1); continue; }

                double valorDup  = rsFb.getDouble("VALOR_DUP");
                double valorRec  = rsFb.getDouble("VALOR_REC");
                double valorDes  = rsFb.getDouble("VALOR_DES");
                double saldo     = valorDup - (valorRec + valorDes);
                if (saldo <= 0)  { contarIgnorados(ctx, 1); continue; } // já quitado

                String documento = nvl(rsFb.getString("DOCUMENTO"));
                java.sql.Date emissao = rsFb.getDate("EMISSAO");
                java.sql.Date vencto  = rsFb.getDate("VENCIMENTO");
                String historico      = nvl(rsFb.getString("HISTORICO"));
                String emissaoStr     = emissao != null ? emissao.toString() : new java.sql.Date(System.currentTimeMillis()).toString();

                psIns.setInt(1,    idCli);
                psIns.setInt(2,    ID_EMPRESA);
                psIns.setInt(3,    ID_USUARIO);
                psIns.setInt(4,    ID_COBRADOR);
                psIns.setInt(5,    ID_VENDEDOR);
                psIns.setInt(6,    ID_OPERADOR);
                psIns.setInt(7,    ID_PAGAMENTO_REC);
                psIns.setInt(8,    ID_PLANOCONTAS_REC);
                psIns.setInt(9,    ID_CONTAMOV_REC);
                psIns.setInt(10,   ID_CONVENIO);
                psIns.setString(11,"FD");
                psIns.setString(12,"CA");
                psIns.setString(13,"00");
                psIns.setString(14, documento);
                psIns.setString(15, emissaoStr);
                psIns.setString(16, emissaoStr + " " + new java.sql.Time(System.currentTimeMillis()).toString());
                if (vencto != null) psIns.setDate(17, vencto); else psIns.setNull(17, Types.DATE);
                psIns.setString(18,"1/1");
                psIns.setDouble(19, saldo);
                psIns.setString(20, historico);
                psIns.setString(21, "MIGRACAO GDOOR: " + new java.sql.Timestamp(System.currentTimeMillis()));

                psIns.addBatch();
                ins++;
            }
            psIns.executeBatch();

        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao migrar receber: " + e.getMessage(), e);
        } finally {
            close(rsFb); close(stFb); close(psIns);
        }

        contarInseridos(ctx, ins);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.receber", "rollback ReceberStep");
    }

    private String nvl(String s) { return s == null ? "" : s.trim(); }
}
