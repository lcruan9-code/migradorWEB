package br.com.lcsistemas.host.step;

import br.com.lcsistemas.host.core.MigracaoContext;
import br.com.lcsistemas.host.core.MigracaoException;

import java.sql.*;

/**
 * Migra contas a pagar abertas de host.contas_pagar para lc_sistemas.pagar.
 *
 * Script base:
 *   -- Adiciona coluna auxiliar
 *   alter table host.contas_pagar add column id_fornecedores int(11);
 *   -- Resolve via coluna 'codigo' do fornecedor no destino
 *   update host.contas_pagar p
 *   inner join lc_sistemas.fornecedor f on f.codigo = p.id_fornecedor
 *   set p.id_fornecedores = f.id where situacao = 'ABERTO';
 *
 *   -- Insert somente ABERTO e fornecedor mapeado
 *   INSERT INTO LC_SISTEMAS.PAGAR (...)
 *   SELECT 1, 1, 1, 38, id_fornecedores, 1, 'FP', 'CA', ...
 *   FROM host.contas_pagar WHERE situacao = 'ABERTO' AND id_fornecedores IS NOT NULL;
 */
public class PagarStep extends StepBase {

    @Override
    public String getNome() { return "PagarStep"; }

    @Override
    public void prepare(MigracaoContext ctx) throws MigracaoException {
    }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection origem  = ctx.getOrigemConn();
        Connection destino = ctx.getDestinoConn();

        String selectSql =
            "SELECT ID_FORNECEDOR, NUMERO_DOCUMENTO, DT_CONTA, DT_VENC, HISTORICO, VLR_CONTA, VLR_QUITACAO" +
            " FROM CONTAS_PAGAR" +
            " WHERE SITUACAO = 'ABERTO'";

        String insertSql =
            "INSERT INTO lc_sistemas.pagar" +
            "  (id_usuario, id_empresa, id_pagamento, id_planocontas, id_fornecedor," +
            "   id_contamovimento, tipo, status, documento, numero_nf," +
            "   lancamento, emissao, vencimento, parcela," +
            "   valor_original, valor, valor_desc, valor_pag, data_pag," +
            "   juros_pag, multa_pag, multa, juros, agencia, conta," +
            "   conciliado, numero_cheque, numero_boleto, historico, valor_arec, obs)" +
            " VALUES (?,?,?,?,?, ?,?,?,?,NULL, ?,?,?,?, ?,?,0.000,0.000,NULL, 0.000,0.000,0.000,0.000,'','', NULL,'','',?,0.000,?)";

        int ins = 0;
        Statement stO = null; ResultSet rsO = null;
        PreparedStatement psI = null;

        try {
            stO = origem.createStatement();
            rsO = stO.executeQuery(selectSql);
            psI = destino.prepareStatement(insertSql);

            while (rsO.next()) {
                int    idFornRaw = rsO.getInt("ID_FORNECEDOR");
                
                int idForn = 1; // Default fallback if not found
                if (ctx.getMapaFornecedor() != null && ctx.getMapaFornecedor().containsKey(idFornRaw)) {
                    idForn = ctx.getMapaFornecedor().get(idFornRaw);
                } else {
                    contarIgnorados(ctx, 1);
                    continue; // Pula se n achar o fornecedor
                }
                
                String numDoc    = nvl(rsO.getString("NUMERO_DOCUMENTO"));
                java.sql.Date dtConta = rsO.getDate("DT_CONTA");
                java.sql.Date dtVenc  = rsO.getDate("DT_VENC");
                String historico      = nvl(rsO.getString("HISTORICO"));
                double vlrConta       = rsO.getDouble("VLR_CONTA");
                double vlrQuit        = rsO.getDouble("VLR_QUITACAO");
                double saldo          = vlrConta - vlrQuit;

                if (saldo <= 0) { contarIgnorados(ctx, 1); continue; }

                String dtContaStr = dtConta != null ? dtConta.toString() : new java.sql.Date(System.currentTimeMillis()).toString();

                psI.setInt(1,    ID_USUARIO);
                psI.setInt(2,    ID_EMPRESA);
                psI.setInt(3,    ID_PAGAMENTO_PAGAR);
                psI.setInt(4,    ID_PLANOCONTAS_PAGAR);
                psI.setInt(5,    idForn);
                psI.setInt(6,    ID_CONTAMOV_PAGAR);
                psI.setString(7, "FP");
                psI.setString(8, "CA");
                psI.setString(9, numDoc.isEmpty() ? "00" : numDoc);
                psI.setString(10, dtContaStr);
                psI.setString(11, dtContaStr + " 00:00:00");
                if (dtVenc != null) psI.setDate(12, dtVenc); else psI.setNull(12, Types.DATE);
                psI.setString(13, "1/1");
                psI.setDouble(14, saldo);
                psI.setDouble(15, saldo);
                psI.setString(16, "MIGRAÇÃO HOST\n" + historico);
                psI.setString(17, "");

                psI.addBatch();
                ins++;
            }
            psI.executeBatch();

        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao migrar pagar: " + e.getMessage(), e);
        } finally {
            close(rsO); close(stO); close(psI);
        }

        contarInseridos(ctx, ins);
        LOG.info("[PagarStep] " + ins + " contas a pagar inseridas.");
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.pagar", "rollback PagarStep");
    }

    @Override
    public void cleanup(MigracaoContext ctx) {
    }

    private String nvl(String s) { return s == null ? "" : s.trim(); }
}
