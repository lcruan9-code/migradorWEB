package br.com.lcsistemas.gdoor.step;

import br.com.lcsistemas.gdoor.core.MigracaoContext;
import br.com.lcsistemas.gdoor.core.MigracaoException;

import java.sql.*;
import java.util.Map;

/**
 * Migra contas a pagar do Firebird (PAGAR) para lc_sistemas.pagar.
 *
 * Usa mapaFornecedor (codigo_gdoor -> id_mysql) gerado pelo FornecedorStep.
 * Itera PAGAR no Firebird e faz JOIN com FORNECEDOR para obter codigo_fornecedor.
 */
public class PagarStep extends StepBase {

    @Override
    public String getNome() { return "PagarStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Map<Integer, Integer> mapaForn = ctx.getMapaFornecedor();

        exec(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.pagar");
        // exec(ctx.getDestinoConn(), "ALTER TABLE lc_sistemas.pagar AUTO_INCREMENT = 1");

        String selectSql =
            "SELECT p.DOCUMENTO, p.EMISSAO, p.VENCIMENTO, p.VALOR_DUP, p.HISTORICO," +
            "       p.PAGAMENTO, f.CODIGO AS COD_FORN " +
            "FROM PAGAR p " +
            "LEFT JOIN FORNECEDOR f ON f.CODIGO = p.COD_FORNECEDOR " +
            "WHERE p.PAGAMENTO IS NULL";

        String insertSql =
            "INSERT INTO lc_sistemas.pagar " +
            "  (id_usuario, id_empresa, id_pagamento, id_planocontas, id_fornecedor," +
            "   id_contamovimento, tipo, status, documento, numero_nf," +
            "   lancamento, emissao, vencimento, parcela," +
            "   valor_original, valor, valor_desc, valor_pag, data_pag," +
            "   juros_pag, multa_pag, multa, juros, agencia, conta," +
            "   conciliado, numero_cheque, numero_boleto, historico, valor_arec, obs) " +
            "VALUES (?,?,?,?,?, ?,?,?,?,NULL, ?,?,?,?, ?,?,0.000,0.000,NULL, 0.000,0.000,0.000,0.000,'','', NULL,'','',?,0.000,?)";

        int ins = 0;
        Statement stFb = null; ResultSet rsFb = null;
        PreparedStatement psIns = null;
        try {
            stFb  = ctx.getOrigemConn().createStatement();
            rsFb  = stFb.executeQuery(selectSql);
            psIns = ctx.getDestinoConn().prepareStatement(insertSql);

            while (rsFb.next()) {
                int    codForn   = rsFb.getInt("COD_FORN");
                int    idForn    = mapaForn.getOrDefault(codForn, 0);
                if (idForn == 0) { contarIgnorados(ctx, 1); continue; } // fornecedor não migrado

                String documento = nvl(rsFb.getString("DOCUMENTO"));
                java.sql.Date emissao  = rsFb.getDate("EMISSAO");
                java.sql.Date vencto   = rsFb.getDate("VENCIMENTO");
                double valor           = rsFb.getDouble("VALOR_DUP");
                String historico       = nvl(rsFb.getString("HISTORICO"));
                String emissaoStr      = emissao != null ? emissao.toString() : new java.sql.Date(System.currentTimeMillis()).toString();

                psIns.setInt(1,    ID_USUARIO);
                psIns.setInt(2,    ID_EMPRESA);
                psIns.setInt(3,    ID_PAGAMENTO_PAGAR);
                psIns.setInt(4,    ID_PLANOCONTAS_PAGAR);
                psIns.setInt(5,    idForn);
                psIns.setInt(6,    ID_CONTAMOV_PAGAR);
                psIns.setString(7, "FP");
                psIns.setString(8, "CA");
                psIns.setString(9, documento);
                psIns.setString(10, emissaoStr);
                psIns.setString(11, emissaoStr + " 00:00:00");
                if (vencto != null) psIns.setDate(12, vencto); else psIns.setNull(12, Types.DATE);
                psIns.setString(13, "1/1");
                psIns.setDouble(14, valor);
                psIns.setDouble(15, valor);
                psIns.setString(16, historico);
                psIns.setString(17, "MIGRACAO GDOOR: " + new java.sql.Timestamp(System.currentTimeMillis()));

                psIns.addBatch();
                ins++;
            }
            psIns.executeBatch();

        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao migrar pagar: " + e.getMessage(), e);
        } finally {
            close(rsFb); close(stFb); close(psIns);
        }

        contarInseridos(ctx, ins);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.pagar", "rollback PagarStep");
    }

    private String nvl(String s) { return s == null ? "" : s.trim(); }
}
