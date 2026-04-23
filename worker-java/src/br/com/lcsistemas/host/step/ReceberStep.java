package br.com.lcsistemas.host.step;

import br.com.lcsistemas.host.core.MigracaoContext;
import br.com.lcsistemas.host.core.MigracaoException;

import java.sql.*;
import java.util.Map;

/**
 * Migra contas a receber abertas de host.contas_receber para lc_sistemas.receber.
 *
 * Script base:
 *   -- Adiciona coluna auxiliar
 *   alter table host.contas_receber add column id_clientes int(11);
 *   -- Resolve id_clientes via numero_cartao do lc_sistemas.cliente
 *   update host.contas_receber r
 *   inner join lc_sistemas.cliente c on c.numero_cartao = r.id_cliente
 *   set r.id_clientes = c.id where r.situacao = 'ABERTO';
 *
 *   -- Insert somente ABERTO e com cliente mapeado
 *   insert into lc_sistemas.receber(...)
 *   select id_clientes, 1, 1, 1, 1, 1, 6, 28, 1, 0, 'FD', 'CA', '00', numero_documento,
 *          date(dt_conta), ..., date(dt_venc), '1/1',
 *          (vlr_conta - ifnull(vlr_quitacao,0.0)), 0.000, null, 0.000, 0.000, 0.000, 0.000,
 *          '', '', '', '', '', concat('MIGRAÇÃO HOST\n', historico), null
 *   from host.contas_receber where situacao = 'ABERTO' and id_clientes is not null;
 */
public class ReceberStep extends StepBase {

    @Override
    public String getNome() { return "ReceberStep"; }

    @Override
    public void prepare(MigracaoContext ctx) throws MigracaoException {
    }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection origem  = ctx.getOrigemConn();
        Connection destino = ctx.getDestinoConn();

        String selectSql =
            "SELECT ID_CLIENTE, NUMERO_DOCUMENTO, DT_CONTA, DT_VENC, HISTORICO, VLR_CONTA, VLR_QUITACAO" +
            " FROM CONTAS_RECEBER" +
            " WHERE SITUACAO = 'ABERTO'";

        String insertSql =
            "INSERT INTO lc_sistemas.receber" +
            "  (id_cliente, id_usuario, id_operador, id_cobrador, id_vendedor, id_empresa," +
            "   id_pagamento, id_planocontas, id_contamovimento, id_convenio," +
            "   tipo, status, documento, n_documento," +
            "   lancamento, emissao, vencimento, parcela," +
            "   valor_original, valor_rec, data_rec, juros_rec, multa_rec," +
            "   valor_arec, valor_desconto," +
            "   agencia, conta, conciliado, numero_cheque, numero_boleto," +
            "   historico, obs)" +
            " VALUES (?,?,?,?,?,?, ?,?,?,?, ?,?,?,?, ?,?,?,?, ?,0.000,NULL,0.000,0.000, 0.000,0.000, '','','','','', ?,NULL)";

        int ins = 0;
        Statement stO = null; ResultSet rsO = null;
        PreparedStatement psI = null;

        try {
            stO = origem.createStatement();
            rsO = stO.executeQuery(selectSql);
            psI = destino.prepareStatement(insertSql);

            while (rsO.next()) {
                int    idCliRaw  = rsO.getInt("ID_CLIENTE");
                
                int idCli = 1;
                if (ctx.getMapaCliente() != null && ctx.getMapaCliente().containsKey(idCliRaw)) {
                    idCli = ctx.getMapaCliente().get(idCliRaw);
                } else {
                    contarIgnorados(ctx, 1);
                    continue; // pula boleto de cliente nao encontrado
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

                psI.setInt(1,     idCli);
                psI.setInt(2,     ID_USUARIO);
                psI.setInt(3,     ID_OPERADOR);
                psI.setInt(4,     ID_COBRADOR);
                psI.setInt(5,     ID_VENDEDOR);
                psI.setInt(6,     ID_EMPRESA);
                psI.setInt(7,     ID_PAGAMENTO_REC);
                psI.setInt(8,     ID_PLANOCONTAS_REC);
                psI.setInt(9,     ID_CONTAMOV_REC);
                psI.setInt(10,    ID_CONVENIO);
                psI.setString(11, "FD");
                psI.setString(12, "CA");
                psI.setString(13, "00");
                psI.setString(14, numDoc.isEmpty() ? "00" : numDoc);
                psI.setString(15, dtContaStr);
                psI.setString(16, dtContaStr + " " + new java.sql.Time(System.currentTimeMillis()).toString());
                if (dtVenc != null) psI.setDate(17, dtVenc); else psI.setNull(17, Types.DATE);
                psI.setString(18, "1/1");
                psI.setDouble(19, saldo);
                psI.setString(20, "MIGRAÇÃO HOST\n" + historico);

                psI.addBatch();
                ins++;
            }
            psI.executeBatch();

        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao migrar receber: " + e.getMessage(), e);
        } finally {
            close(rsO); close(stO); close(psI);
        }

        contarInseridos(ctx, ins);
        LOG.info("[ReceberStep] " + ins + " contas a receber inseridas.");
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.receber", "rollback ReceberStep");
    }

    @Override
    public void cleanup(MigracaoContext ctx) {
    }

    private String nvl(String s) { return s == null ? "" : s.trim(); }
}
