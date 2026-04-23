package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * SYSPDV (Firebird) -> lc_sistemas.cliente
 *
 * Le CLIENTE do Firebird. Resolve id_cidade/id_estado via CLICODIGOIBGE.
 * Salva mapaCliente: CLICOD -> id em ctx para uso no ReceberStep.
 *
 * Colunas Firebird: CLICOD, CLIDES, CLIEND, CLICPFCGC, CLIBAI, CLITEL,
 *   CLICEP, CLINUM, CLICMP, CLILIMCRE, CLIRGCGF, CLIDTNAS,
 *   CLIPAI, CLIMAE, CLIPFPJ, CLIEMAIL, CLICODIGOIBGE
 */
public class ClienteStep extends StepBase {

    @Override
    public String getNome() { return "ClienteStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb  = ctx.getOrigemConn();
        Connection my  = ctx.getDestinoConn();
        int cidadeDef  = ctx.getConfig().getCidadeDefaultId();
        int estadoDef  = ctx.getConfig().getEstadoDefaultId();

        exec(my, "DELETE FROM lc_sistemas.cliente WHERE id > 2");
        // execIgnore(my, "ALTER TABLE lc_sistemas.cliente AUTO_INCREMENT = 1", "reset AI cliente");

        Map<Integer, int[]> mapaCidade = carregarMapaCidade(my, getNome());

        int ins = 0, err = 0;
        PreparedStatement pst = null;
        Statement stFb = null; ResultSet rs = null;
        Map<Integer, Integer> mapaCliente = new HashMap<Integer, Integer>();

        try {
            stFb = fb.createStatement();
            rs = stFb.executeQuery(
                "SELECT CLICOD, CLIDES, CLIEND, CLICPFCGC, CLIBAI, CLITEL," +
                "  CLICEP, CLINUM, CLICMP, CLILIMCRE, CLIRGCGF, CLIDTNAS," +
                "  CLIPAI, CLIMAE, CLIPFPJ, CLIEMAIL, CLICODIGOIBGE " +
                "FROM CLIENTE WHERE CLICOD <> 0 AND CHAR_LENGTH(TRIM(CLIDES)) >= 1");

            pst = my.prepareStatement(
                "INSERT INTO lc_sistemas.cliente(" +
                "numero_cartao,nome,razao_social,endereco,cpf_cnpj,bairro,obs," +
                "cep,numero,referencia,limite_credito,rg,ie,nascimento_adi," +
                "pai_adi,mae_adi,tipo,email_adi,telefone," +
                "id_cidade,id_cidade2,id_estado,id_estado2,id_pais,id_clientecanal,id_empresa)" +
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,34,0,1)",
                Statement.RETURN_GENERATED_KEYS);

            while (rs.next()) {
                int    clicod      = rs.getInt(1);
                String clides      = rs.getString(2);
                if (clides == null || clides.trim().isEmpty()) continue;
                String cliend      = rs.getString(3);
                String clicpfcgc   = rs.getString(4);
                String clibai      = rs.getString(5);
                String clitel      = rs.getString(6);
                String clicep      = rs.getString(7);
                String clinum      = rs.getString(8);
                String clicmp      = rs.getString(9);
                double clilimcre   = rs.getDouble(10);
                String clirgcgf    = rs.getString(11);
                Date   clidtnas    = rs.getDate(12);
                String clipai      = rs.getString(13);
                String climae      = rs.getString(14);
                String clipfpj     = rs.getString(15);
                String cliemail    = rs.getString(16);
                int    cliibge     = rs.getInt(17);

                int[] cid = mapaCidade.get(cliibge);
                int idCidade = (cid != null) ? cid[0] : cidadeDef;
                int idEstado = (cid != null) ? cid[1] : estadoDef;

                if (clilimcre > 1000000.0) clilimcre = 0.0;

                String cpf  = (clicpfcgc != null && clicpfcgc.trim().length() < 14) ? clicpfcgc.trim() : "";
                String cnpj = (clicpfcgc != null && clicpfcgc.trim().length() == 14) ? clicpfcgc.trim() : "";
                String rg   = (clicpfcgc != null && clicpfcgc.trim().length() < 14)  ? (clirgcgf != null ? clirgcgf.trim() : "") : "";
                String ie   = (clicpfcgc != null && clicpfcgc.trim().length() == 14) ? (clirgcgf != null ? clirgcgf.trim() : "") : "";
                String tipo = (clipfpj != null && clipfpj.trim().equals("J")) ? "J" : "F";
                String cpfCnpj = !cnpj.isEmpty() ? cnpj : cpf;

                String obs = "TELEFONE: " + (clitel != null ? clitel : "") +
                             "\nMIGRACAO SYSPDV\nMIGRADO EM: " + new java.util.Date();

                try {
                    pst.setInt(1, clicod);
                    pst.setString(2, clides.trim());
                    pst.setString(3, clides.trim());
                    pst.setString(4, cliend  != null ? cliend.trim()  : "");
                    pst.setString(5, cpfCnpj);
                    pst.setString(6, clibai  != null ? clibai.trim()  : "");
                    pst.setString(7, obs);
                    pst.setString(8, clicep  != null ? clicep.trim()  : "");
                    pst.setString(9, clinum  != null ? clinum.trim()  : "");
                    pst.setString(10, clicmp != null ? clicmp.trim()  : "");
                    pst.setDouble(11, clilimcre);
                    pst.setString(12, rg);
                    pst.setString(13, ie);
                    pst.setDate(14, clidtnas);
                    pst.setString(15, clipai  != null ? clipai.trim() : "");
                    pst.setString(16, climae  != null ? climae.trim() : "");
                    pst.setString(17, tipo);
                    pst.setString(18, cliemail != null ? cliemail.trim() : "");
                    pst.setString(19, clitel  != null ? clitel.trim()  : "");
                    pst.setInt(20, idCidade); pst.setInt(21, idCidade);
                    pst.setInt(22, idEstado); pst.setInt(23, idEstado);
                    pst.executeUpdate();
                    ResultSet gk = pst.getGeneratedKeys();
                    if (gk.next()) mapaCliente.put(clicod, gk.getInt(1));
                    close(gk);
                    ins++;
                } catch (SQLException e) {
                    LOG.warning("[ClienteStep] Erro clicod=" + clicod + ": " + e.getMessage());
                    err++;
                }
                if ((ins + err) % 200 == 0) { try { my.commit(); } catch (SQLException ex) {} }
            }
            my.commit();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo CLIENTE do Firebird: " + e.getMessage(), e);
        } finally { close(rs); close(stFb); close(pst); }

        ctx.setMapaCliente(mapaCliente);
        contarInseridos(ctx, ins);
        contarErros(ctx, err);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.cliente WHERE id > 2", "rollback ClienteStep");
    }
}
