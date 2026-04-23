package br.com.lcsistemas.syspdv.step;

import br.com.lcsistemas.syspdv.core.MigracaoContext;
import br.com.lcsistemas.syspdv.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Clipp (Firebird) → lc_sistemas.cliente
 * TB_CLIENTE + TB_CLI_PF (CPF/identidade) + TB_CLI_PJ (CNPJ/IE).
 * ID_CIDADE = código IBGE (VARCHAR).
 */
public class ClippClienteStep extends StepBase {

    @Override public String getNome() { return "ClienteStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection fb  = ctx.getOrigemConn();
        Connection my  = ctx.getDestinoConn();
        int cidadeDef  = ctx.getConfig().getCidadeDefaultId();
        int estadoDef  = ctx.getConfig().getEstadoDefaultId();

        exec(my, "DELETE FROM lc_sistemas.cliente WHERE id > 2");
        // execIgnore(my, "ALTER TABLE lc_sistemas.cliente AUTO_INCREMENT = 1", "reset AI cliente");

        Map<Integer, int[]>    mapaCidade         = carregarMapaCidade(my, getNome());
        Map<Integer, String[]> mapaClippMunicipio = carregarMunicipioClipp(fb, getNome());

        // Pré-carrega TB_CLI_PF: id_cliente → {cpf, identidade, dt_nascto, nome_pai, nome_mae}
        Map<Integer, String[]> mapaPf = new HashMap<>();
        Statement stPf = null; ResultSet rsPf = null;
        try {
            stPf = fb.createStatement();
            rsPf = stPf.executeQuery("SELECT ID_CLIENTE, CPF, IDENTIDADE, DT_NASCTO, NOME_PAI, NOME_MAE FROM TB_CLI_PF");
            while (rsPf.next()) {
                mapaPf.put(rsPf.getInt(1), new String[]{
                    rsPf.getString(2), rsPf.getString(3),
                    rsPf.getString(4), rsPf.getString(5), rsPf.getString(6)
                });
            }
        } catch (SQLException e) { LOG.warning("[ClippClienteStep] TB_CLI_PF: " + e.getMessage()); }
        finally { close(rsPf); close(stPf); }

        // Pré-carrega TB_CLI_PJ: id_cliente → {cnpj, nome_fanta, insc_estad}
        Map<Integer, String[]> mapaPj = new HashMap<>();
        Statement stPj = null; ResultSet rsPj = null;
        try {
            stPj = fb.createStatement();
            rsPj = stPj.executeQuery("SELECT ID_CLIENTE, CNPJ, NOME_FANTA, INSC_ESTAD FROM TB_CLI_PJ");
            while (rsPj.next()) {
                mapaPj.put(rsPj.getInt(1), new String[]{
                    rsPj.getString(2), rsPj.getString(3), rsPj.getString(4)
                });
            }
        } catch (SQLException e) { LOG.warning("[ClippClienteStep] TB_CLI_PJ: " + e.getMessage()); }
        finally { close(rsPj); close(stPj); }

        int ins = 0, err = 0;
        PreparedStatement pst = null; Statement stFb = null; ResultSet rs = null;
        Map<Integer, Integer> mapaCliente = new HashMap<>();
        try {
            stFb = fb.createStatement();
            rs = stFb.executeQuery(
                "SELECT ID_CLIENTE, NOME, END_LOGRAD, END_BAIRRO, END_CEP, END_NUMERO, END_COMPLE, " +
                "LIMITE, DDD_RESID, FONE_RESID, DDD_COMER, FONE_COMER, DDD_CELUL, FONE_CELUL, " +
                "EMAIL_CONT, ID_CIDADE, STATUS " +
                "FROM TB_CLIENTE WHERE ID_CLIENTE > 0");
            pst = my.prepareStatement(
                "INSERT INTO lc_sistemas.cliente(" +
                "numero_cartao,nome,razao_social,endereco,cpf_cnpj,bairro,obs," +
                "cep,numero,referencia,limite_credito,rg,ie,nascimento_adi," +
                "pai_adi,mae_adi,tipo,email_adi,telefone," +
                "id_cidade,id_cidade2,id_estado,id_estado2,id_pais,id_clientecanal,id_empresa," +
                "data_cadastro,datahora_alteracao)" +
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,34,0,1,DATE(NOW()),NOW())",
                Statement.RETURN_GENERATED_KEYS);
            while (rs.next()) {
                int    id     = rs.getInt(1);
                String nome   = rs.getString(2);
                if (nome == null || nome.trim().isEmpty()) continue;
                String end    = rs.getString(3);
                String bai    = rs.getString(4);
                String cep    = rs.getString(5);
                String num    = rs.getString(6);
                String cmp    = rs.getString(7);
                double lim    = rs.getDouble(8);
                if (lim > 1_000_000.0) lim = 0.0;
                String dddR   = rs.getString(9);
                String foneR  = rs.getString(10);
                String dddC   = rs.getString(11);
                String foneC  = rs.getString(12);
                String dddCel = rs.getString(13);
                String foneCel= rs.getString(14);
                String email  = rs.getString(15);
                String cidIbge= rs.getString(16);

                int[] cid    = resolverCidade(cidIbge, mapaClippMunicipio, mapaCidade, cidadeDef, estadoDef);
                int idCidade = cid[0];
                int idEstado = cid[1];

                String tel = ((dddR != null ? dddR.trim() : "") + (foneR != null ? foneR.trim() : "")).trim();
                if (tel.isEmpty()) tel = ((dddC != null ? dddC.trim() : "") + (foneC != null ? foneC.trim() : "")).trim();
                if (tel.isEmpty()) tel = ((dddCel != null ? dddCel.trim() : "") + (foneCel != null ? foneCel.trim() : "")).trim();

                String[] pj   = mapaPj.get(id);
                String[] pf   = mapaPf.get(id);
                String tipo   = (pj != null) ? "J" : "F";
                String cpfCnpj= "";
                String rg = "", ie = "", pai = "", mae = "";
                Date dtNasc = null;
                if (pj != null) {
                    cpfCnpj = pj[0] != null ? pj[0].trim() : "";
                    ie      = pj[2] != null ? pj[2].trim() : "";
                }
                if (pf != null) {
                    if (cpfCnpj.isEmpty()) cpfCnpj = pf[0] != null ? pf[0].trim() : "";
                    rg   = pf[1] != null ? pf[1].trim() : "";
                    pai  = pf[4] != null ? pf[4].trim() : "";
                    mae  = pf[5] != null ? pf[5].trim() : "";
                }

                String obs = "TEL: " + tel + "\nMIGRACAO CLIPP";

                try {
                    pst.setInt(1, id); pst.setString(2, nome.trim()); pst.setString(3, nome.trim());
                    pst.setString(4, end   != null ? end.trim()  : "");
                    pst.setString(5, cpfCnpj);
                    pst.setString(6, bai   != null ? bai.trim()  : "");
                    pst.setString(7, obs);
                    pst.setString(8, cep   != null ? cep.trim()  : "");
                    pst.setString(9, num   != null ? num.trim()  : "");
                    pst.setString(10, cmp  != null ? cmp.trim()  : "");
                    pst.setDouble(11, lim); pst.setString(12, rg); pst.setString(13, ie);
                    pst.setNull(14, Types.DATE); // dtNasc
                    pst.setString(15, pai); pst.setString(16, mae);
                    pst.setString(17, tipo);
                    pst.setString(18, email != null ? email.trim() : "");
                    pst.setString(19, tel);
                    pst.setInt(20, idCidade); pst.setInt(21, idCidade);
                    pst.setInt(22, idEstado); pst.setInt(23, idEstado);
                    pst.executeUpdate();
                    ResultSet gk = pst.getGeneratedKeys();
                    if (gk.next()) mapaCliente.put(id, gk.getInt(1));
                    close(gk); ins++;
                } catch (SQLException e) {
                    LOG.warning("[ClippClienteStep] id=" + id + ": " + e.getMessage()); err++;
                }
                if ((ins + err) % 200 == 0) { try { my.commit(); } catch (SQLException ex) {} }
            }
            my.commit();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro lendo TB_CLIENTE: " + e.getMessage(), e);
        } finally { close(rs); close(stFb); close(pst); }

        ctx.setMapaCliente(mapaCliente);
        contarInseridos(ctx, ins); contarErros(ctx, err);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(), "DELETE FROM lc_sistemas.cliente WHERE id > 2", "rollback ClippClienteStep");
    }
}
