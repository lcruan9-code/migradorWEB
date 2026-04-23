package br.com.lcsistemas.host.step;

import br.com.lcsistemas.host.core.MigracaoContext;
import br.com.lcsistemas.host.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Migra NCMs únicos de host.produtos para lc_sistemas.ncm.
 *
 * Script base:
 *   -- Correção de NCM (remove vírgulas e pontos)
 *   update host.produtos set ncm = replace(replace(ncm,',',''),'.','');
 *   -- Insere NCMs com 8 dígitos e > 0
 *   insert into lc_sistemas.ncm(codigo,...) select ncm, ... from host.produtos
 *   where length(ncm) = 8 and ncm > 0 group by ncm;
 */
public class NcmStep extends StepBase {

    @Override
    public String getNome() { return "NcmStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection origem  = ctx.getOrigemConn();
        Connection destino = ctx.getDestinoConn();

        // 1. Limpa destino (mantém id=1)
        exec(destino, "DELETE FROM lc_sistemas.ncm WHERE id > 1");
        // exec(destino, "ALTER TABLE lc_sistemas.ncm AUTO_INCREMENT = 1");

        // 2. Coleta NCMs únicos da origem
        Map<String, Integer> mapaNcm = new HashMap<String, Integer>();
        PreparedStatement ps  = null;
        Statement         st  = null;
        ResultSet         rs  = null;
        java.util.Set<String> processados = new java.util.HashSet<String>();

        try {
            ps = destino.prepareStatement(
                "INSERT INTO lc_sistemas.ncm" +
                "  (codigo, ex, descricao, aliquota_nacional, aliquota_internacional," +
                "   aliquota_estadual, aliquota_municipal, vigenciainicio, vigenciafim," +
                "   chave, versao, ativo)" +
                " VALUES (?, '', '', 0.000, 0.000, 0.000, 0.000, NULL, NULL, '', '', 1)",
                Statement.RETURN_GENERATED_KEYS);

            st = origem.createStatement();
            rs = st.executeQuery("SELECT DISTINCT NCM FROM PRODUTOS WHERE NCM IS NOT NULL");

            int ins = 0;
            while (rs.next()) {
                String ncmBruto = rs.getString(1);
                if (ncmBruto == null) continue;
                String ncm = ncmBruto.replace(",", "").replace(".", "").trim();
                
                if (ncm.length() == 8 && ncm.matches("\\d+") && !ncm.matches("0+")) {
                    if (processados.add(ncm)) {
                        ps.setString(1, ncm);
                ps.executeUpdate();
                ResultSet keys = ps.getGeneratedKeys();
                        if (keys.next()) {
                            mapaNcm.put(ncm.trim(), keys.getInt(1));
                            ins++;
                        }
                        keys.close();
                    }
                }
            }
            contarInseridos(ctx, ins);
            LOG.info("[NcmStep] " + ins + " NCMs inseridos.");

        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao migrar NCM: " + e.getMessage(), e);
        } finally {
            close(rs); close(st); close(ps);
        }

        // 4. Também carrega NCMs já existentes (id=1 padrão etc.)
        carregarMapaNcm(ctx, mapaNcm);
    }

    private void carregarMapaNcm(MigracaoContext ctx, Map<String, Integer> mapaNcm)
            throws MigracaoException {
        Statement st = null; ResultSet rs = null;
        try {
            st = ctx.getDestinoConn().createStatement();
            rs = st.executeQuery("SELECT id, codigo FROM lc_sistemas.ncm");
            while (rs.next()) mapaNcm.put(rs.getString(2), rs.getInt(1));
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao carregar mapa NCM: " + e.getMessage(), e);
        } finally {
            close(rs); close(st);
        }
        ctx.setMapaNcm(mapaNcm);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "DELETE FROM lc_sistemas.ncm WHERE id > 1",
            "rollback NcmStep");
    }
}
