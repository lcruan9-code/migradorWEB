package br.com.lcsistemas.host.step;

import br.com.lcsistemas.host.core.MigracaoContext;
import br.com.lcsistemas.host.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Migra CESTs únicos de host.produtos para lc_sistemas.cest.
 *
 * Script base:
 *   insert into lc_sistemas.cest(cest, ncm, descricao)
 *   select cest, '00000000', '' from host.produtos
 *   where length(cest) = 7 and cest > 0 group by cest;
 */
public class CestStep extends StepBase {

    @Override
    public String getNome() { return "CestStep"; }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection origem  = ctx.getOrigemConn();
        Connection destino = ctx.getDestinoConn();

        // Limpa destino (mantém id=1)
        exec(destino, "DELETE FROM lc_sistemas.cest WHERE id > 1");
        // exec(destino, "ALTER TABLE lc_sistemas.cest AUTO_INCREMENT = 1");

        Map<String, Integer> mapaCest = new HashMap<String, Integer>();

        PreparedStatement ps = null;
        Statement         st = null;
        ResultSet         rs = null;

        try {
            ps = destino.prepareStatement(
                "INSERT INTO lc_sistemas.cest (cest, ncm, descricao) VALUES (?, '00000000', '')",
                Statement.RETURN_GENERATED_KEYS);

            st = origem.createStatement();
            rs = st.executeQuery("SELECT DISTINCT CEST FROM PRODUTOS WHERE CEST IS NOT NULL");

            int ins = 0;
            while (rs.next()) {
                String cestBruto = rs.getString(1);
                if (cestBruto == null) continue;
                String cest = cestBruto.trim();
                
                if (cest.length() == 7 && cest.matches("\\d+") && !cest.matches("0+")) {
                    ps.setString(1, cest);
                    ps.executeUpdate();
                    ResultSet keys = ps.getGeneratedKeys();
                    if (keys.next()) {
                        mapaCest.put(cest, keys.getInt(1));
                        ins++;
                    }
                    keys.close();
                }
            }
            contarInseridos(ctx, ins);
            LOG.info("[CestStep] " + ins + " CESTs inseridos.");

        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao migrar CEST: " + e.getMessage(), e);
        } finally {
            close(rs); close(st); close(ps);
        }

        // Recarrega mapa completo (inclui id=1)
        try {
            Statement stD = destino.createStatement();
            ResultSet rsD = stD.executeQuery("SELECT id, cest FROM lc_sistemas.cest");
            while (rsD.next()) mapaCest.put(rsD.getString(2), rsD.getInt(1));
            rsD.close(); stD.close();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao recarregar mapa CEST: " + e.getMessage(), e);
        }

        ctx.setMapaCest(mapaCest);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "DELETE FROM lc_sistemas.cest WHERE id > 1",
            "rollback CestStep");
    }
}
