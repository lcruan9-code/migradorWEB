package br.com.lcsistemas.host.step;

import br.com.lcsistemas.host.core.MigracaoContext;
import br.com.lcsistemas.host.core.MigracaoException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Migra marcas de produtos de host.produtos_marca para lc_sistemas.fabricante.
 *
 * Script base:
 *   insert into lc_sistemas.fabricante(nome, datahora_alteracao, ativo)
 *   select marca, now(), 1 from host.produtos_marca
 *   where length(trim(marca)) >= 1 group by marca;
 *
 *   update host.produtos p
 *   inner join host.produtos_marca m on p.marca = m.id
 *   inner join lc_sistemas.fabricante f on m.marca = f.nome
 *   set p.id_fabricante = f.id;
 */
public class FabricanteStep extends StepBase {

    @Override
    public String getNome() { return "FabricanteStep"; }

    @Override
    public void prepare(MigracaoContext ctx) throws MigracaoException {
    }

    @Override
    public void execute(MigracaoContext ctx) throws MigracaoException {
        Connection origem  = ctx.getOrigemConn();
        Connection destino = ctx.getDestinoConn();

        // Limpa destino (mantém id=1)
        exec(destino, "DELETE FROM lc_sistemas.fabricante WHERE id > 1");
        // exec(destino, "ALTER TABLE lc_sistemas.fabricante AUTO_INCREMENT = 1");

        Map<String, Integer> mapaFabricante = new HashMap<String, Integer>();

        PreparedStatement ps = null;
        Statement         st = null;
        ResultSet         rs = null;

        try {
            ps = destino.prepareStatement(
                "INSERT INTO lc_sistemas.fabricante (nome, datahora_alteracao, ativo)" +
                " VALUES (?, NOW(), 1)",
                Statement.RETURN_GENERATED_KEYS);

            st = origem.createStatement();
            rs = st.executeQuery(
                "SELECT DISTINCT MARCA FROM PRODUTOS_MARCA WHERE MARCA IS NOT NULL");

            int ins = 0;
            while (rs.next()) {
                String marca = rs.getString(1);
                if (marca == null || marca.trim().isEmpty()) continue;
                ps.setString(1, marca.trim());
                ps.executeUpdate();
                ResultSet keys = ps.getGeneratedKeys();
                if (keys.next()) {
                    mapaFabricante.put(marca.trim(), keys.getInt(1));
                    ins++;
                }
                keys.close();
            }
            contarInseridos(ctx, ins);
            LOG.info("[FabricanteStep] " + ins + " fabricantes inseridos.");

        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao migrar fabricantes: " + e.getMessage(), e);
        } finally {
            close(rs); close(st); close(ps);
        }

        // Recarrega mapa completo
        try {
            Statement stD = destino.createStatement();
            ResultSet rsD = stD.executeQuery("SELECT id, nome FROM lc_sistemas.fabricante");
            while (rsD.next()) mapaFabricante.put(rsD.getString(2), rsD.getInt(1));
            rsD.close(); stD.close();
        } catch (SQLException e) {
            throw new MigracaoException(getNome(), "Erro ao recarregar fabricantes: " + e.getMessage(), e);
        }

        ctx.setMapaFabricante(mapaFabricante);
    }

    @Override
    public void rollback(MigracaoContext ctx) {
        execIgnore(ctx.getDestinoConn(),
            "DELETE FROM lc_sistemas.fabricante WHERE id > 1",
            "rollback FabricanteStep");
    }
}
